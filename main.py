from typing import Union
from fastapi import FastAPI
import pyodbc,time,schedule,requests
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from fastapi.responses import JSONResponse
from model import TableName,RowData,UpdateValue,InsertData,SubmitDate,ColumnDetails,UpdateData,InsertDatas,DeleteData
from dotenv import load_dotenv
import os
load_dotenv()


app = FastAPI()
connection_string = "Driver={SQL Server};Server=CRSDWSQLDEV02\SDW_QA;Database=STG_SRVC_WH;Trusted_Connection=yes"


origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8080",
    "https://frmwrkuiweb.onrender.com"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Use "*" for testing, replace with `origins` for security
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

columnValue = []

@app.options("/{full_path:path}")
async def preflight(full_path: str):
    return JSONResponse(
        content={"message": "CORS preflight OK"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Credentials": "true",
        }
    )

@app.middleware("http")
async def add_cors_headers(request, call_next):
    response = await call_next(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "*"
    print('')
    return response

@app.get("/tableName")
def read_root():
    Table_Name = ['FRMWRKCONFIG.keys','FRMWRKCONFIG.sqlserverdelta','FRMWRKCONFIG.ExcludeHash','FRMWRKCONFIG.projectorchestration','FRMWRKCONFIG.configserver','FRMWRKCONFIG.project','FRMWRKCONFIG.sqlserversource']
    return Table_Name

@app.post("/columnName")
def columnName(value : TableName):
    print(value,'namme')
    conn = pyodbc.connect(connection_string)
    print("Connected")
    cursor = conn.cursor()
    tablename = value.tablename
    if(tablename[0:4] != 'FRMW'):
        tablename = 'frmwrkconfig.'+value.tablename
    
    Column,columnDic,ctr = [],{},0
    for column in cursor.execute("SELECT TOP 0 * FROM {}".format(tablename)).description:
        columnDic[ctr] = column[0] 
        ctr+=1
        Column.append(column[0])
  
    cursor.execute("SELECT * FROM "+tablename)
    rows = cursor.fetchall()
    tableData,dic = [],{}
    for row in rows:
        lis,ctr = [],0
        for j in row:
            if(columnDic[ctr] not in dic and j):
                dic[columnDic[ctr]] = [j]
            else:
                if(j and j not in dic[columnDic[ctr]]):
                    dic[columnDic[ctr]].append(j)
            ctr+=1
            lis.append(j)
        tableData.append(lis)
    cursor.close()
    conn.close()
    return JSONResponse(content={'column':Column,'tableData':tableData,'uniqueDate':dic},status_code=200)



@app.post("/postdata")
def post_data(row_data: RowData):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    columnValue = row_data.values
    tableName = row_data.tableName
    columnCount = 0 
    rowCount = 0 
    values = []
    hash = []
    for data in columnValue:
        lis = []
        columnCount = 0 
        hashValue = {}
        count = 0 
        for i,j in data["values"].items():
            ln = len(i)
            if(tableName=="FRMWRKCONFIG.project"):
                if(i=="Project"):
                    hashValue[i] = [j,rowCount]
                elif(i=="ROWID"):
                    hashValue[i] = [j,rowCount]
            else:
                if(i=="Project"):
                    hashValue[i] = [j,rowCount]
                elif(i[ln-2:]=='ID'):
                    hashValue[i] = [j,rowCount]
    
            columnCount+=1
            count+=1
            lis.append(j)
        hash.append(hashValue)
        values.append(lis)
        rowCount+=1
    columnValue = []
    for i in hash:
        selectQuery = "SELECT * from "+tableName+" where "
        rowcount = 0 
        key = []
        for x,y in i.items():
            selectQuery += "{}='{}' and ".format(x,y[0])
            key.append(y[0])
            rowcount = y[1]
        selectQuery = selectQuery[0:len(selectQuery)-4:]
        val = []
        for data in values:
            for x in key:
                if(x in data):
                    val = data
        cursor.execute(selectQuery)
        rows = cursor.fetchall()
        if(rows):
            return JSONResponse(content={'message':'Duplicate key Insertion','Rowcount':rowcount,'keys':key,'datas':val},status_code=202)
    for i in hash:
        columnValue = "(?"
        for i in range(columnCount-1):
            columnValue+=',?'
        columnValue+=')'
        insert_query = "INSERT INTO {} VALUES {}".format(tableName,columnValue)
    
        for row in values:
            print(row)
            cursor.execute(insert_query, row)
            conn.commit() 
        cursor.close()
        conn.close()
        return {"message": "Data received successfully"}
    


@app.post('/updatedata')
def updateData(data : UpdateValue):
    tableName = data.TableName
    columnName = data.ColumnName
    key = data.key
    val = data.datas
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    ctr = 0 
    query = "update {} set ".format(tableName)
    for cl in columnName:
        query+= cl +"=" + "'"+str(val[ctr])+"'" +" , "
        ctr+=1
    server = val[0]
    dbID = val[1]
    l = len(query)
    query = query[0:l-3]
    query+=" where Project = '{}' and keyID = {}".format(server,dbID)
    print(query)

    try:
        cursor.execute(query)
        conn.commit() 
        return JSONResponse(content={'message':'Data get updated'},status_code=200)
    except Exception as e:
        print("Error:", e)
        return JSONResponse(content={'message':'Error updating data'},status_code=500)  

@app.post("/InsertData")
def InsertData(value : InsertData):
    tblname = value.TableName
    columnValue = value.ColumnValue
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    insert_statement = f'select * from {tblname} where '
    for i,j in columnValue.items():
        insert_statement+= f" {i} = '{j}' and "
    insert_statement = insert_statement[:len(insert_statement)-4]
    cursor.execute(insert_statement)
    rows = cursor.fetchall()
    columnCount,columns = 0,'('
    for i in cursor.execute(f'select top 0 * from {tblname}').description:
        columnCount+=1 
        columns+=i[0]+','
    columns = columns[0:len(columns)-1]+')'
   
    columnValue = "(?"
    for i in range(columnCount-1):
        columnValue+=',?'
    columnValue+=')'
    insert_query = "INSERT INTO {} VALUES {}".format(tblname,columns)
    lis = []
    for data in rows:
        script = insert_query+" into " + str(data)
        lis.append(script)
    return JSONResponse(content={
                    'message': f'Insert Scripts',
                    'scripts': lis
                }, status_code=200)
    

@app.post("/submitData")
async def submit_data(data: dict):
    conn = pyodbc.connect(connection_string)
    cursor,insertScript = conn.cursor(),[]
    
    for tableName, columnValue in data.items():
        values = []
        hash,columncount = [],0
        hashValue = {}
        selectQuery = "SELECT * FROM FRMWRKCONFIG.{} WHERE ".format(tableName)
        key = []
        for rowCount, (columnName, columnData) in enumerate(columnValue.items()):
            columncount+=1
            if tableName == "project":
                if columnName in ["Project", "ROWID"]:
                    selectQuery += "{} = '{}' AND ".format(columnName, columnData)
            else:
                if columnName == "Project" or columnName.endswith("ID"):
                    selectQuery += "{} = '{}' AND ".format(columnName, columnData)
            key.append(columnData)
        selectQuery = selectQuery[:-4]
        try:
            cursor.execute(selectQuery)
            rows = cursor.fetchall()
            if rows:
                return JSONResponse(content={
                    'message': f'Duplicate key Insertion on table {tableName}',
                    'TableName': tableName,
                    'keys': key,
                    'datas': values
                }, status_code=203)
        except pyodbc.Error as ex:
            sqlstate = ex.args[0]
            errmsg = ex.args[1]
            print(f"SQL Error: {errmsg}") 
            return JSONResponse(content={
                'message': 'Database error occurred',
                'SQLState': sqlstate,
                'Error': errmsg
            }, status_code=500)

    for tableName, columnValue in data.items():        
        columnValue = "("
        val = []
        help = "(?"
        for i in key:
            columnValue+=f'{i},'
            help+=",?"
            val.append(i)
        help = help[0:len(help)-2]+')'
        inst = "INSERT INTO frmwrkconfig.{} VALUES {}".format(tableName,help)
        cursor.execute(inst,val)
        cursor.commit()
        columnValue = columnValue[0:len(columnValue)-1]+')'
        insert_query = "INSERT INTO frmwrkconfig.{} VALUES {}".format(tableName,columnValue)
        insertScript.append(insert_query)
    
    return JSONResponse(content={
                    'message': f'Insert Scripts',
                    'scripts': insertScript
                }, status_code=200)
          

@app.post("/submitdata")
def SubmitDate(data : SubmitDate):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    for scripts in data:
        cursor.execute(scripts)



@app.post("/columnDetails")
def get_column_suggestions(value: ColumnDetails):
    print(value)
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    
    tablename = value.tablename
    previous_data = value.previousData

    # Base query to fetch all data from the table
    query = f"SELECT * FROM FRMWRKCONFIG.{tablename}"
    flag = 0 
    if previous_data:
        for i,j in previous_data.items():
            if(j):
                flag = 1
                break 
    if flag:
        conditions = " WHERE " + " AND ".join([f"{col} = '{val}'" for col, val in previous_data.items() if val])
        query += conditions

    try:
        print(query)
        cursor.execute(query)
        rows = cursor.fetchall()

        suggestions = {}
        column_names = [desc[0] for desc in cursor.description]

        # Collect unique values for each column
        for row in rows:
            for col_name, value in zip(column_names, row):
                if col_name not in suggestions:
                    suggestions[col_name] = [value]
                elif value not in suggestions[col_name]:
                    suggestions[col_name].append(value)

    except Exception as e:
        print("Error:", e)
        return JSONResponse(content={'message': 'Error fetching data'}, status_code=500)
    finally:
        cursor.close()
        conn.close()

    return {"uniqueDate": suggestions}


@app.post("/updateData")
def update(value : UpdateData):
    tableName = value.tableName
    oldData = value.oldData

    update_clauses = []
    where_clauses = []
    newData = value.newData
    for key, new_val in newData.items():
        old_val = oldData.get(key)
        if new_val != old_val:
            update_clauses.append(f"{key} = '{new_val}'")

    for key, old_val in oldData.items():
        where_clauses.append(f"{key} = '{old_val}'")
    sql = f"""
    UPDATE {tableName}
    SET {', '.join(update_clauses)}
    WHERE {' AND '.join(where_clauses)}
    """
    print(sql,'hehhe')
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        return JSONResponse(content={
                    'message': f'Update Scripts',
                    'scripts': [sql]
                }, status_code=200)
    except Exception as e:
        return JSONResponse(content={'message': 'Error fetching data'}, status_code=500)

    
    


    
@app.post("/InsertValue")
def post_data(row_data: InsertDatas):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    columnValue = row_data.ColumnValue
    tableName = row_data.TableName
    has,where_clauses = {},[]
    for i,j in columnValue.items():
        ln = len(i)
        if(tableName=="FRMWRKCONFIG.project"):
            if(i=="Project" or i=="ROWID"):
               has[i] = j      
               where_clauses.append(f"{i} = '{j}'")
        else:
            if(i=="Project" or i[ln-2:]=='ID'):
                has[i] = j 
                where_clauses.append(f"{i} = '{j}'")
    smt = f"select * from {tableName}  WHERE {' AND '.join(where_clauses)}"
    try:
        cursor.execute(smt)
        rows = cursor.fetchall()
        print(rows)
        if(rows):
            return JSONResponse(content={
                    'message': f'Duplicate key insertion',
                }, status_code=201)
        else:
             smt = f"""INSERT INTO {tableName} VALUES """
        smt+="("
        for i,j in columnValue.items():
            smt+= f" '{str(j)}',"
        smt = smt[0:len(smt)-1] + ")"
        print(smt)
        try: 
            cursor.execute(smt)
            cursor.commit()
            return JSONResponse(content={
                    'message': f'Tnsert Scripts',
                    'scripts': [smt]
                }, status_code=200)
        except Exception as e:
            return JSONResponse(content={
                    'message': e
                }, status_code=400)

    except Exception as e:
            return JSONResponse(content={
                    'message': e
                }, status_code=400)
        

@app.post("/deleteValue")
def deletevalue(value : DeleteData):
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    tableName = value.TableName
    values = value.ColumnValue
    stm = f"DELETE FROM {tableName} WHERE "
    for i,j in values.items():
        stm+= f"{i} = '{j}' AND "
    stm = stm[0:len(stm)-4]
    try:
        print(stm)
        cursor.execute(stm)
        cursor.commit()
        # CCO_MART_RPLCTN	3292	OPT_ID
        return JSONResponse(content={
                    'message': 'Data deleted',
                    'scripts' : [stm]
                }, status_code=200)
    except Exception as e:
        return JSONResponse(content={
                    'message': e,
                }, status_code=401)


