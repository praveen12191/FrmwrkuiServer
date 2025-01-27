from pydantic import BaseModel
from typing import List,Any,Dict,Optional



class TableName(BaseModel):
     tablename : str



class RowData(BaseModel):
    values: list
    tableName : str


class UpdateValue(BaseModel):
     TableName : str 
     ColumnName : list
     key : list
     datas: List[Any]


class InsertData(BaseModel):
     TableName : str
     ColumnValue : Dict
     
class SubmitDate(BaseModel):
     Scripts : list 
     

class ColumnDetails(BaseModel):
    tablename: str
    previousData: Dict[str, str]

class UpdateData(BaseModel):
     tableName : str
     oldData: Dict
     newData: Dict


class InsertDatas(BaseModel):
     TableName : str
     ColumnValue : Dict

class DeleteData(BaseModel):
     TableName : str
     ColumnValue : Dict

