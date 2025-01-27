import pandas as pd

def GetDescription(file_path, sheet_name, table_name, column_names, output_file):
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.upper()
    results = []
    for column_name in column_names:
        filtered_df = df[(df['TABLE_NAME'] == table_name) & (df['COLUMN_NAME'] == column_name)]
        if not filtered_df.empty:
            desc = filtered_df['REQUIREMENT'].values[0]
        else:
            desc = "No matching record found"
        
        results.append({'COLUMN_NAME': column_name, 'DESC': desc})
    
    result_df = pd.DataFrame(results)

    result_df.to_excel(output_file, index=False)


file_path = r'C:\Users\pr38\Documents\DG\STTM\PROD_Servicing_Data_Warehouse_STTM_MASTER 9.xlsx'
sheet_name = 'SRVC_WH'  
table_name = 'INVSTR' 
column_names = ['INVSTR_CD',
'SOR_CD',
'EFF_DTTM',
'END_DTTM',
'UNQ_KEY_TXT',
'INVSTR_NM',
'INVSTR_ADDR_ID',
'PRTFL_NM',
'FNMA_FLG',
'INVSTR_SRVC_CD',
'INVSTR_TYPE_CD',
'PH_NBR',
'AUD_CRE_BY_NM',
'AUD_CRE_DTTM',
'AUD_UPD_BY_NM',
'AUD_UPD_DTTM',
'ETL_BATCH_ID',
'FRCLS_IN_THE_NM_OF',
'CRDTR_OWN',
'MSR_IND',
'RECAP_AGRM_IND',
'TM1_PRTFL_NM',
'INVSTR_ORG_ID',
'SRVC_ORG_ID',
'CUSTODIAN_ORG_ID',
'CMPNY_CD',
'CTF_FRQ',
'INVSTR_RPT_CTF_DT',
'LAST_RPT_DT',
'INVSTR_SRVC_CNTCT',
'INVSTR_TIN_NBR',
'INVSTR_TIN_ID',
'INVSTR_ENDRS_CD',
'INVSTR_BNK_NBR',
'INVSTR_PRIN_AND_INT_ACCT_NBR',
'INVSTR_ESCRW_BNK_NBR',
'INVSTR_ESCRW_ACCT_NBR',
'INVSTR_SUSP_BNK_NBR',
'INVSTR_SUSP_ACCT_NBR',
'INVSTR_SBSDY_BNK_NBR',
'INVSTR_SBSDY_ACCT_NBR',
'STS',
'POOL_CLAS_CD',
'PRTFL_IND',
'CFPB_WTFALL']  
output_file = r'C:\Users\pr38\Documents\DG\STTM\output_results.xlsx' 


GetDescription(file_path, sheet_name, table_name, column_names, output_file)

print("Output saved to:", output_file)
