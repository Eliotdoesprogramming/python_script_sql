import pyodbc
import pandas as pd
def multiple_fast_insert(dataframe:pd.DataFrame,
                        db:str,
                        schema:str,
                        table:str,
                        server_name:str='localhost',
                        uid:str='sa', 
                        pw:str='Password123',
                        batch_size:int=1000)->int: 
    sql_conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server_name+';DATABASE='+db+';UID='+uid+';PWD='+pw)
    cur = sql_conn.cursor()
    headers = ''
    qmark = ''
    for column in dataframe.columns:
        headers += column + ','
        qmark += '?,'
    headers = headers[:-1]
    qmark = qmark[:-1]

    df_listed = dataframe.values.tolist()
    i = 0
    while i < len(df_listed):
        chunk = df_listed[i:i+batch_size]
        cur.executemany('INSERT INTO '+schema+'.'+table+' ('+headers+') VALUES ('+qmark+')', chunk)
        i += batch_size
    sql_conn.commit()
    cur.close()
    return df_listed.__len__()