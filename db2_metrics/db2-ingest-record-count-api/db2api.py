import jaydebeapi
from fastapi import FastAPI


def cedp_dz_connect():
    dsn_database = ""  
    dsn_hostname = ""  
    dsn_port = ""
    dsn_uid = ""
    dsn_pwd = ""
    jdbc_driver_name = ""
    jdbc_driver_loc = ""
    
   
    
    jdbc_url = "jdbc:db2://"+dsn_hostname+":"+dsn_port+"/"+dsn_database+":sslConnection=true;"
    
    conn = jaydebeapi.connect(jdbc_driver_name,jdbc_url,{"user":dsn_uid,"password":dsn_pwd},jars=jdbc_driver_loc)
    curs = conn.cursor()
    return curs


conn=cedp_dz_connect()



app =FastAPI()

@app.get("/query/{TENANT_ID}/{MONITOR_START_TIME}/{MONITOR_END_TIME}")
def run_query(TENANT_ID: str, MONITOR_START_TIME: str, MONITOR_END_TIME:str):
    sql_st = ' SELECT sum("IngestRecordCount") FROM ' +TENANT_ID +'.DM_FILE_INFO WHERE'+" '"+MONITOR_START_TIME+"' "+ '< "RetryStartTime" and "RetryStartTime" <'+" '"+ MONITOR_END_TIME+"' "
    print(sql_st)
    conn.execute(sql_st)
    results = conn.fetchall()
    return results