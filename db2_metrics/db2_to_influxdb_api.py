import jaydebeapi
from fastapi import FastAPI,Response
from fastapi.middleware.cors import CORSMiddleware
import time,math
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


org = "PSL"
bucket = "grafana"
token=""

def to_influxdb(storage_utilization):

    '''
    Insert storage utilization data in influxdb
    
    '''

    client = InfluxDBClient(url="http://localhost:8086",token=token)  # replace with influxdb url
    print("InfluxDB Client Connected")
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for data in storage_utilization:
        TENANT_ID=data['TENANT_ID']
        LOGICAL_STORAGE_KB=data['LOGICAL_STORAGE_KB']
        PHYSICAL_STORAGE_KB=data['PHYSICAL_STORAGE_KB']
        insert=f"db2_stats,tenant={TENANT_ID} logical={LOGICAL_STORAGE_KB},physical={PHYSICAL_STORAGE_KB}"
        write_api.write(bucket, org, insert)

    print("Data inserted in InfluxDB")



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
    print("DB2 connected")
    curs = conn.cursor()
    return [conn,curs]


app =FastAPI()


@app.get("/")
def home():
    description='''/getStorage : Get the Storage Utilization of DB2 Schemas'''
    return description

@app.get("/getStorage")
def run_query():
    sql_st = '''SELECT TABSCHEMA , SUM(DATA_OBJECT_L_SIZE + INDEX_OBJECT_L_SIZE + LONG_OBJECT_L_SIZE + LOB_OBJECT_L_SIZE + XML_OBJECT_L_SIZE + COL_OBJECT_L_SIZE) AS LOGICAL_SIZE,
            SUM(DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE + LONG_OBJECT_P_SIZE + LOB_OBJECT_P_SIZE + XML_OBJECT_P_SIZE + COL_OBJECT_P_SIZE) AS PHYSICAL_SIZE  
            FROM SYSIBMADM.ADMINTABINFO WHERE TABSCHEMA LIKE 'TNT_%'
            GROUP BY TABSCHEMA
            HAVING SUM(DATA_OBJECT_L_SIZE + INDEX_OBJECT_L_SIZE + LONG_OBJECT_L_SIZE + LOB_OBJECT_L_SIZE + XML_OBJECT_L_SIZE + COL_OBJECT_L_SIZE)>1000
            AND SUM(DATA_OBJECT_P_SIZE + INDEX_OBJECT_P_SIZE + LONG_OBJECT_P_SIZE + LOB_OBJECT_P_SIZE + XML_OBJECT_P_SIZE + COL_OBJECT_P_SIZE) > 1000'''
    

    conn,curs=cedp_dz_connect()
    start=time.time()
    
    curs.execute(sql_st)
    end=time.time()
    collect_time=time.ctime(end)
    print(collect_time)
    print(f"Query Executed. Execution Time: {math.trunc(end-start)} s")

    results = curs.fetchall()
    print("Result Fetched")

    storage_utilization=[]
    for row in results:
        tenant_detail={}
        TENANT_ID=row[0]
        LOGICAL_STORAGE=row[1]
        PHYSICAL_STORAGE=row[2]

        tenant_detail['TENANT_ID']=TENANT_ID
        tenant_detail['LOGICAL_STORAGE_KB']=LOGICAL_STORAGE
        tenant_detail['PHYSICAL_STORAGE_KB']=PHYSICAL_STORAGE
        tenant_detail['COLLECT_TIME']=collect_time
        storage_utilization.append(tenant_detail)

    to_influxdb(storage_utilization)
    return storage_utilization
