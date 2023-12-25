import requests
import jaydebeapi
import time,math
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS



def to_influxdb(partition_details):

    '''
    Insert storage utilization data in influxdb
    '''


    url="http://influxdb-sre-deployments.apps.ap-ossdev.zxnj.p1.openshiftapps.com/" # replace with influxdb url
    org = "IBM"
    bucket = "db2_stats"
    token="M370PPVRj2IaiJjR_UlqeS7Yl4Oy9AP69Nl1VAPypRiSBEngIUjfHCtNEBp5_qVsIPxlfxW12CNJZY33Pit0Sw=="

    client = InfluxDBClient(url=url,token=token)  
    print("InfluxDB Client Connected")
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for data in partition_details:
        DBPARTITIONNUM=data['DBPARTITIONNUM']
        UTILIZATION_PERCENT=data['UTILIZATION_PERCENT']
        insert=f"partition-data,partition={DBPARTITIONNUM} utilization_percent={UTILIZATION_PERCENT}"
        write_api.write(bucket, org, insert)


    print("Data inserted in InfluxDB")


def cedp_dz_connect():
    '''
    Connect to DB2 Database
    Returns [list]: conn and curs object 
    '''
    dsn_database = ""  
    dsn_hostname = ""  
    dsn_port = ""
    dsn_uid = ""
    dsn_pwd = ""
    jdbc_driver_name = ""
    jdbc_driver_loc = "db2jcc-db2jcc4.jar"
    
    jdbc_url = "jdbc:db2://"+dsn_hostname+":"+dsn_port+"/"+dsn_database+":sslConnection=true;"
    conn = jaydebeapi.connect(jdbc_driver_name,jdbc_url,{"user":dsn_uid,"password":dsn_pwd},jars=jdbc_driver_loc)
    print("DB2 connected")
    curs = conn.cursor()
    return [conn,curs]

def get_partitions():
    sql_st = '''SELECT DBPARTITIONNUM , ROUND((SUM(FS_USED_SIZE)::DECFLOAT / 1073741824), 2) USED_GB, ROUND((SUM(FS_TOTAL_SIZE)::DECFLOAT / 1073741824), 2) TOTAL_GB, ROUND((SUM(FS_USED_SIZE) / SUM(FS_TOTAL_SIZE)::DECFLOAT) * 100,2)  AS UTILIZATION_PERCENT, ROUND(( SUM(FS_TOTAL_SIZE) - SUM(FS_USED_SIZE)::DECFLOAT) / 1073741824,2) AS FREE_GB
                FROM TABLE(ADMIN_GET_STORAGE_PATHS(NULL,-2)) AS T
                WHERE STORAGE_GROUP_NAME NOT LIKE '%TEMP%'
                GROUP BY DBPARTITIONNUM , CASE WHEN DBPARTITIONNUM = 0 THEN 'NP' ELSE 'P' END'''
    
    conn,curs=cedp_dz_connect()
    curs.execute(sql_st)
    print("Query Executed")
    results = curs.fetchall()
    # Process and convert the results to Python types
    
    partition_details = []
    for row in results:
        partition_detail={}
        used_gb = float(row[1].toString())
        total_gb = float(row[2].toString())
        utilization_percent = float(row[3].toString())
        free_gb = float(row[4].toString())

        used_gb = round(used_gb, 2)
        total_gb = round(total_gb, 2)
        utilization_percent = round(utilization_percent, 2)
        free_gb = round(free_gb, 2)
        

        partition_detail['DBPARTITIONNUM']=row[0]
        #partition_detail['USED GB']=used_gb
        #partition_detail['TOTAL GB']=total_gb
        partition_detail['UTILIZATION_PERCENT']=utilization_percent
        #partition_detail['FREE GB']=free_gb

        partition_details.append(partition_detail)
    
    to_influxdb(partition_details)


get_partitions()

