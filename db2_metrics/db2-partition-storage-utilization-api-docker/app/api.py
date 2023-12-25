import jaydebeapi
from fastapi import FastAPI,Response,Depends
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
import time,math
from app.auth.auth_handler import signJWT
from app.auth.auth_bearer import JWTBearer
import decimal

class User(BaseModel):
    username: str
    password: str

users = [
    User(username="gi_test", password="8jKKvDjKNP0Op3Zg59UaFjmKh"),
]

def check_user(data):
    """
    Check if the provided user credentials exist in the users list.
    """
    for user in users:
        if user.username == data.username and user.password == data.password:
            return True
    return False


app =FastAPI()



@app.post("/token")
async def user_login(user: User):
    """
    Endpoint for user login. Generates a JWT token if the provided credentials are valid.
    """
    if check_user(user):
        return signJWT(user.username + user.password)
    return {
        "error": "Wrong login details!"
    }

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
    jdbc_driver_name = "com.ibm.db2.jcc.DB2Driver"
    jdbc_driver_loc = "db2jcc-db2jcc4.jar"
    
    jdbc_url = "jdbc:db2://"+dsn_hostname+":"+dsn_port+"/"+dsn_database+":sslConnection=true;"
    conn = jaydebeapi.connect(jdbc_driver_name,jdbc_url,{"user":dsn_uid,"password":dsn_pwd},jars=jdbc_driver_loc)
    print("DB2 connected")
    curs = conn.cursor()
    return [conn,curs]



app =FastAPI()

@app.get("/getPartitions",dependencies=[Depends(JWTBearer())])
def run_query():
    sql_st = '''SELECT DBPARTITIONNUM , ROUND((SUM(FS_USED_SIZE)::DECFLOAT / 1073741824), 2) USED_GB, ROUND((SUM(FS_TOTAL_SIZE)::DECFLOAT / 1073741824), 2) TOTAL_GB, ROUND((SUM(FS_USED_SIZE) / SUM(FS_TOTAL_SIZE)::DECFLOAT) * 100,2)  AS UTILIZATION_PERCENT, ROUND(( SUM(FS_TOTAL_SIZE) - SUM(FS_USED_SIZE)::DECFLOAT) / 1073741824,2) AS FREE_GB
                FROM TABLE(ADMIN_GET_STORAGE_PATHS(NULL,-2)) AS T
                WHERE STORAGE_GROUP_NAME NOT LIKE '%TEMP%'
                GROUP BY DBPARTITIONNUM , CASE WHEN DBPARTITIONNUM = 0 THEN 'NP' ELSE 'P' END'''
    
    conn,curs=cedp_dz_connect()
    print("DB2 Connected")
    curs.execute(sql_st)
    print("Query Executed")
    results = curs.fetchall()
    # Process and convert the results to Python types
    
    partition_details = {}
    data=[]
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
        partition_detail['USED GB']=used_gb
        partition_detail['TOTAL GB']=total_gb
        partition_detail['UTILIZATION_PERCENT']=utilization_percent
        partition_detail['FREE GB']=free_gb

        data.append(partition_detail)

    print("Partitions Fetched", len(data))
    partition_details['data']=data

    return partition_details
