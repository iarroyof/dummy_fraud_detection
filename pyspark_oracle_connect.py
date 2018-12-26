from pyspark.sql import SparkSession
import json


def get_auth(conn_info="connection.json", db_type="ORACLE", service="DWHPR1"):

    with open(conn_info) as f:
        j = json.load(f)
        return {'usr': j[db_type][service]['USER'], 
                'pass': j[db_type][service]['PASSWORD'],
                'ip': j[db_type][service]['URL'].split(':')[3][1:],
                'port': j[db_type][service]['URL'].split(':')[4],
                'service':  j[db_type][service]['URL'].split(':')[5]}



#path="C:\\Users\\HCAOA911\\Desktop\\sqldeveloper-17.4.1.054.0712-no-jre\\sqldeveloper\\jdbc\\lib\\"
#path="C:\\Users\\HCAOA911\\Music\\spark-2.2.1-bin-hadoop2.7\\jars\\"
#path="C:\\Users\\HCAOA911\\Downloads\\JDBC_Driver_6.4_SQL_Server\\sqljdbc_6.4\\enu\\"
#driver = "ojdbc7.jar"
#driver = "ojdbc8.jar"
#driver = "sqljdbc42.jar"
#driver = "mssql-jdbc-6.4.0.jre8.jar"
# TEST QUERY select * from DWHRAW.S_PEN_SOBREVIVENCIA where POLIZA_ID = '000291';
# Credentials file
# /Aplication/job/connection.json:
# Get serviceName from the database:
# SELECT sys_context('USERENV', 'SERVICE_NAME') FROM DUAL;
serviceName = "DWHPR1"
jdbcDatabase = "DWHRAW.S_PEN_SOBREVIVENCIA"

auth = get_auth(service=serviceName, db_type="ORACLE")
jdbcHostname = auth["ip"] 
jdbcPort = auth["port"]
jdbcUsername=auth['usr']
jdbcPassword=auth['pass']
jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(jdbcHostname, jdbcPort, serviceName)
connectionProperties = {
  "user": jdbcUsername,
  "password": jdbcPassword,
  "dbtable": jdbcDatabase,
  "driver" : "oracle.jdbc.driver.OracleDriver"
}

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

# For PostgreSQL:
# $ spark-submit --packages org.postgresql:postgresql:42.1.4 pyspark_learn_from_db.py

df = spark.read.jdbc(url=jdbcUrl, table=jdbcDatabase, properties=connectionProperties)
print(df.columns)
df.createOrReplaceTempView("DATA")
#query = "select * from DATA where NPOLIZA = '00074490'"
query = "select * from DATA where POLIZA_ID = '000291'"
spark.sql(query).show()
