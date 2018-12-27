from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime
import json


def get_auth(conn_info="connection.json", db_type="ORACLE", service="DWHPR1"):
    with open(conn_info) as f:
        j = json.load(f)
        return {'usr': j[db_type][service]['USER'], 
                'pass': j[db_type][service]['PASSWORD'],
                'ip': j[db_type][service]['URL'].split(':')[3][1:],
                'port': j[db_type][service]['URL'].split(':')[4],
                'service':  j[db_type][service]['URL'].split(':')[5]}


serviceName = "DWHPR1"
#jdbcDatabase = "DWHRAW.S_PEN_SOBREVIVENCIA"
jdbcDatabase = "DWHRAW.S_PENSIONES_CLIENTE"
# Putting something like this when reading the database can serve as an initial filter making more 
# efficient the treatment of the dataframe pointed to it.
#init_query = "select CAST(FECHA_SEMESTRE1 as date) date_semestre1, * from {} as tmp".format(jdbcDatabase)
init_query = "select * from {} where MUERTO IS NOT null".format(jdbcDatabase)


auth = get_auth(conn_info="../connection.json", service=serviceName, db_type="ORACLE")
jdbcHostname = auth["ip"] 
jdbcPort = auth["port"]
jdbcUsername=auth['usr']
jdbcPassword=auth['pass']
jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(jdbcHostname, jdbcPort, serviceName)
connectionProperties = {
  "user": jdbcUsername,
  "password": jdbcPassword,
  "dbtable": jdbcDatabase,
  "driver" : "oracle.jdbc.driver.OracleDriver",
  #"dbtable": init_query  # This filters the database at time of reading it
}

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

# A number of top rows for testing
get_n = 1000

date = datetime.datetime.strptime('2016-02-02', '%Y-%m-%d').date()

df = spark.read.jdbc(url=jdbcUrl, 
                     table=jdbcDatabase, 
                     properties=connectionProperties).limit(get_n)
df.createOrReplaceTempView("DATA")
#query = "select CAST(FECHA_SEMESTRE1 as date) date_semestre1, * from DATA as tmp"
query = "select * from DATA where MUERTO IS NOT null"

#df.select(["HSID_PENSIONES_CLIENTE", "ESTATUS", "CVE_ESTADO", "MUERTO"])

df_x = spark.sql(query)
df_x.select(["HSID_PENSIONES_CLIENTE", "ESTATUS", "CVE_ESTADO", "MUERTO"]).show()
#df.filter(F.col('date_semestre1') == date).show()
#df_x.show()
