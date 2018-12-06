from pyspark.sql import SparkSession

# Get serviceName from the database:
# SELECT sys_context('USERENV', 'SERVICE_NAME') FROM DUAL;
serviceName = "DWHPR1"
jdbcHostname = "xxx.xxx.xxx.xxx"
jdbcDatabase = "schema.table"
jdbcPort = "1521"
jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(jdbcHostname, jdbcPort, serviceName)
jdbcUsername='user'
jdbcPassword='pass'
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

# For Oracle SQL:
# REMOTE_SPARK=yyy.yyy.yyy.yyy:7077
# $ spark-submit --master spark://$REMOTE_SPARK --jars C:\path\to\spark-2.2.1-bin-hadoop2.7\jars\ojdbc7.jar sql_server_test.py 

df = spark.read.jdbc(url=jdbcUrl, table=jdbcDatabase, properties=connectionProperties)
print(df.columns)
df.createOrReplaceTempView("DATA")
query = "select * from DATA where NPOLIZA = '00074490'"
spark.sql(query).show()
