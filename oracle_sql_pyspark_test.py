from pyspark.sql import SparkSession


jdbcHostname = "xxx.xxx.xxx.xxx"
jdbcDatabase = "schema.table"
jdbcPort = "1521"
jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/DWHPR1".format(jdbcHostname,jdbcPort)
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
# REMOTE_SPARK=xxx.xxx.xxx.xxx:7077
# $ spark-submit --master spark://$REMOTE_SPARK --jars C:\path\to\spark-2.2.1-bin-hadoop2.7\jars\ojdbc7.jar sql_server_test.py 

df = spark.read.jdbc(url=jdbcUrl, table=jdbcDatabase, properties=connectionProperties)
print(df.columns)
df.createOrReplaceTempView("DATA")
query = "select * from DATA where NPOLIZA = '00074490'"
spark.sql(query).show()
