from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# "bin/sqljdbc42.jar") 
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.driver.extraClassPath", "/almac/ignacio/spark-2.4.0-bin-hadoop2.7/jars/jdbc-postgresql.jar") \
    .getOrCreate()

## DATABASE: semantica, csdwhpr1
#url = "jdbc:sqlserver://15.10.154.77\csdwhpr1;database=semantica;user=ReportUser;password=ReportUser"
query = "select * from semapp.fraud_data_sample"
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:postgres") \
    .option("dbtable", "semapp.fraud_data_sample") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .load()

df.show()
#df = spark.sql(query)
#df.show()
