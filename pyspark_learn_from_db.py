from pyspark.sql import SparkSession
from pyspark.sql import SQLContext


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.driver.extraClassPath","bin/sqljdbc42.jar") \
    .getOrCreate()

## DATABASE: semantica, csdwhpr1
#url = "jdbc:sqlserver://15.10.154.77\csdwhpr1;database=semantica;user=ReportUser;password=ReportUser"
query = "select * from semapp.hc_recibo where rownum < 10"
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgres:localhost:5432") \
    .option("dbtable", "schema.hc_recibo") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .load()

df.show()
df = spark.sql(query)
df.show()
