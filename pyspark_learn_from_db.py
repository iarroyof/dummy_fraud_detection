from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# Correct command to submit and satisfy dependencies in local mode:
# $ spark-submit --packages org.postgresql:postgresql:42.1.4 pyspark_learn_from_db.py
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()

## DATABASE: semantica, semapp.csdwhpr1
# Local mode connection and extraction
query = "select * from semapp.fraud_data_sample"
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:postgres") \
    .option("dbtable", "semapp.fraud_data_sample") \
    .option("user", "postgres") \
    .option("password", "postgresql") \
    .load()

df.show()
