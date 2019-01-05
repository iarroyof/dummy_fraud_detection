
from pyspark.sql import SparkSession
#from pyspark.ml.tuning import TrainValidationSplit
#from pyspark.ml.classification import LogisticRegression
import pyspark.sql.functions as F
from itertools import chain
import datetime
import json


def printc(s):
    beg = '\x1b[6;30;42m'
    end = '\x1b[0m'
    print(beg + s + end)


def get_auth(conn_info="connection.json", db_type="ORACLE", service="DWHPR1"):
    with open(conn_info) as f:
        j = json.load(f)
        return {'usr': j[db_type][service]['USER'], 
                'pass': j[db_type][service]['PASSWORD'],
                'ip': j[db_type][service]['URL'].split(':')[3][1:],
                'port': j[db_type][service]['URL'].split(':')[4],
                'service':  j[db_type][service]['URL'].split(':')[5]}


def df_hstack(df_list):
    stacked = df_list[0].withColumn("g_id", F.lit(0))
    for i, next_df in enumerate(df_list[1:]):
        stacked = stacked.union(next_df.withColumn("g_id", F.lit(i + 1)))

    return stacked

#jdbcDatabase = "DWHRAW.S_PEN_SOBREVIVENCIA"
# Tal vez si el nombre del titular es diferente al del asegurado 
# tenga cierta relevancia como feature
LEAVE = "Death"
leave = "MUERTO"
relation = "nomina"  # "voz", "todo"
# This is a directory for spark. 
n_groups = 4
out_csv = "/DB_PQ/pyrthon_data/_data/random_sample_pensiones_{}-fold".format(n_groups) 
#out_csv = None
out_null_percent_csv = out_csv + ('' if out_csv is None else "_null_percents")
remove = [
	"CALLE",
	"CELULAR",
	"CUENTA", 
	"CURP",
	"CVE_TARJETA",
	"SSID_DATOS_NOMINA", 
	"SSID_PENSIONES_CLIENTE", 
	"NUMERO_OFERTA",
	"RFC",
	"EMAIL",
	"HASH_CD",
	"USR_MOD",
	"NUM_EXTERIOR",
	"NOMBRE",
	"NOMBRE_2",
	"APELLIDO_MATERNO",
	"APELLIDO_PATERNO",
	"TELEFONO",
	"DIRECCION",
	"NUMERO_OFERTA",
	"NUMERO_SEGURO_SOCIAL"
	]
holder = "NOMBRE_TITULAR"
insured = "NOMBRE_ASEGURADO"
holder_insured = [holder, insured]
hol_ins_col = "HolderEqInsured"
pivots = {"hsid": ("HSID_PENSIONES_CLIENTE", "HSID_PENSIONES_CLIENTE"),
	"client": ("CLIENTE_ID", "NUMERO_CLIENTE_ID"),
	"policy": ("POLIZA", "POLIZA_ID"), 
	"nucleo": ("NUCLEO", "NUCLEO_ID"),
	"seguro": ("REGIMEN_SEG_SOCIAL", "REGIMEN_SEG_SOCIAL_ID")
	}
nomina = ["hsid", "policy", "nucleo", "seguro"]
voz = ["client"]

if relation == "nomina":
    pivot = {k: pivots[k] for k in pivots if k in nomina}
elif  relation == "voz":
    pivot = {k: pivots[k] for k in pivots if k in voz}
elif relation == "todo":
    pivot = pivot

jdbcDatabase = "DWHRAW.S_PENSIONES_CLIENTE"
serviceName = "DWHPR1"

auth = get_auth(conn_info="../connection.json", service=serviceName, db_type="ORACLE")

jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(auth["ip"],  auth["port"],  auth["service"])
connectionProperties = {
  "user": auth['usr'],
  "password": auth['pass'],
  "dbtable": jdbcDatabase,
  "driver" : "oracle.jdbc.driver.OracleDriver",
}

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()
# Avoid INFO logs
spark.sparkContext.setLogLevel('WARN')
# A number of top rows for testing
get_n = 1000

dfa = spark.read.jdbc(url=jdbcUrl, 
                     table=jdbcDatabase, 
                     properties=connectionProperties)  #.limit(get_n)

jdbcDatabase = "DWHRAW.S_PEN_DATOS_NOMINA"
dfb = spark.read.jdbc(url=jdbcUrl,
                     table=jdbcDatabase,
                     properties=connectionProperties)  #.limit(get_n)

repeats = [ ]
for f in dfa.columns:
     if f in dfb.columns and f not in list(chain(*pivot.values())):
         repeats.append(f)

dfb = dfb.drop(*repeats)
df = dfa.join(dfb, list(chain(*pivot.values()))[0], "right")

# Remove uninformative columns and putting label LEAVE = "Death"
valids = [v for v in df.columns if not v in remove]
df = df.select(valids).orderBy(list(chain(*pivot.values()))[0]) \
                      .withColumn(hol_ins_col, 
                                  F.when(F.col(holder) == F.col(insured), 1) \
                                       .otherwise(0) 
                                   ) \
                      .withColumn(LEAVE, F.when(F.col(leave).isNull(), 0) \
                                   .otherwise(1)
                                   ) \
                      .drop(leave)# \
                      #.drop(*holder_insured)
# Drop uninformative features (most samples are null for them)
N = float(df.count())
# Equalize class imbalance while stratified sampling
Np =  float(df.filter(F.col(LEAVE) == 1).count())
Nn =  float(df.filter(F.col(LEAVE) == 0).count())

if n_groups > 1:
    # Create multiple ('n_groups') of seeded random datasets
    groups = [df.sampleBy(LEAVE, fractions={0:  Np / N, 1:  Nn / N}, seed=g)
                                                       for g in range(n_groups)]
    if not out_csv is None:
        df_hstack(groups).coalesce(1) \
                         .write.option("inferSchema", "true") \
                         .csv(out_csv, header =True, 
                                       dateFormat="yyyy-MM-dd HH:mm:ss")
    else:
        df_hstack(groups).show()

else:        
    dfs = df.sampleBy(LEAVE, fractions={0:  Np / N, 1:  Nn / N}, seed=0)
    if not out_csv is None:
        dfs.coalesce(1).write.option("inferSchema", "true") \
                       .csv(out_csv, header =True, 
                                       dateFormat="yyyy-MM-dd HH:mm:ss")
    else:
        dfs.show()

missing_stats_data = spark.createDataFrame([(c, 1 - float(df.filter(F.col(c).isNull()).count()) / N)
                                                 for c in df.columns], 
                                           ["feature", "miss_percent"]
		                           ).orderBy("miss_percent")
if not out_null_percent_csv is None:
    missing_stats_data.coalesce(1).write.option("inferSchema", "true") \
                   .csv(out_null_percent_csv, header =True)
    printc("Missing Stats written into %s" % out_null_percent_csv)
else:
    missing_stats_data.show()

missing_stats_data.orderBy("miss_percent", ascending=False) \
                  .limit(int(missing_stats_data.count() * 0.2)).show()
##train, test = dfs.randomSplit([0.7, 0.3], seed=12345)

#printc("train Positive n_samples: %f\n"
#       "train Negative n_samples: %f\n"  
#       "test Positive n_samples: %f\n"
#       "test Negative n_samples: %f" \
#       .format(train.filter(F.col(LEAVE) == 1).count(),
#               train.filter(F.col(LEAVE) == 0).count(),
#               test.filter(F.col(LEAVE) == 1).count(),
#               test.filter(F.col(LEAVE) == 0).count()
#               )
#         )
#printc("%s\n" % dfs.columns)
#dfs.toPandas().to_csv("/DB_PQ/pyrthon_data/_data/random_sample_pensiones.csv", header=True)
##df.createOrReplaceTempView("DATA")
##query = "select * from DATA where {} is not null".format(leave)
##df_p = spark.sql(query)

##printc("%s" % df.columns)
##df.select(list(chain(*pivot.values())) + [holder, insured, hol_ins_col, LEAVE]).filter(F.col(hol_ins_col) == 0).show()


# Verify dimensionality number of samples and class imbalance
##N = float(df.count())
##Np =  float(dfp.count())
##Nn =  float(dfn.count())
##printc("DF:\nDimensionality: {}\tNumber of samples: {}\n".format(len(df.columns), N))
##printc("DF_POSITIVE CLASS:\nDimensionality: {}\tNumber of samples: {}\n".format(len(dfp.columns), Np))
##printc("DF_NEGATIVE CLASS:\nDimensionality: {}\tNumber of samples: {}\n".format(len(dfn.columns), Nn))
##printc("Class imbalance: P: {}% N: {}%".format(100 * Np / N, 100 * Nn / N))

# Now generate windows
# Verificar si existen varios registros para LEAVE = 0 y solo uno o pocos para LEAVE = 1

