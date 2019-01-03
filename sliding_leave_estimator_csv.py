
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator as Evaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
import pyspark.sql.functions as F
from itertools import chain
import datetime
import json
import sys


def printc(s):
    beg = '\x1b[6;30;42m'
    end = '\x1b[0m'
    print(beg + s + end)


def vectorizeData(df, labelsCol, weighClass=False, featsCol=None):
    """Creates dataset from spark DataFrame of mixed categorical and numerical
    features. The function returns only two columns 'label' and 'features'. The 
    input Spark dataframe is 'df'. The column name corresponding to the training 
    labels must be provided in 'labelsCol'."""
    #if not weighClass is None:
    assert labelsCol in df.columns  # 'weighClass' must be binary (0 or 1)

    stringColList = [i[0] for i in df.dtypes if i[1] == 'string' if i[0] != labelsCol]
    # Indexing categorical features (string types)
    indexedCategoricalCols = [categoricalCol + "Index" for categoricalCol in stringColList]
    stages = [StringIndexer(inputCol = categoricalCol, outputCol = idx_categoricalCol) 
                        for categoricalCol, idx_categoricalCol in zip(stringColList, 
                                                                   indexedCategoricalCols)]
    indexer = Pipeline(stages=stages)
    df = indexer.fit(df).transform(df)

    # Assembling indexed and numeric features
    numericColList = [i[0] for i in df.dtypes if i[1] != 'string']
    assemblerInputs = indexedCategoricalCols + numericColList
    assembler = VectorAssembler(inputCols=assemblerInputs, 
                                outputCol="features" if featsCol is None else featsCol)
    df = assembler.transform(df) 
    # Indexing binary labels
    labeller = StringIndexer(inputCol=labelsCol, outputCol="label").fit(df)
    df = labeller.transform(df).select(["features" if featsCol is None else featsCol, "label"])

    if weighClass:
        from pyspark.sql.functions import when
        from sklearn.utils.class_weight import compute_class_weight as weigh
        labels = [int(i.label) for i in df.select('label').collect()]
        wC0, wC1 = list(weigh(class_weight='balanced', classes=[0.0, 1.0], y=labels))
        return assemblerInputs, df.withColumn('weight', when(df.label==0.0, wC0).otherwise(wC1))
    else:
        return assemblerInputs, df


LEAVE = "Death"
leave = "MUERTO"
relation = "nomina"  # "voz", "todo"

remove = [
    "NOMBRE_TITULAR",
    "NOMBRE_ASEGURADO",
    "HSID_PENSIONES_CLIENTE",
    "HSID_PENSIONES_CLIENTE",
    "CLIENTE_ID",
    "NUMERO_CLIENTE_ID",
    "POLIZA", 
    "POLIZA_ID",
    "REGIMEN_SEG_SOCIAL",
    "REGIMEN_SEG_SOCIAL_ID",
    "COLONIA",
    "FECHA_PROCESO",
    "FECHA_EMISION",
    "FECHA_SOLICITUD",
    "FECHA_NACIMIENTO",
    "FECHA_INICIO_DERECHO",
    "OBSERVACIONES",
    "FECHA_MOVIMIENTO",
    "FECHA_RESOLUCION",
    "FECHA_VIGENCIA",
    "FECHA_BAJA",
    "FECHA_ACTUALIZACION",
    "CEDULA",
    "FECNAC_TITULAR",
    "REGIMEN_SEG_SOCIAL",
    "CLIENTE_ID",
    "ID_PAIS",
    "CARGA_DT",
    "VIG_INI_DT",
    "VIG_FIN_DT",
    "POLIZA_ID",
    "LOCALIDAD",
    "RI",
    "COMENTARIO",
    "REGIMEN_SEG_SOCIAL_ID"
]


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .getOrCreate()


fileStore = sys.argv[1]

df = spark.read.format("csv")\
          .options(inferSchema=True, header=True)\
          .load(fileStore)
valids = [v for v in df.columns if not v in remove]
df = df.select(valids + [LEAVE])

inputs, df = vectorizeData(df=df, labelsCol=LEAVE)
train, test = df.randomSplit([0.7, 0.3], seed=12345)

# Train Logistic Regression
lr = LogisticRegression(regParam=0.01)
lr = lr.fit(train)
# Make predictions.
predictions = lr.transform(test)
evaluator = Evaluator()
# Select example rows to display.
#predictions.select("prediction", "label", "features").show()
# Evaluate the learned model
print("Pensiones random deads Test %s: %f"    % (evaluator.getMetricName(), evaluator.evaluate(predictions)))
