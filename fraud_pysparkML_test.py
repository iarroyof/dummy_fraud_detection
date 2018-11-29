from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator as Evaluator
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import LogisticRegression
import warnings
import pyspark
import platform
#from pyspark.sql import Row

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

sc = SparkContext.getOrCreate()
## spark-submit C:\Users\HCAOA911\Documents\PythonScripts\csv_toMLLIB.py 
## C:\\Users\\HCAOA911\\Desktop\\data\\fraud_data_sample.csv
warnings.filterwarnings('ignore')

def vectorizeData(data, validCols, labelsCol):
    #return data.rdd.map(lambda r: [int(r[-1]), Vectors.dense(r[:-1])]).toDF(['label','features'])

    vectorizer = VectorAssembler(
        inputCols=validCols,
        outputCol='features'
        )

    return vectorizer.transform(data).withColumn('label', 
                                                data[labelsCol].cast(IntegerType())
                                                ).select(['features', 'label'])


def labelData(data):
    """Only for mllib api. Use vectorizeData with ml api"""
    return data.rdd.map(lambda row: LabeledPoint(row[-1], row[:-1])).toDF()


#in_file = "C:\\Users\\HCAOA911\\Desktop\\data\\PS_20174392719_1491204439457_log.csv"
if platform.system() != 'Linux':
    in_file = "data\\fraud_data_sample.csv"
else:
    in_file = "data/fraud_data_sample.csv"

CV_data = spark.read.csv(in_file, header=True, inferSchema=True) 
to_vectorize = ['step','amount','oldbalanceOrg','newbalanceOrig',
                'oldbalanceDest','newbalanceDest','isFlaggedFraud']

training_data, testing_data = CV_data.randomSplit([0.8, 0.2])

#xytrain = labelData(training_data)

xytrain = vectorizeData(training_data, validCols=to_vectorize, labelsCol='isFraud')

#model = LogisticRegressionWithLBFGS.train(xytrain)
lr = LogisticRegression(regParam=0.01)
model = lr.fit(xytrain)

xytest = vectorizeData(testing_data, validCols=to_vectorize, labelsCol='isFraud')
predicted_train = model.transform(xytrain)
predicted_test = model.transform(xytest)
evaluator = Evaluator()

print("Train %s: %f" % (evaluator.getMetricName(), evaluator.evaluate(predicted_train)))
print("Test %s: %f" % (evaluator.getMetricName(), evaluator.evaluate(predicted_test)))
