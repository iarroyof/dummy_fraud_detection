from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.evaluation import BinaryClassificationEvaluator as Evaluator
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import LogisticRegression
import warnings

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

warnings.filterwarnings('ignore')

def vectorizeData(data):
    return data.rdd.map(lambda r: [int(r[-1]), Vectors.dense(r[:-1])]).toDF(['label','features'])



in_file = "C:\\Users\\HCAOA911\\Desktop\\data\\small_sample.csv"

CV_data = spark.read.csv(in_file, header=True) 

CV_data = CV_data[['step','amount','oldbalanceOrg','newbalanceOrig',
                'oldbalanceDest','newbalanceDest','isFlaggedFraud', 'isFraud']]
training_data, testing_data = CV_data.randomSplit([0.8, 0.2])

xytrain = vectorizeData(training_data)

lr = LogisticRegression(regParam=0.01)
model = lr.fit(xytrain)

xytest = vectorizeData(testing_data)
predicted_train = model.transform(xytrain)
predicted_test = model.transform(xytest)
evaluator = Evaluator()

print("Train %s: %f" % (evaluator.getMetricName(), evaluator.evaluate(predicted_train)))
print("Test %s: %f" % (evaluator.getMetricName(), evaluator.evaluate(predicted_test)))
