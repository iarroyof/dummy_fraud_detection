from pyspark.sql.types import IntegerType
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator as Evaluator
from pyspark.mllib.regression import LabeledPoint
from pyspark.ml.classification import LogisticRegression
import warnings
import pyspark
import argparse


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

## Commands and paths:
## spark-submit C:\Users\HCAOA911\Documents\PythonScripts\csv_toMLLIB.py 
## in_file = "C:\\Users\\HCAOA911\\Desktop\\data\\PS_20174392719_1491204439457_log.csv"
## in_file = "../data/PS_20174392719_1491204439457_log.csv"
## in_file = "C:\\Users\\HCAOA911\\Desktop\\data\\fraud_data_sample.csv"

warnings.filterwarnings('ignore')

def vectorizeData(data, validCols, labelsCol):
    """Creates dataset from spark DataFrame getting only two columns 'label' 
    and 'features'. Alist of valid column names must be provided in 
    'validCols' and the original name of the labels in 'labelsCol'."""
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test fraud detection script in "
                                                    "credit card payment.")
    parser.add_argument("--csv", help = "Input file containing features and "
                                                        "labels in columns")
    args = parser.parse_args()

    in_file = args.csv
    CV_data = spark.read.csv(in_file, header=True, inferSchema=True) 
    to_vectorize = ['step','amount','oldbalanceOrg','newbalanceOrig',
                'oldbalanceDest','newbalanceDest','isFlaggedFraud']

    training_data, testing_data = CV_data.randomSplit([0.8, 0.2])

#xytrain = labelData(training_data)

    xytrain = vectorizeData(training_data, validCols=to_vectorize, labelsCol='isFraud')
    lr = LogisticRegression(regParam=0.01)
    model = lr.fit(xytrain)

    xytest = vectorizeData(testing_data, validCols=to_vectorize, labelsCol='isFraud')
    predicted_train = model.transform(xytrain)
    predicted_test = model.transform(xytest)
    evaluator = Evaluator()

    print("Train %s: %f" % (evaluator.getMetricName(), evaluator.evaluate(predicted_train)))
    print("Test %s: %f" % (evaluator.getMetricName(), evaluator.evaluate(predicted_test)))
