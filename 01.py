import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = (SparkSession
    .builder
    .appName("PythonMnMCount")
    .getOrCreate())

spark.stop()


print('paso')