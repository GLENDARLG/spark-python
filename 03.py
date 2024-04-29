from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark import SparkContext
import numpy as np

sc = SparkContext(master = "local", appName="TransformacionesyAcciones")


sc.stop()