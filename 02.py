from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark import SparkContext
import numpy as np

sc = SparkContext(master = "local", appName="TransformacionesyAcciones")
#rdd1 = sc.parallelize([1,2,3])
#print(rdd1.collect())


#rdd2 = sc.parallelize(np.array(range(100)),2)
#print(rdd2.collect())

path = "./files/"
equiposOlimpicosRDD = sc.textFile(path+"paises.csv",2) \
                        .map(lambda line : line.split(","))
#print(equiposOlimpicosRDD.take(100))

num_equipos = equiposOlimpicosRDD.map(lambda x : (x[2])).distinct().count()
print(num_equipos)

equipos_by_pais = equiposOlimpicosRDD.map(lambda x : (x[2],x[1])) \
                                     .groupByKey().mapValues(len).take(5)
print(equipos_by_pais)
equipos_by_pais2 = equiposOlimpicosRDD.map(lambda x : (x[2],x[1])) \
                                     .groupByKey().mapValues(list).take(5)
print(equipos_by_pais2)

equiposArgentinos = equiposOlimpicosRDD.filter(lambda l : "ARG" in l).collect()
print(equiposArgentinos)

print(equiposOlimpicosRDD.count())  # puede tardar mucho si es una gran cantidad de registros

print(equiposOlimpicosRDD.countApprox()) # da una cantidad aproximada de registros en menos tiempo.



sc.stop()