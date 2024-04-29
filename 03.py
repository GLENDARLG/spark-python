from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark import SparkContext
import numpy as np

sc = SparkContext(master = "local", appName="TransformacionesyAcciones")
path = "./files/"

equiposOlimpicosRDD = sc.textFile(path+"paises.csv",2) \
                        .map(lambda line : line.split(","))

deportistaOlimpicoRDD = sc.textFile(path + "deportista.csv") \
                          .map(lambda line : line.split(",")) 

deportistaOlimpicoRDD2 = sc.textFile(path + "deportista2.csv") \
                            .map(lambda line : line.split(",")) 

deportistaOlimpicoRDD = deportistaOlimpicoRDD.union(deportistaOlimpicoRDD2)
#print(deportistaOlimpicoRDD.count())
#Join por Id del equipo
joinRDD1 = deportistaOlimpicoRDD.map(lambda x :[x[-1], x[:-1]]).join(equiposOlimpicosRDD.map(lambda x : [x[0],x[2]]))
#print(joinRDD1.takeSample(False,6,25))
#take(n) toma los primeros
#takeSample() toma una muestra aleatoria sirve para validar data de la operación

resultadoGanador = sc.textFile(path+"resultados.csv").map(lambda line : line.split(","))
resultadoGanador = resultadoGanador.map(lambda l : [l[2],l[1]])

jugadoresMedalla =  joinRDD1.join(resultadoGanador)
print(jugadoresMedalla.takeSample(False,6,25))
#print(resultadoGanador.takeSample(False,6,25))

valoresMedalla = {'Gold':7, 'Silver':5, 'Bronze':4}
paisesMedalla = joinRDD1.join(resultadoGanador)
print(paisesMedalla.take(10))


sc.stop()