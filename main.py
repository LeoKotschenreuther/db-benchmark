from db import mysql, postgis, spatialite, hana
from microBenchmark import microBenchmark
from macroBenchmark import macroBenchmark
import dataCreation
import output
import random
import math
import time

numberOfExecutions = 1
areaLength = 100000

results = list()

dataCreation.createData(areaLength)

# results = microBenchmark.run(numberOfExecutions, areaLength)
# results = macroBenchmark.run(numberOfExecutions)

print('Start printing results')
# output.printSummary(results)
print('Finished printing results')

# db = postgis.Postgis()
# db = spatialite.Spatialite('benchmark.db')
# query = "SELECT ST_Equals(one.point, two.point) FROM (SELECT * FROM POINTS WHERE ID = 0) one, (SELECT * FROM POINTS WHERE ID = 99999) two"
# query = "SELECT ONE.ID, TWO.ID, one.X, one.Y, one.POINT FROM (SELECT ID, X, Y, POINT FROM B_POINTS) one JOIN B_POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID"
# start = time.time()
# result = db.cursor.execute(query)
# end = time.time()
# print "time: " + str(end-start)
# for row in db.cursor:
# for row in result:
# 	print row