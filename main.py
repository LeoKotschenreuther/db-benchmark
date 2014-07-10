from db import mysql, postgis, spatialite, hana
from microBenchmark import microBenchmark
from macroBenchmark import macroBenchmark
import dataCreation
import output
import random
import math

numberOfExecutions = 1
areaLength = 100000

results = list()

# dataCreation.createData(areaLength)

results = microBenchmark.run(numberOfExecutions, areaLength)
# results = macroBenchmark.run(numberOfExecutions)

print('Start printing results')
output.printSummary(results)
print('Finished printing results')