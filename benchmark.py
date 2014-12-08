from microBenchmark import microBenchmark
from macroBenchmark import macroBenchmark
import dataCreation
import output
import time

numberOfExecutions = 50
areaLength = 10000

results = list()

start = time.time()
# dataCreation.createData(areaLength)
results = microBenchmark.run(numberOfExecutions, areaLength)
end = time.time()
print (end - start)
# results = macroBenchmark.run(numberOfExecutions)

print('Start printing results')
# output.print9ISummary(results)
print('Finished printing results')