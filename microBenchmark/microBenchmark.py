import nineIntersection
import pointPoint
import lineLinePoint
import point
import line
import polygon
import spatialAnalysis

def run(numberOfExecutions, areaLength):
	results = list()
	# results = nineIntersection.run(numberOfExecutions, areaLength)
	# results.extend(point.run(numberOfExecutions))
	# results.extend(line.run(numberOfExecutions))
	# results.extend(polygon.run(numberOfExecutions))
	results.extend(spatialAnalysis.run(numberOfExecutions))
	return results