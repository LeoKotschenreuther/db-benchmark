import nineIntersection
import pointPoint
import lineLinePoint
import point
import line
import polygon
import spatialAnalysis

def run(numberOfExecutions, areaLength):
	# results = nineIntersection.run(numberOfExecutions, areaLength)
	results = point.run(numberOfExecutions)
	results = line.run(numberOfExecutions)
	results = polygon.run(numberOfExecutions)
	results = spatialAnalysis.run(numberOfExecutions)
	return results