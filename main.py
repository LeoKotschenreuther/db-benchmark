from db import mysql, postgis, spatialite, hana
import output
import soccerAnalytics
import nineIntersection
import random
import math

numberOfExecutions = 100
polygonSize = 5000
results = list()
areaLength = 100

# def runHana():
# 	print('Starting Hana Benchmark')
# 	hanaDB = hana.Hana()
# 	hanaResults = hanaDB.runQueries(hanaqueries(), numberOfExecutions)
# 	hanaDB.disconnect()
# 	print('Finished Hana Benchmark')
# 	output.printSingleResult(hanaResults)
# 	return hanaResults!

def runPostgis(polygons, areaPoints):
	print('Started postgis Benchmark')
	postgisDB = postgis.Postgis()
	postgisDB.insertPolygons(polygons)
	print('\tInserted the polygons into a table')
	areaString = postgisDB.polygonString(areaPoints)
	postgisResults = postgisDB.runQueries(nineIntersection.postgisqueries(areaString), numberOfExecutions)
	postgisDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain():
	print('Started spatialite Benchmark')
	spatialiteDB = spatialite.Spatialite()
	spatialiteDB.insertPolygons(polygons)
	print('\tInserted the polygons into a table')
	areaString = spatialiteDB.polygonString(areaPoints)
	spatialiteResults = postgisDB.runQueries(nineIntersection.postgisqueries(areaString), numberOfExecutions)
	spatialiteDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(spatialiteResults)
	return spatialiteResults

def createPolygon(numPoints, midX, midY, areaLength):
	if numPoints < 3:
		return False

	points = list()
	step = 2 * math.pi / numPoints
	for i in range(0, numPoints):
		distanceMid = random.random() * areaLength
		if distanceMid == 0: distanceMid = 0.1 * areaLength
		x = midX + math.cos(step*i) * distanceMid
		y = midY + math.sin(step*i) * distanceMid
		if x > areaLength : x = areaLength
		if x < -areaLength : x = -areaLength
		if y > areaLength : y = areaLength
		if y < -areaLength : y = -areaLength
		points.append({'x': x, 'y': y})
	# points.append(points[0])
	# the first point doesn't get inserted a second time, we will handle that in the db-files
	return points

def runSoccerAnalyticsWorkload():
	results.append(soccerAnalytics.runHana(numberOfExecutions))
	# results.append(soccerAnalytics.runMySQL(numberOfExecutions))
	# results.append(soccerAnalytics.runPostgis(numberOfExecutions))
	# results.append(soccerAnalytics.runSpatialiteMain(numberOfExecutions))

def run9IntersectionWorkload():
	polygonIsValid = False
	polygonsIntersect = False
	polygons = list()
	polygon1 = list()
	while not polygonIsValid:
		polygon1 = createPolygon(polygonSize, -1, -1, areaLength)
		# check whether they intersect:
		# hanaDB = hana.Hana()
		# polygonsNotValid = hanaDB.checkIntersection(polygons)
		# hanaDB.disconnect()
		postgisDB = postgis.Postgis()
		polygonIsValid = postgisDB.isPolygonValid(polygon1)
		postgisDB.disconnect()

	polygons.append(polygon1)
	polygon2 = list()
	polygonIsValid = False
	while not polygonIsValid and not polygonsIntersect:
		polygon2 = createPolygon(polygonSize, 1, 1, areaLength)
		postgisDB = postgis.Postgis()
		polygonIsValid = postgisDB.isPolygonValid(polygon2)
		polygonsIntersect = postgisDB.checkIntersection([polygon1, polygon2])
		postgisDB.disconnect()

	polygons.append(polygon2)

	# print(polygons)

	print("Created two valid polygons")
	areaPoints = list()
	areaPoints.append({'x': -areaLength, 'y': areaLength})
	areaPoints.append({'x': areaLength, 'y': areaLength})
	areaPoints.append({'x': areaLength, 'y': -areaLength})
	areaPoints.append({'x': -areaLength, 'y': -areaLength})
	areaPoints.append({'x': -areaLength, 'y': areaLength})

	# Create table in each db, insert polygons and measure results
	# MySQL has no Intersectino function, so we can't test the nine intersection model
	results.append(runPostgis(polygons, areaPoints))
	# the following do not work yet
	# runSpatialite()
	# runHana()


	# Check relations

def printResultsToFile():
	print('Start printing results')
	output.printSummary(results)
	print('Finished printing results')

runSoccerAnalyticsWorkload()
# run9IntersectionWorkload()

printResultsToFile()

