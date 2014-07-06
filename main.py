from db import mysql, postgis, spatialite, hana
import output
import soccerAnalytics
import nineIntersection
import random
import math

numberOfExecutions = 100
polygonSizes = [
	10,
	# 2000,
]
# polygonSize = 3000
areaLength = 100

def runHana(polygonSize, polygons, areaPoints):
	print('Starting Hana Benchmark')
	hanaDB = hana.Hana()
	hanaDB.insertPolygons(polygons, polygonSize)
	print('\tInserted the polygons into a table')
	areaString = hanaDB.polygonString(areaPoints)
	hanaResults = hanaDB.runQueriesPoly(nineIntersection.hanaqueries(areaString, polygonSize), numberOfExecutions, polygonSize)
	hanaDB.disconnect()
	print('Finished Hana Benchmark')
	output.printSingleResult(hanaResults)
	return hanaResults

def runPostgis(polygonSize, polygons, areaPoints):
	print('Started postgis Benchmark')
	postgisDB = postgis.Postgis()
	postgisDB.insertPolygons(polygons, polygonSize)
	print('\tInserted the polygons into a table')
	areaString = postgisDB.polygonString(areaPoints)
	postgisResults = postgisDB.runQueriesPoly(nineIntersection.postgisqueries(areaString, polygonSize), numberOfExecutions, polygonSize)
	postgisDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain(polygonSize, polygons, areaPoints):
	print('Started spatialite Benchmark')
	spatialiteDB = spatialite.Spatialite(':memory:')
	spatialiteDB.insertPolygons(polygons)
	print('\tInserted the polygons into a table')
	areaString = spatialiteDB.polygonString(areaPoints)
	spatialiteResults = spatialiteDB.runQueriesPoly(nineIntersection.spatialitequeries(areaString), numberOfExecutions, polygonSize)
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
	results = list()
	results.append(soccerAnalytics.runHana(numberOfExecutions))
	# results.append(soccerAnalytics.runMySQL(numberOfExecutions))
	# results.append(soccerAnalytics.runPostgis(numberOfExecutions))
	# results.append(soccerAnalytics.runSpatialiteMain(numberOfExecutions))
	output.printSoccerSummary(results)

def run9IntersectionWorkload():
	results = list()
	# hanaDB = hana.Hana()
	# hanaDB.dropCreateTable('BENCHMARK.POLYGONS')
	# hanaDB.disconnect()
	postgisDB = postgis.Postgis()
	postgisDB.dropCreateTable('POLYGONS')
	postgisDB.disconnect()
	for polygonSize in polygonSizes:
		polygonIsValid = False
		polygonsIntersect = False
		polygons = list()
		polygon1 = list()
		while not polygonIsValid:
			polygon1 = createPolygon(polygonSize, -1, -1, areaLength)
			# check whether they intersect:
			# hanaDB = hana.Hana()
			# polygonIsValid = hanaDB.isPolygonValid(polygon1)
			# hanaDB.disconnect()
			postgisDB = postgis.Postgis()
			polygonIsValid = postgisDB.isPolygonValid(polygon1)
			postgisDB.disconnect()
			# spatialiteDB = spatialite.Spatialite(':memory:')
			# polygonIsValid = spatialiteDB.isPolygonValid(polygon1)
			# spatialiteDB.disconnect()

		polygons.append(polygon1)
		polygon2 = list()
		polygonIsValid = False
		while not polygonIsValid and not polygonsIntersect:
			polygon2 = createPolygon(polygonSize, 1, 1, areaLength)
			# hanaDB = hana.Hana()
			# polygonsNotValid = hanaDB.isPolygonValid(polygon2)
			# polygonsIntersect = hanaDB.checkIntersection([polygon1, polygon2])
			# hanaDB.disconnect()
			postgisDB = postgis.Postgis()
			polygonIsValid = postgisDB.isPolygonValid(polygon2)
			polygonsIntersect = postgisDB.checkIntersection([polygon1, polygon2])
			postgisDB.disconnect()
			# spatialiteDB = spatialite.Spatialite(':memory:')
			# polygonsNotValid = spatialiteDB.isPolygonValid(polygon2)
			# polygonsIntersect = spatialiteDB.checkIntersection([polygon1, polygon2])
			# spatialiteDB.disconnect()

		polygons.append(polygon2)

		for x in range(0, 9998):
			polygon = list()
			polygonIsValid = False
			while not polygonIsValid:
				polygon = createPolygon(polygonSize, 0, 0, areaLength)
				postgisDB = postgis.Postgis()
				polygonIsValid = postgisDB.isPolygonValid(polygon1)
				postgisDB.disconnect()
			polygons.append(polygon)
			if x % 50 == 0:
				print "finished: " + str(x)

		# print(polygons)

		print("Created two valid polygons")
		areaPoints = list()
		areaPoints.append({'x': -areaLength, 'y': areaLength})
		areaPoints.append({'x': areaLength, 'y': areaLength})
		areaPoints.append({'x': areaLength, 'y': -areaLength})
		areaPoints.append({'x': -areaLength, 'y': -areaLength})

		# Create table in each db, insert polygons and measure results
		# MySQL has no Intersectino function, so we can't test the nine intersection model
		results.append(runPostgis(polygonSize, polygons, areaPoints))
		results.append(runHana(polygonSize, polygons, areaPoints))
		results.append(runSpatialiteMain(polygonSize, polygons, areaPoints))
	
	output.print9ISummary(results)

def printResultsToFile():
	print('Start printing results')
	output.printSummary(results)
	print('Finished printing results')


# runSoccerAnalyticsWorkload()
run9IntersectionWorkload()

# db = postgis.Postgis()
# db.test()
# db.disconnect()
