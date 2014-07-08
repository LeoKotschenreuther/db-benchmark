from db import mysql, postgis, spatialite, hana
import dataCreation
import output
import soccerAnalytics
import nineIntersection
import random
import math

numberOfExecutions = 100
polygonSizes = [10, 50, 200, 1000]
areaLength = 100000
resetTables = False


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
	print('Finished spatialite Benchmark')
	output.printSingleResult(spatialiteResults)
	return spatialiteResults

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
		areaPoints = list()
		areaPoints.append({'x': -areaLength, 'y': areaLength})
		areaPoints.append({'x': areaLength, 'y': areaLength})
		areaPoints.append({'x': areaLength, 'y': -areaLength})
		areaPoints.append({'x': -areaLength, 'y': -areaLength})

		# MySQL has no Intersection function, so we can't test the nine intersection model
		# results.append(runPostgis(polygonSize, polygons, areaPoints))
		# results.append(runHana(polygonSize, polygons, areaPoints))
		results.append(runSpatialiteMain(polygonSize, polygons, areaPoints))
	
	output.print9ISummary(results)

def printResultsToFile():
	print('Start printing results')
	output.printSummary(results)
	print('Finished printing results')

# dataCreation.createPoints(areaLength)
dataCreation.createPolygons(resetTables, polygonSizes, areaLength)
# dataCreation.createLines()


# runSoccerAnalyticsWorkload()
# run9IntersectionWorkload()

# db = spatialite.Spatialite(':memory:')
# db.setUpDB(False)
# db.disconnect()
