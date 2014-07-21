from db import hana, postgis, spatialite, mysql
import output
import random

def run(numberOfExecutions):
	print "Polygons:\n"
	results = list()
	polygonID = 0
	hanaDB = hana.Hana()
	query = "SELECT COUNT(*) FROM BENCHMARK.POLYGONS"
	hanaDB.cursor.execute(query)
	result = hanaDB.cursor.fetchall()
	for row in result:
		polygonID = int(random.random() * int(str(row[0])))

	results.append(runMySQL(numberOfExecutions, str(polygonID)))
	# results.append(runPostgis(numberOfExecutions, [polygonID]))
	# results.append(runHana(numberOfExecutions, str(polygonID)))
	# results.append(runSpatialiteMain(numberOfExecutions, [polygonID]))
	
	print ""
	return results

def runMySQL(numberOfExecutions, polygonID):
	print('Started MySQL Benchmark')
	db = mysql.Mysql()
	results = db.runQueries(mysqlqueries(polygonID), numberOfExecutions)
	db.disconnect()
	print('Finished MySQL Benchmark')
	output.printSingleResult(results)
	return results

def runHana(numberOfExecutions, polygonID):
	print('Starting Hana Benchmark')
	hanaDB = hana.Hana()
	hanaResults = hanaDB.runQueries(hanaqueries(polygonID), numberOfExecutions)
	hanaDB.disconnect()
	print('Finished Hana Benchmark')
	output.printSingleResult(hanaResults)
	return hanaResults

def runPostgis(numberOfExecutions, params):
	print('Started postgis Benchmark')
	postgisDB = postgis.Postgis()
	postgisResults = postgisDB.runQueries(postgisqueries(), numberOfExecutions, params)
	postgisDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain(numberOfExecutions, params):
	print('Started spatialite Benchmark')
	db = spatialite.Spatialite(':memory:')
	db.loadDiskData('benchmark.db')
	spatialiteResults = db.runQueries(spatialitequeries(), numberOfExecutions, params)
	db.disconnect()
	print('Finished spatialite Benchmark')
	output.printSingleResult(spatialiteResults)
	return spatialiteResults

def mysqlqueries(polygonID):
	return [
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Equals(one.polygon, two.polygon) = 1 WHERE one.ID != two.ID AND one.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Disjoint(one.polygon, two.polygon) = 1 WHERE one.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Touches(one.polygon, two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN B_LINES two ON Touches(one.polygon, two.line) = 1 WHERE one.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_LINES one JOIN POLYGONS two ON Crosses(one.line, two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Overlaps(one.polygon, two.polygon) = 1 WHERE one.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_POINTS one JOIN POLYGONS two ON Within(one.point, two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_LINES one JOIN POLYGONS two ON Within(one.line, two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Within(one.polygon, two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Contains(one.polygon, two.polygon) = 1 WHERE two.ID = " + polygonID
		]

def postgisqueries():
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON ST_Equals(one.polygon, two.polygon) = True WHERE ONE.ID != Two.ID AND ONE.ID = %s",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON ST_Disjoint(one.polygon, two.polygon) = True WHERE ONE.ID = %s",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON ST_Touches(one.polygon, two.polygon) = True WHERE TWO.ID = %s",
		"SELECT COUNT(*) FROM POLYGONS one JOIN LINES two ON ST_Touches(one.polygon, two.line) = True WHERE ONE.ID = %s",
		"SELECT COUNT(*) FROM LINES one JOIN POLYGONS two ON ST_Crosses(one.line, two.polygon) = True WHERE two.ID = %s",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON ST_Overlaps(one.polygon, two.polygon) = True WHERE ONE.ID = %s",
		"SELECT COUNT(*) FROM Points one JOIN POLYGONS two ON ST_Within(one.point, two.polygon) = True WHERE TWO.ID = %s",
		"SELECT COUNT(*) FROM LINES one JOIN POLYGONS two ON ST_Within(one.line, two.polygon) = True WHERE two.ID = %s",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON ST_Within(one.polygon, two.polygon) = True WHERE TWO.ID = %s",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON ST_Contains(one.polygon, two.polygon) = True WHERE TWO.ID = %s"
		]

def hanaqueries(polygonID):
	return [
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.POLYGONS two ON one.polygon.ST_Equals(two.polygon) = 1 WHERE ONE.ID != Two.ID AND ONE.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.POLYGONS two ON one.polygon.ST_Disjoint(two.polygon) = 1 WHERE ONE.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.POLYGONS two ON one.polygon.ST_Touches(two.polygon) = 1 WHERE TWO.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.LINES two ON one.polygon.ST_Touches(two.line) = 1 WHERE ONE.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.LINES one JOIN BENCHMARK.POLYGONS two ON one.line.ST_Crosses(two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.POLYGONS two ON one.polygon.ST_Overlaps(two.polygon) = 1 WHERE ONE.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.B_Points one JOIN BENCHMARK.POLYGONS two ON one.point.ST_Within(two.polygon) = 1 WHERE TWO.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.LINES one JOIN BENCHMARK.POLYGONS two ON one.line.ST_Within(two.polygon) = 1 WHERE two.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.POLYGONS two ON one.polygon.ST_Within(two.polygon) = 1 WHERE TWO.ID = " + polygonID,
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.POLYGONS two ON one.polygon.ST_Contains(two.polygon) = 1 WHERE TWO.ID = " + polygonID
		]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Equals(one.polygon, two.polygon) = 1 WHERE ONE.ID != Two.ID AND ONE.ID = ?",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Disjoint(one.polygon, two.polygon) = 1 WHERE ONE.ID = ?",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Touches(one.polygon, two.polygon) = 1 WHERE TWO.ID = ?",
		"SELECT COUNT(*) FROM POLYGONS one JOIN LINES two ON Touches(one.polygon, two.line) = 1 WHERE ONE.ID = ?",
		"SELECT COUNT(*) FROM LINES one JOIN POLYGONS two ON Crosses(one.line, two.polygon) = 1 WHERE two.ID = ?",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Overlaps(one.polygon, two.polygon) = 1 WHERE ONE.ID = ?",
		"SELECT COUNT(*) FROM B_Points one JOIN POLYGONS two ON Within(one.point, two.polygon) = 1 WHERE TWO.ID = ?",
		"SELECT COUNT(*) FROM LINES one JOIN POLYGONS two ON Within(one.line, two.polygon) = 1 WHERE two.ID = ?",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Within(one.polygon, two.polygon) = 1 WHERE TWO.ID = ?",
		"SELECT COUNT(*) FROM POLYGONS one JOIN POLYGONS two ON Contains(one.polygon, two.polygon) = 1 WHERE TWO.ID = ?"
		]