from db import hana, postgis, spatialite, mysql, monetdb_db
import output
import random

def run(numberOfExecutions):
	print "Points:\n"
	results = list()
	pointID = 0
	monetDB = monetdb_db.Monetdb()
	query = "SELECT COUNT(*) FROM POINTS"
	monetDB.cursor.execute(query)
	result = monetDB.cursor.fetchall()
	for row in result:
		pointID = int(random.random() * int(str(row[0])))

	# results.append(runMySQL(numberOfExecutions, str(pointID)))
	# results.append(runPostgis(numberOfExecutions, [pointID]))
	# results.append(runHana(numberOfExecutions, str(pointID)))
	# results.append(runSpatialiteMain(numberOfExecutions, [pointID]))
	results.append(runMonetDB(numberOfExecutions, str(pointID)))
	
	print ""
	return results

def runMySQL(numberOfExecutions, pointID):
	print('Started MySQL Benchmark')
	db = mysql.Mysql()
	results = db.runQueries(mysqlqueries(pointID), numberOfExecutions)
	db.disconnect()
	print('Finished MySQL Benchmark')
	output.printSingleResult(results)
	return results

def runHana(numberOfExecutions, pointID):
	print('Started Hana Benchmark')
	hanaDB = hana.Hana()
	hanaResults = hanaDB.runQueries(hanaqueries(pointID), numberOfExecutions)
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

def runMonetDB(numberOfExecutions, pointID):
	print('Started MonetDB Benchmark')
	db = monetdb_db.Monetdb()
	results = db.runQueries(monetdbqueries(pointID), numberOfExecutions)
	db.disconnect()
	print('Finished MonetDB Benchmark')
	output.printSingleResult(results)
	return results

def mysqlqueries(pointID):
	return [
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_POINTS one JOIN B_POINTS two ON Equals(one.point, two.point) = 1 WHERE one.ID != two.ID AND two.ID = " + pointID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN B_POINTS two ON Intersects(one.polygon, two.point) = 1 WHERE two.ID = " + pointID
		]

def postgisqueries():
	return [
		"SELECT COUNT(*) FROM POINTS one JOIN POINTS two ON ST_Equals(one.point, two.point) = True WHERE ONE.ID != TWO.ID AND ONE.ID = %s",
		"SELECT COUNT(*) FROM Polygons one JOIN POINTS two ON ST_Intersects(one.polygon, two.point) = True WHERE TWO.ID = %s",
		"SELECT COUNT(*) FROM POINTS one JOIN (SELECT point FROM POINTS WHERE ID = %s) two ON ST_DWithin(one.point, two.point, 50) = True"
		]

def hanaqueries(pointID):
	return [
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS one JOIN BENCHMARK.B_POINTS two ON one.point.ST_Equals(two.point) = 1 WHERE ONE.ID != TWO.ID AND ONE.ID = " + pointID,
		"SELECT COUNT(*) FROM BENCHMARK.Polygons one JOIN BENCHMARK.B_POINTS two ON one.polygon.ST_Intersects(two.point) = 1 WHERE TWO.ID = " + pointID,
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS one JOIN (SELECT point FROM BENCHMARK.B_POINTS WHERE ID = " + pointID + ") two ON one.point.ST_WithinDistance(two.point, 50) = 1"
		]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM B_POINTS one JOIN B_POINTS two ON Equals(one.point, two.point) = 1 WHERE ONE.ID != TWO.ID AND ONE.ID = ?",
		"SELECT COUNT(*) FROM Polygons one JOIN B_POINTS two ON Intersects(one.polygon, two.point) = 1 WHERE TWO.ID = ?",
		"SELECT COUNT(*) FROM B_POINTS one JOIN (SELECT point FROM B_POINTS WHERE ID = ?) two ON Distance(one.point, two.point) <= 50"
		]

def monetdbqueries(pointID):
	return [
		"SELECT COUNT(*) FROM POINTS one JOIN POINTS two ON Equals(one.point, two.point) = TRUE  WHERE ONE.ID <> TWO.ID AND ONE.ID = " + pointID,
		"SELECT COUNT(*) FROM Polygons one JOIN POINTS two ON \"Intersect\"(one.polygon, two.point) = TRUE WHERE TWO.ID = " + pointID,
		"SELECT COUNT(*) FROM POINTS one JOIN (SELECT point FROM POINTS WHERE ID = " + pointID + ") two ON Distance(one.point, two.point) <= 50"
		]