from db import hana, postgis, spatialite, mysql, monetdb_db
import output
import random

def run(numberOfExecutions):
	print "Lines:\n"
	results = list()
	lineID = 0
	monetDB = monetdb_db.Monetdb()
	query = "SELECT COUNT(*) FROM LINES"
	monetDB.cursor.execute(query)
	result = monetDB.cursor.fetchall()
	for row in result:
		lineID = int(random.random() * int(str(row[0])))

	# results.append(runMySQL(numberOfExecutions, str(lineID)))
	# results.append(runPostgis(numberOfExecutions, [lineID]))
	# results.append(runHana(numberOfExecutions, str(lineID)))
	# results.append(runSpatialiteMain(numberOfExecutions, [lineID]))
	results.append(runMonetDB(numberOfExecutions, str(lineID)))
	
	print ""
	return results

def runMySQL(numberOfExecutions, lineID):
	print('Started MySQL Benchmark')
	db = mysql.Mysql()
	results = db.runQueries(mysqlqueries(lineID), numberOfExecutions)
	db.disconnect()
	print('Finished MySQL Benchmark')
	output.printSingleResult(results)
	return results

def runHana(numberOfExecutions, lineID):
	print('Starting Hana Benchmark')
	hanaDB = hana.Hana()
	hanaResults = hanaDB.runQueries(hanaqueries(lineID), numberOfExecutions)
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

def runMonetDB(numberOfExecutions, lineID):
	print('Started MonetDB Benchmark')
	db = monetdb_db.Monetdb()
	results = db.runQueries(monetdbqueries(lineID), numberOfExecutions)
	db.disconnect()
	print('Finished MonetDB Benchmark')
	output.printSingleResult(results)
	return results

def mysqlqueries(lineID):
	return [
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN B_LINES two ON Intersects(one.polygon, two.line) = 1 WHERE two.ID = " + lineID,
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_POINTS one JOIN B_LINES two ON Intersects(one.point, two.line) = 1 WHERE two.ID = " + lineID, 
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_LINES one JOIN B_LINES two ON Crosses(one.line, two.line) = 1 WHERE one.ID = " + lineID
		]

def postgisqueries():
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN LINES two ON ST_Intersects(one.polygon, two.line) = True WHERE TWO.ID = %s",
		"SELECT COUNT(*) FROM POINTS one JOIN LINES two ON ST_Intersects(one.point, two.line) = True WHERE two.ID = %s", 
		"SELECT COUNT(*) FROM LINES one JOIN LINES two ON ST_Crosses(one.line, two.line) = True WHERE one.ID = %s"
		]

def hanaqueries(lineID):
	return [
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN BENCHMARK.LINES two ON one.polygon.ST_Intersects(two.line) = 1 WHERE TWO.ID = " + lineID,
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS one JOIN BENCHMARK.LINES two ON one.point.ST_Intersects(two.line) = 1 WHERE two.ID = " + lineID, 
		"SELECT COUNT(*) FROM BENCHMARK.LINES one JOIN BENCHMARK.LINES two ON one.line.ST_Crosses(two.line) = 1 WHERE one.ID = " + lineID
		]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN LINES two ON Intersects(one.polygon, two.line) = 1 WHERE TWO.ID = ?",
		"SELECT COUNT(*) FROM B_POINTS one JOIN LINES two ON Intersects(one.point, two.line) = 1 WHERE two.ID = ?", 
		"SELECT COUNT(*) FROM LINES one JOIN LINES two ON Crosses(one.line, two.line) = 1 WHERE one.ID = ?"
		]

def monetdbqueries(lineID):
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN LINES two ON \"Intersect\"(one.polygon, two.line) = TRUE WHERE TWO.ID = " + lineID,
		"SELECT COUNT(*) FROM POINTS one JOIN LINES two ON \"Intersect\"(one.point, two.line) = TRUE WHERE two.ID = " + lineID, 
		"SELECT COUNT(*) FROM LINES one JOIN LINES two ON Crosses(one.line, two.line) = TRUE WHERE one.ID = " + lineID
		]