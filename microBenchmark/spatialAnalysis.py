from db import hana, postgis, spatialite, mysql
import output

def run(numberOfExecutions):
	print "Spatial Analysis:\n"
	results = list()

	results.append(runMySQL(numberOfExecutions))
	# results.append(runPostgis(numberOfExecutions))
	# results.append(runHana(numberOfExecutions))
	# results.append(runSpatialiteMain(numberOfExecutions))
	
	print ""
	return results

def runMySQL(numberOfExecutions):
	print('Started MySQL Benchmark')
	db = mysql.Mysql()
	results = db.runQueries(mysqlqueries(), numberOfExecutions)
	db.disconnect()
	print('Finished MySQL Benchmark')
	output.printSingleResult(results)
	return results

def runHana(numberOfExecutions):
	print('Starting Hana Benchmark')
	hanaDB = hana.Hana()
	hanaResults = hanaDB.runQueries(hanaqueries(), numberOfExecutions)
	hanaDB.disconnect()
	print('Finished Hana Benchmark')
	output.printSingleResult(hanaResults)
	return hanaResults

def runPostgis(numberOfExecutions):
	print('Started postgis Benchmark')
	postgisDB = postgis.Postgis()
	postgisResults = postgisDB.runQueries(postgisqueries(), numberOfExecutions, None)
	postgisDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain(numberOfExecutions):
	print('Started spatialite Benchmark')
	db = spatialite.Spatialite(':memory:')
	db.loadDiskData('benchmark.db')
	spatialiteResults = db.runQueries(spatialitequeries(), numberOfExecutions, None)
	db.disconnect()
	print('Finished spatialite Benchmark')
	output.printSingleResult(spatialiteResults)
	return spatialiteResults

def mysqlqueries():
	return [
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN (SELECT LINE FROM B_LINES ORDER BY GLength(line) DESC LIMIT 1) two ON Intersects(one.polygon, two.line) = 1",
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_LINES one JOIN (SELECT POLYGON FROM POLYGONS ORDER BY Area(polygon) DESC LIMIT 1) two ON Intersects(one.line, two.polygon) = 1",
		"SELECT SQL_NO_CACHE COUNT(*) FROM B_POINTS one JOIN (SELECT polygon FROM POLYGONS ORDER BY Area(polygon) DESC LIMIT 1) two ON Contains(two.polygon, one.point) = 1",
		"SELECT SQL_NO_CACHE COUNT(*) FROM POLYGONS one JOIN (SELECT polygon FROM POLYGONS ORDER BY Area(polygon) DESC LIMIT 1) two ON Overlaps(two.polygon, one.polygon) = 1",
		"SELECT SQL_NO_CACHE ID, LINE FROM B_LINES ORDER BY GLength(line) LIMIT 1",
		"SELECT SQL_NO_CACHE ID, POLYGON FROM POLYGONS ORDER BY Area(polygon) LIMIT 1"
		]

def postgisqueries():
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN (SELECT LINE FROM LINES ORDER BY ST_Length(line) DESC LIMIT 1) two ON ST_Intersects(one.polygon, two.line) = True",
		"SELECT COUNT(*) FROM LINES one JOIN (SELECT POLYGON FROM POLYGONS ORDER BY ST_Area(polygon) DESC LIMIT 1) two ON ST_Intersects(one.line, two.polygon) = True",
		"SELECT COUNT(*) FROM POINTS one JOIN (SELECT polygon FROM POLYGONS ORDER BY ST_Area(polygon) DESC LIMIT 1) two ON ST_Contains(two.polygon, one.point) = True",
		"SELECT COUNT(*) FROM POLYGONS one JOIN (SELECT polygon FROM POLYGONS ORDER BY ST_Area(polygon) DESC LIMIT 1) two ON ST_Overlaps(two.polygon, one.polygon) = True",
		"SELECT ID, LINE FROM LINES ORDER BY ST_Length(line) LIMIT 1",
		"SELECT ID, POLYGON FROM POLYGONS ORDER BY ST_Area(polygon) LIMIT 1"
		]

def hanaqueries():
	return [
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN (SELECT LINE FROM BENCHMARK.LINES ORDER BY line.ST_Length() DESC LIMIT 1) two ON one.polygon.ST_Intersects(two.line) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.LINES one JOIN (SELECT POLYGON FROM BENCHMARK.POLYGONS ORDER BY polygon.ST_Area() DESC LIMIT 1) two ON one.line.ST_Intersects(two.polygon) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS one JOIN (SELECT polygon FROM BENCHMARK.POLYGONS ORDER BY polygon.ST_Area() DESC LIMIT 1) two ON two.polygon.ST_Contains(one.point) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.POLYGONS one JOIN (SELECT polygon FROM BENCHMARK.POLYGONS ORDER BY polygon.ST_Area() DESC LIMIT 1) two ON two.polygon.ST_Overlaps(one.polygon) = 1",
		"SELECT ID, LINE FROM BENCHMARK.LINES ORDER BY line.ST_Length() LIMIT 1",
		"SELECT ID, POLYGON FROM BENCHMARK.POLYGONS ORDER BY polygon.ST_Area() LIMIT 1"
		]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM POLYGONS one JOIN (SELECT LINE FROM LINES ORDER BY Length(line) DESC LIMIT 1) two ON Intersects(one.polygon, two.line) = 1",
		"SELECT COUNT(*) FROM LINES one JOIN (SELECT POLYGON FROM POLYGONS ORDER BY Area(polygon) DESC LIMIT 1) two ON Intersects(one.line, two.polygon) = 1",
		"SELECT COUNT(*) FROM B_POINTS one JOIN (SELECT polygon FROM POLYGONS ORDER BY Area(polygon) DESC LIMIT 1) two ON Contains(two.polygon, one.point) = 1",
		"SELECT COUNT(*) FROM POLYGONS one JOIN (SELECT polygon FROM POLYGONS ORDER BY Area(polygon) DESC LIMIT 1) two ON Overlaps(two.polygon, one.polygon) = 1",
		"SELECT ID, LINE FROM LINES ORDER BY Length(line) LIMIT 1",
		"SELECT ID, POLYGON FROM POLYGONS ORDER BY Area(polygon) LIMIT 1"
		]