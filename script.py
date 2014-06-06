import mysql
import postgis
import spatialite
import output
from threading import Thread

numberOfExecutions = 1
mysqlqueries = [
	"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE x > -0.98",
	"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE X(point) > -0.98",
	"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y<= 0.7",
	"SELECT SQL_NO_CACHE COUNT(*) FROM (SELECT * FROM test where x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y <= 0.7) as t WHERE Contains(GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))'), t.point) = 1",
	"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE intersects(point, GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))'))=1",
	"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE contains(GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))'), point) = 1",
	"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) <= 0.1*0.1"
	# missing buffer function
	# missing within_distance function
	# missing within_distance function
	# missing buffer function
	]

postgisqueries = [
	"SELECT COUNT(*) FROM test WHERE x > -0.98",
	"SELECT COUNT(*) FROM test WHERE ST_X(point) > -0.98",
	"SELECT COUNT(*) FROM test WHERE x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y<= 0.7",
	"SELECT COUNT(*) FROM (SELECT * FROM test where x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y <= 0.7) as t WHERE ST_Contains(ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4, -0.8 0.7))', 4326), t.point) = True",
	"SELECT COUNT(*) FROM test WHERE ST_Intersects(point, ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4, -0.8 0.7))', 4326))=True",
	"SELECT COUNT(*) FROM test WHERE ST_Contains(ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))', 4326), point) = True",
	"SELECT COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) <= 0.1*0.1",
	"SELECT COUNT(*) FROM (SELECT * FROM test WHERE X >= 0 AND X <= 0.2 AND Y >= 0 AND Y <= 0.2) as t WHERE ST_Contains(ST_Buffer(ST_GeomFromText('Point(0.1 0.1)', 4326), 0.2), t.point) = True",
	"SELECT Count(*) FROM (SELECT * FROM test WHERE ST_Intersects(ST_GeomFromText('Polygon((0 0.2,0.2 0.2,0.2 0,0 0, 0 0.2))', 4326),point) = True ) subquery WHERE ST_DWithin(point, ST_GeomFromText('Point(0.1 0.1)',4326), 0.1) = True",
	"SELECT Count(*) FROM (SELECT * FROM test WHERE X >= 0 AND X <= 0.2 AND Y >= 0 AND Y <= 0.2) as t WHERE ST_DWithin(t.point, ST_GeomFromText('Point(0.1 0.1)',4326), 0.1) = True",
	"SELECT Count(*) FROM test WHERE ST_Contains(ST_Buffer(ST_GeomFromText('Point(0.1 0.1)', 4326), 0.1), point) = True"
	]

spatialitequeries = [
	"SELECT COUNT(*) FROM test WHERE x > -0.98",
	"SELECT COUNT(*) FROM test WHERE X(point) > -0.98",
	"SELECT COUNT(*) FROM test WHERE x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y<= 0.7",
	"SELECT COUNT(*) FROM (SELECT * FROM test where x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y <= 0.7) as t WHERE MBRContains(BuildMBR(-0.8, 0.7, -0.6, 0.4), t.point) = 1",
	"SELECT COUNT(*) FROM test WHERE MBRIntersects(point, BuildMBR(-0.8, 0.7, -0.6, 0.4)) = 1",
	"SELECT COUNT(*) FROM test WHERE MBRContains(BuildMBR(-0.8, 0.7, -0.6, 0.4), point) = 1",
	"SELECT COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) <= 0.1*0.1",
	"SELECT COUNT(*) FROM (SELECT * FROM test WHERE X >= 0 AND X <= 0.2 AND Y >= 0 AND Y <= 0.2) as t WHERE MBRContains(Buffer(MakePoint(0.1, 0.1, 4326), 0.1), t.point) = 1",
	"SELECT Count(*) FROM (SELECT * FROM test WHERE MBRIntersects(BuildMBR(0, 0.2, 0.2, 0),point) = 1 ) subquery WHERE Distance(point, MakePoint(0.1, 0.1, 4326)) <= 0.1",
	"SELECT Count(*) FROM (SELECT * FROM test WHERE X >= 0 AND X <= 0.2 AND Y >= 0 AND Y <= 0.2) as t WHERE Distance(t.point, MakePoint(0.1, 0.1, 4326)) <= 0.1",
	"SELECT Count(*) FROM test WHERE MBRContains(Buffer(MakePoint(0.1, 0.1, 4326), 0.1), point) = 1"
	]

allResults = []

def runMySQL():
	print('Starting MySQL Benchmark')
	mysqlDB = mysql.Mysql()
	mySQLResults = mysqlDB.runQueries(mysqlqueries, numberOfExecutions)
	mysqlDB.disconnect()
	print('Finished MySQL Benchmark')
	output.printSingleResult(mySQLResults)
	allResults.append(mySQLResults)

def runPostgis():
	print('Starting postgresql (postgis) Benchmark')
	postgisDB = postgis.Postgis()
	postgisResults = postgisDB.runQueries(postgisqueries, numberOfExecutions)
	postgisDB.disconnect()
	print('Finished postgresql (postgis) Benchmark')
	output.printSingleResult(postgisResults)
	allResults.append(postgisResults)

def runSpatialiteMain():
	print('Starting spatialite Benchmark - main-memory')
	spatialiteDBMain = spatialite.Spatialite(':memory:')
	spatialiteDBMain.setUpDB()
	spatialiteResultsMain = spatialiteDBMain.runQueries(spatialitequeries, numberOfExecutions)
	spatialiteDBMain.disconnect()
	print('Finished spatialite Benchmark - main-memory')
	output.printSingleResult(spatialiteResultsMain)
	allResults.append(spatialiteResultsMain)

def runSpatialiteDisk():
	print('Starting spatialite Benchmark - disk')
	spatialiteDB = spatialite.Spatialite('benchmark.db')
	# spatialiteDB.setUpDB()
	spatialiteResultsDisk = spatialiteDB.runQueries(spatialitequeries, numberOfExecutions)
	spatialiteDB.disconnect()
	print('Finished spatialite Benchmark - disk')
	output.printSingleResult(spatialiteResultsDisk)
	allResults.append(spatialiteResultsDisk)

def printResultsToFile():
	print('Start printing results')
	output.printSummary(allResults)
	print('Finished printing results')


# t1 = Thread(target=runMySQL, args = ())
# t2 = Thread(target=runPostgis, args = ())
# t3 = Thread(target=runSpatialiteMain, args = ())
# t4 = Thread(target=runSpatialiteDisk, args = ())

# t1.start()
# t2.start()
# t3.start()
# t4.start()

# t1.join()
# t2.join()
# t3.join()
# t4.join()

runMySQL()
runPostgis()
runSpatialiteMain()
runSpatialiteDisk()

printResultsToFile()


