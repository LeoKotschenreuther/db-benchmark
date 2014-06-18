from db import hana, mysql, postgis, spatialite
import output

def mysqlqueries():
	# As mysql doesn't support Buttor or Distance functions, we can only measure seven instead of eleven queries
	return [
		"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE x > -0.98",
		"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE X(point) > -0.98",
		"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE x > -0.8 AND x < -0.6 AND y > 0.4 AND y< 0.7",
		"SELECT SQL_NO_CACHE COUNT(*) FROM (SELECT * FROM test where x > -0.8 AND x < -0.6 AND y > 0.4 AND y < 0.7) as t WHERE Contains(GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))'), t.point) = 1",
		"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE intersects(point, GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))'))=1",
		"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE contains(GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))'), point) = 1",
		"SELECT SQL_NO_CACHE COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) < 0.1*0.1"
	]

def postgisqueries():
	return [
		"SELECT COUNT(*) FROM test WHERE x > -0.98",
		"SELECT COUNT(*) FROM test WHERE ST_X(point) > -0.98",
		"SELECT COUNT(*) FROM test WHERE x > -0.8 AND x < -0.6 AND y > 0.4 AND y< 0.7",
		"SELECT COUNT(*) FROM (SELECT * FROM test where x > -0.8 AND x < -0.6 AND y > 0.4 AND y < 0.7) as t WHERE ST_Contains(ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4, -0.8 0.7))', 4326), t.point) = True",
		"SELECT COUNT(*) FROM test WHERE ST_Intersects(point, ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4, -0.8 0.7))', 4326))=True",
		"SELECT COUNT(*) FROM test WHERE ST_Contains(ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))', 4326), point) = True",
		"SELECT COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) < 0.1*0.1",
		"SELECT COUNT(*) FROM (SELECT * FROM test WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE ST_Contains(ST_Buffer(ST_GeomFromText('Point(0.1 0.1)', 4326), 0.2), t.point) = True",
		"SELECT Count(*) FROM (SELECT * FROM test WHERE ST_Intersects(ST_GeomFromText('Polygon((0 0.2,0.2 0.2,0.2 0,0 0, 0 0.2))', 4326),point) = True ) subquery WHERE ST_DWithin(point, ST_GeomFromText('Point(0.1 0.1)',4326), 0.1) = True",
		"SELECT Count(*) FROM (SELECT * FROM test WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE ST_DWithin(t.point, ST_GeomFromText('Point(0.1 0.1)',4326), 0.1) = True",
		# "SELECT Count(*) FROM test WHERE ST_Contains(ST_Buffer(ST_GeomFromText('Point(0.1 0.1)', 4326), 0.1), point) = True"
	]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM test WHERE x > -0.98",
		"SELECT COUNT(*) FROM test WHERE X(point) > -0.98",
		"SELECT COUNT(*) FROM test WHERE x > -0.8 AND x < -0.6 AND y > 0.4 AND y< 0.7",
		"SELECT COUNT(*) FROM (SELECT * FROM test where x > -0.8 AND x < -0.6 AND y > 0.4 AND y < 0.7) as t WHERE MBRContains(BuildMBR(-0.8, 0.7, -0.6, 0.4), t.point) = 1",
		"SELECT COUNT(*) FROM test WHERE MBRIntersects(point, BuildMBR(-0.8, 0.7, -0.6, 0.4)) = 1",
		"SELECT COUNT(*) FROM test WHERE MBRContains(BuildMBR(-0.8, 0.7, -0.6, 0.4), point) = 1",
		"SELECT COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) < 0.1*0.1",
		"SELECT COUNT(*) FROM (SELECT * FROM test WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE MBRContains(Buffer(MakePoint(0.1, 0.1, 4326), 0.1), t.point) = 1",
		"SELECT Count(*) FROM (SELECT * FROM test WHERE MBRIntersects(BuildMBR(0, 0.2, 0.2, 0),point) = 1 ) subquery WHERE Distance(point, MakePoint(0.1, 0.1, 4326)) < 0.1",
		"SELECT Count(*) FROM (SELECT * FROM test WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE Distance(t.point, MakePoint(0.1, 0.1, 4326)) < 0.1",
		"SELECT Count(*) FROM test WHERE MBRContains(Buffer(MakePoint(0.1, 0.1, 4326), 0.1), point) = 1"
	]

def hanaqueries():
	# return [
	# 	"SELECT COUNT(*) FROM BENCHMARK.POINTS WHERE  X > -0.98",
	# 	"SELECT COUNT(*) FROM BENCHMARK.POINTS WHERE POINT.ST_X() > -0.98",
	# 	"SELECT Count(*) FROM BENCHMARK.POINTS WHERE X > -0.8 AND X < -0.6 AND Y > 0.4 AND Y < 0.7",
	# 	"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.POINTS WHERE X > -0.8 AND X < -0.6 AND Y > 0.4 AND Y < 0.7) as t WHERE NEW ST_POLYGON('Polygon((-0.8 0.7, -0.6 0.7, -0.6 0.4, -0.8 0.4, -0.8 0.7))').ST_CONTAINS(t.POINT) = 1",
	# 	"SELECT COUNT(*) FROM BENCHMARK.POINTS WHERE POINT.ST_IntersectsRect(new ST_Point(-0.8, 0.7), new ST_Point(-0.6,0.4)) = 1",
	# 	"SELECT COUNT(*) FROM BENCHMARK.POINTS WHERE NEW ST_POLYGON('Polygon((-0.8 0.7, -0.6 0.7, -0.6 0.4, -0.8 0.4, -0.8 0.7))').ST_CONTAINS(POINT) = 1",
	# 	"SELECT COUNT(*) FROM BENCHMARK.POINTS WHERE (0.1-X)*(0.1-X) + (0.1-Y)*(0.1-Y) < 0.1*0.1",
	# 	"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.POINTS WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE NEW ST_POINT(0.1, 0.1).ST_BUFFER(0.1).ST_CONTAINS(t.POINT) = 1",
	# 	"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.POINTS WHERE POINT.ST_IntersectsRect(new ST_Point(0, 0.2), new ST_Point(0.2,0)) = 1 ) WHERE POINT.ST_WithinDistance(new ST_Point(0.1, 0.1), 0.1) = 1",
	# 	"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.POINTS WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE t.POINT.ST_WithinDistance(new ST_Point(0.1, 0.1), 0.1) = 1",
	# 	"SELECT COUNT(*) FROM BENCHMARK.POINTS WHERE NEW ST_POINT(0.1, 0.1).ST_BUFFER(0.1).ST_CONTAINS(POINT) = 1"
	# ]

	return [
		"SELECT COUNT(*) FROM BENCHMARK.TEST WHERE  X > -0.98",
		"SELECT COUNT(*) FROM BENCHMARK.TEST WHERE POINT.ST_X() > -0.98",
		"SELECT Count(*) FROM BENCHMARK.TEST WHERE X > -0.8 AND X < -0.6 AND Y > 0.4 AND Y < 0.7",
		"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.TEST WHERE X > -0.8 AND X < -0.6 AND Y > 0.4 AND Y < 0.7) as t WHERE NEW ST_POLYGON('Polygon((-0.8 0.7, -0.6 0.7, -0.6 0.4, -0.8 0.4, -0.8 0.7))').ST_CONTAINS(t.POINT) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.TEST WHERE POINT.ST_IntersectsRect(new ST_Point(-0.8, 0.7), new ST_Point(-0.6,0.4)) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.TEST WHERE NEW ST_POLYGON('Polygon((-0.8 0.7, -0.6 0.7, -0.6 0.4, -0.8 0.4, -0.8 0.7))').ST_CONTAINS(POINT) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.TEST WHERE (0.1-X)*(0.1-X) + (0.1-Y)*(0.1-Y) < 0.1*0.1",
		"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.TEST WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE NEW ST_POINT(0.1, 0.1).ST_BUFFER(0.1).ST_CONTAINS(t.POINT) = 1",
		"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.TEST WHERE POINT.ST_IntersectsRect(new ST_Point(0, 0.2), new ST_Point(0.2,0)) = 1 ) WHERE POINT.ST_WithinDistance(new ST_Point(0.1, 0.1), 0.1) = 1",
		"SELECT COUNT(*) FROM (SELECT * FROM BENCHMARK.TEST WHERE X > 0 AND X < 0.2 AND Y > 0 AND Y < 0.2) as t WHERE t.POINT.ST_WithinDistance(new ST_Point(0.1, 0.1), 0.1) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.TEST WHERE NEW ST_POINT(0.1, 0.1).ST_BUFFER(0.1).ST_CONTAINS(POINT) = 1"
	]

def runHana(numberOfExecutions):
	print('Starting Hana Benchmark')
	hanaDB = hana.Hana()
	hanaResults = hanaDB.runQueries(hanaqueries(), numberOfExecutions)
	hanaDB.disconnect()
	print('Finished Hana Benchmark')
	output.printSingleResult(hanaResults)
	return hanaResults

def runMySQL(numberOfExecutions):
	print('Starting MySQL Benchmark')
	mysqlDB = mysql.Mysql()
	mySQLResults = mysqlDB.runQueries(mysqlqueries(), numberOfExecutions)
	mysqlDB.disconnect()
	print('Finished MySQL Benchmark')
	output.printSingleResult(mySQLResults)
	return mySQLResults

def runPostgis(numberOfExecutions):
	print('Starting postgresql (postgis) Benchmark')
	postgisDB = postgis.Postgis()
	postgisResults = postgisDB.runQueries(postgisqueries(), numberOfExecutions)
	postgisDB.disconnect()
	print('Finished postgresql (postgis) Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain(numberOfExecutions):
	print('Starting spatialite Benchmark - main-memory')
	spatialiteDBMain = spatialite.Spatialite(':memory:')
	spatialiteDBMain.setUpDB()
	spatialiteResultsMain = spatialiteDBMain.runQueries(spatialitequeries(), numberOfExecutions)
	spatialiteDBMain.disconnect()
	print('Finished spatialite Benchmark - main-memory')
	output.printSingleResult(spatialiteResultsMain)
	return spatialiteResultsMain
