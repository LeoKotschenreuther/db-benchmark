import mysql
import postgis
import spatialite
import output
# import thread

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
	"SELECT COUNT(*) FROM (SELECT * FROM test where x >= -0.8 AND x <= -0.6 AND y >= 0.4 AND y <= 0.7) as t WHERE Contains(BuildMBR(-0.8, 0.7, -0.6, 0.4), t.point) = 1",
	"SELECT COUNT(*) FROM test WHERE Intersects(point, BuildMBR(-0.8, 0.7, -0.6, 0.4)) = 1",
	"SELECT COUNT(*) FROM test WHERE Contains(BuildMBR(-0.8, 0.7, -0.6, 0.4), point) = 1",
	"SELECT COUNT(*) FROM test WHERE (0.1-x)*(0.1-x)+(0.1-y)*(0.1-y) <= 0.1*0.1",
	"SELECT COUNT(*) FROM (SELECT * FROM test WHERE X >= 0 AND X <= 0.2 AND Y >= 0 AND Y <= 0.2) as t WHERE Contains(Buffer(MakePoint(0.1, 0.1, 4326), 0.1), t.point) = 1",
	"SELECT Count(*) FROM (SELECT * FROM test WHERE Intersects(BuildMBR(0, 0.2, 0.2, 0),point) = 1 ) subquery WHERE Distance(point, MakePoint(0.1, 0.1, 4326)) <= 0.1",
	"SELECT Count(*) FROM (SELECT * FROM test WHERE X >= 0 AND X <= 0.2 AND Y >= 0 AND Y <= 0.2) as t WHERE Distance(t.point, MakePoint(0.1, 0.1, 4326)) <= 0.1",
	"SELECT Count(*) FROM test WHERE Contains(Buffer(MakePoint(0.1, 0.1, 4326), 0.1), point) = 1"
	]

print('Starting MySQL Benchmark')
mysqlDB = mysql.Mysql()
mySQLResults = mysqlDB.runQueries(mysqlqueries, numberOfExecutions)
mysqlDB.disconnect()
print('Finished MySQL Benchmark')

print('Starting postgresql (postgis) Benchmark')
postgisDB = postgis.Postgis()
postgisResults = postgisDB.runQueries(postgisqueries, numberOfExecutions)
postgisDB.disconnect()
print('Finished postgresql (postgis) Benchmark')

print('Starting spatialite Benchmark')
spatialiteDB = spatialite.Spatialite()
spatialiteDB.setUpDB()
spatialiteResults = spatialiteDB.runQueries(spatialitequeries, numberOfExecutions)
spatialiteDB.disconnect()
print('Finished spatialite Benchmark')


print('Start printing results')
output.printSingleResult(mySQLResults)
output.printSingleResult(postgisResults)
output.printSingleResult(spatialiteResults)
output.printSummary([mySQLResults, postgisResults, spatialiteResults])
print('Finished printing results')


