from db import hana, postgis, spatialite
import output

def run(numberOfExecutions, areaLength):
	results = list()

	areaPoints = list()
	areaPoints.append({'x': -areaLength, 'y': areaLength})
	areaPoints.append({'x': areaLength, 'y': areaLength})
	areaPoints.append({'x': areaLength, 'y': -areaLength})
	areaPoints.append({'x': -areaLength, 'y': -areaLength})

	# Skipped Hana as it has performance problems!

	results.append(runPostgis(numberOfExecutions, areaPoints))
	results.append(runHana(numberOfExecutions, areaPoints))
	results.append(runSpatialiteMain(numberOfExecutions, areaPoints))
	
	# output.print9ISummary(results)
	return results

def runHana(numberOfExecutions, areaPoints):
	print('Starting Hana Benchmark')
	hanaDB = hana.Hana()
	areaString = hanaDB.polygonString(areaPoints)
	hanaResults = hanaDB.runQueries(hanaqueries(areaString), numberOfExecutions)
	hanaDB.disconnect()
	print('Finished Hana Benchmark')
	output.printSingleResult(hanaResults)
	return hanaResults

def runPostgis(numberOfExecutions, areaPoints):
	print('Started postgis Benchmark')
	postgisDB = postgis.Postgis()
	areaString = postgisDB.polygonString(areaPoints)
	postgisResults = postgisDB.runQueries(postgisqueries(areaString), numberOfExecutions)
	postgisDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain(numberOfExecutions, areaPoints):
	print('Started spatialite Benchmark')
	db = spatialite.Spatialite(':memory:')
	db.loadDiskData('benchmark.db')
	areaString = db.polygonString(areaPoints)
	spatialiteResults = db.runQueries(spatialitequeries(areaString), numberOfExecutions)
	db.disconnect()
	print('Finished spatialite Benchmark')
	output.printSingleResult(spatialiteResults)
	return spatialiteResults

def postgisqueries(exteriorString):
	return [
		"SELECT ST_Intersection(one.polygon, two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(ST_Boundary(one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(ST_Difference(ST_GeomFromText('" + exteriorString + "', 4326), one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(one.polygon, ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(ST_Boundary(one.polygon), ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(ST_Difference(ST_GeomFromText('" + exteriorString + "', 4326), one.polygon), ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(one.polygon, ST_Difference(ST_GeomFromText('" + exteriorString + "', 4326), two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(ST_Boundary(one.polygon), ST_Difference(ST_GeomFromText('" + exteriorString + "', 4326), two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT ST_Intersection(ST_Difference(ST_GeomFromText('" + exteriorString + "', 4326), one.polygon), ST_Difference(ST_GeomFromText('" + exteriorString + "', 4326), two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two"
		]

def hanaqueries(exteriorString):
	return [
		"SELECT one.polygon.ST_Intersection(two.polygon) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT one.polygon.ST_Intersection(two.polygon) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT NEW ST_Polygon('" + exteriorString + "', 4326).ST_Difference(one.polygon).ST_Intersection(two.polygon) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT one.polygon.ST_Intersection(two.polygon.ST_Boundary()) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT one.polygon.ST_BOUNDARY().ST_Intersection(two.polygon.ST_Boundary()) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT NEW ST_Polygon('" + exteriorString + "').ST_Difference(one.polygon).ST_Intersection(two.polygon.ST_Boundary()) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT one.polygon.ST_Intersection(NEW ST_Polygon('" + exteriorString + "').ST_Difference(two.polygon)) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT one.polygon.ST_Boundary().ST_Intersection(NEW ST_Polygon('" + exteriorString + "').ST_Difference(two.polygon)) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two",
		"SELECT NEW ST_Polygon('" + exteriorString + "').ST_Difference(one.polygon).ST_Intersection(NEW ST_Polygon('" + exteriorString + "').ST_Difference(two.polygon)) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0) one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1) two"
		]

def spatialitequeries(exteriorString):
	return [
		"SELECT Intersection(one.polygon, two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Boundary(one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Difference(GeomFromText('" + exteriorString + "'), one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(one.polygon, Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Boundary(one.polygon), Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Difference(GeomFromText('" + exteriorString + "'), one.polygon), Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(one.polygon, Difference(GeomFromText('" + exteriorString + "'), two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Boundary(one.polygon), Difference(GeomFromText('" + exteriorString + "'), two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Difference(GeomFromText('" + exteriorString + "'), one.polygon), Difference(GeomFromText('" + exteriorString + "'), two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two"
		]