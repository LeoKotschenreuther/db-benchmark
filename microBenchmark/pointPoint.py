from db import hana, postgis, spatialite
import output

def run(numberOfExecutions, areaLength):
	results = list()

	results.append(runPostgis(numberOfExecutions))
	results.append(runHana(numberOfExecutions))
	results.append(runSpatialiteMain(numberOfExecutions))
	
	output.printSummary(results)
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
	postgisResults = postgisDB.runQueries(postgisqueries(), numberOfExecutions)
	postgisDB.disconnect()
	print('Finished postgis Benchmark')
	output.printSingleResult(postgisResults)
	return postgisResults

def runSpatialiteMain(numberOfExecutions):
	print('Started spatialite Benchmark')
	db = spatialite.Spatialite(':memory:')
	db.loadDiskData('benchmark.db')
	spatialiteResults = db.runQueries(spatialitequeries(), numberOfExecutions)
	db.disconnect()
	print('Finished spatialite Benchmark')
	output.printSingleResult(spatialiteResults)
	return spatialiteResults

def postgisqueries():
	return [
		"SELECT ONE.ID, TWO.ID, one.POINT FROM POINTS one JOIN POINTS two ON ST_Equals(one.point, two.point) = True WHERE ONE.ID < TWO.ID",
		"SELECT ONE.ID, TWO.ID, one.X, one.Y FROM POINTS one JOIN POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID AND",
		"SELECT one.ID, two.ID, one.POINT, two.POINT FROM POINTS one JOIN POINTS two ON ST_DWithin(one.POINT, two.POINT, 20) = True WHERE one.ID < TWO.ID",
		"SELECT one.ID, two.ID, one.X, one.Y, two.X, two.Y FROM POINTS one JOIN POINTS two ON (two.x - one.x) * (two.x - one.x) + (two.y - one.y) * (two.y - one.y) <= 20 * 20 WHERE one.ID < TWO.ID"
		]

def hanaqueries():
	return [
		"SELECT ONE.ID, TWO.ID, one.POINT FROM (SELECT ID, POINT FROM BENCHMARK.B_POINTS) one JOIN BENCHMARK.B_POINTS two ON one.point.ST_Equals(two.point) = 1 WHERE ONE.ID < TWO.ID",
		"SELECT ONE.ID, TWO.ID, one.X, one.Y FROM (SELECT ID, X, Y FROM BENCHMARK.B_POINTS) one JOIN BENCHMARK.B_POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID",
		"SELECT one.ID, two.ID, one.POINT, two.POINT FROM BENCHMARK.B_POINTS one JOIN BENCHMARK.B_POINTS two ON one.point.ST_WithinDistance(two.POINT, 20) = 1 WHERE one.ID < TWO.ID",
		"SELECT one.ID, two.ID, one.X, one.Y, two.X, two.Y FROM BENCHMARK.B_POINTS one JOIN BENCHMARK.B_POINTS two ON (two.x - one.x) * (two.x - one.x) + (two.y - one.y) * (two.y - one.y) <= 20 * 20 WHERE one.ID < TWO.ID"
		]

def spatialitequeries():
	return [
		"SELECT ONE.ID, TWO.ID, one.POINT FROM B_POINTS one JOIN B_POINTS two ON Equals(one.point, two.point) = 1 WHERE ONE.ID < TWO.ID",
		"SELECT ONE.ID, TWO.ID, one.X, one.Y FROM B_POINTS one JOIN B_POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID",
		"SELECT one.ID, two.ID, one.POINT, two.POINT FROM B_POINTS one JOIN B_POINTS two ON Distance(ONE.POINT, two.POINT) <= 20 WHERE one.ID < TWO.ID",
		"SELECT one.ID, two.ID, one.X, one.Y, two.X, two.Y FROM B_POINTS one JOIN B_POINTS two ON (two.x - one.x) * (two.x - one.x) + (two.y - one.y) * (two.y - one.y) <= 20 * 20 WHERE one.ID < TWO.ID"
		]