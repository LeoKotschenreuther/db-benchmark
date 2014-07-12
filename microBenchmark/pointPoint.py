from db import hana, postgis, spatialite
import output

def run(numberOfExecutions):
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
		"SELECT COUNT(*) FROM POINTS one JOIN POINTS two ON ST_Equals(one.point, two.point) = True WHERE ONE.ID < TWO.ID",
		"SELECT COUNT(*) FROM POINTS one JOIN POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID",
		]

def hanaqueries():
	return [
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS one JOIN BENCHMARK.B_POINTS two ON one.point.ST_Equals(two.point) = 1 WHERE ONE.ID < TWO.ID",
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS one JOIN BENCHMARK.B_POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID",
		]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM B_POINTS one JOIN B_POINTS two ON Equals(one.point, two.point) = 1 WHERE ONE.ID < TWO.ID",
		"SELECT COUNT(*) FROM B_POINTS one JOIN B_POINTS two ON one.X = two.X and one.Y = two.y WHERE ONE.ID < TWO.ID",
		]