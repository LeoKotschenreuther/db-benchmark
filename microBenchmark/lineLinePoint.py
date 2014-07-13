from db import hana, postgis, spatialite
import output

def run(numberOfExecutions):
	results = list()

	results.append(runPostgis(numberOfExecutions))
	results.append(runHana(numberOfExecutions))
	results.append(runSpatialiteMain(numberOfExecutions))
	
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
		"SELECT COUNT(*) FROM POINTS p JOIN LINES l ON ST_Intersects(p.point, l.line) = True",
		"SELECT COUNT(*) FROM LINES one JOIN LINES two ON ST_Crosses(one.line, two.line) = True WHERE ONE.ID < TWO.ID"
		]

def hanaqueries():
	return [
		"SELECT COUNT(*) FROM BENCHMARK.B_POINTS p JOIN BENCHMARK.LINES l ON p.point.ST_Intersects(l.line) = 1",
		"SELECT COUNT(*) FROM BENCHMARK.LINES one JOIN BENCHMARK.LINES two ON one.line.ST_Crosses(two.line) = 1 WHERE ONE.ID < TWO.ID"
		]

def spatialitequeries():
	return [
		"SELECT COUNT(*) FROM B_POINTS p JOIN LINES l ON Intersects(p.point, l.line) = 1",
		"SELECT COUNT(*) FROM LINES one JOIN LINES two ON Crosses(one.line, two.line) = 1 WHERE ONE.ID < TWO.ID"
		]