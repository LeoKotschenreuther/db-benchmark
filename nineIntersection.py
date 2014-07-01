def postgisqueries(exteriorString, polygonSize):
	return [
		"SELECT ST_Intersection(one.polygon, two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(ST_Boundary(one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(ST_Difference(" + exteriorString + ", one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(one.polygon, ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(ST_Boundary(one.polygon), ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(ST_Difference(" + exteriorString + ", one.polygon), ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(one.polygon, ST_Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(ST_Boundary(one.polygon), ST_Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT ST_Intersection(ST_Difference(" + exteriorString + ", one.polygon), ST_Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two"
		]

def hanaqueries(exteriorString, polygonSize):
	return [
		"SELECT one.polygon.ST_Intersection(two.polygon) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT one.polygon.ST_Intersection(two.polygon) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT " + exteriorString + ".ST_Difference(one.polygon).ST_Intersection(two.polygon) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT one.polygon.ST_Intersection(two.polygon.ST_Boundary()) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT one.polygon.ST_BOUNDARY().ST_Intersection(two.polygon.ST_Boundary()) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT " + exteriorString + ".ST_Difference(one.polygon).ST_Intersection(two.polygon.ST_Boundary()) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT one.polygon.ST_Intersection(" + exteriorString + ".ST_Difference(two.polygon)) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT one.polygon.ST_Boundary().ST_Intersection(" + exteriorString + ".ST_Difference(two.polygon)) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two",
		"SELECT " + exteriorString + ".ST_Difference(one.polygon).ST_Intersection(" + exteriorString + ".ST_Difference(two.polygon)) FROM (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=0 AND SIZE = " + str(polygonSize) + ") one, (SELECT * FROM BENCHMARK.POLYGONS WHERE ID=1 AND SIZE = " + str(polygonSize) + ") two"
		]

def spatialitequeries(exteriorString):
	return [
		"SELECT Intersection(one.polygon, two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Boundary(one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Difference(" + exteriorString + ", one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(one.polygon, Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Boundary(one.polygon), Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Difference(" + exteriorString + ", one.polygon), Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(one.polygon, Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Boundary(one.polygon), Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		"SELECT Intersection(Difference(" + exteriorString + ", one.polygon), Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two"
		]