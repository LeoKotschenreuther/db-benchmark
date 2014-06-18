def postgisqueries(exteriorString):
	return [
		"SELECT ST_Intersection(one.polygon, two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(ST_Boundary(one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(ST_Difference(" + exteriorString + ", one.polygon), two.polygon) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(one.polygon, ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(ST_Boundary(one.polygon), ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(ST_Difference(" + exteriorString + ", one.polygon), ST_Boundary(two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(one.polygon, ST_Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(ST_Boundary(one.polygon), ST_Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two",
		# "SELECT ST_Intersection(one.polygon, ST_Difference(" + exteriorString + ", two.polygon)) FROM (SELECT * FROM POLYGONS WHERE ID=0) one, (SELECT * FROM POLYGONS WHERE ID=1) two"
		]