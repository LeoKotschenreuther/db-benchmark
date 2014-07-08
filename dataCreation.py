from db import mysql, postgis, spatialite, hana
import random
import math

numPoints = 1000000
numLines = 1000
numPolygons = 10000

def createPolygon(numPoints, areaLength):
	if numPoints < 3:
		return False

	points = list()
	midX = random.random() * 2 * areaLength * 3 / 4 - areaLength * 3.0 / 4.0
	midY = random.random() * 2 * areaLength * 3 / 4 - areaLength * 3.0 / 4.0
	step = 2 * math.pi / numPoints
	for i in range(0, numPoints):
		distanceMid = random.random() * areaLength / 8.0
		if distanceMid < areaLength / 8.0 * 0.05: distanceMid = areaLength / 8.0 * 0.05
		x = midX + math.cos(step*i) * distanceMid
		y = midY + math.sin(step*i) * distanceMid
		if x > areaLength : x = areaLength
		if x < -areaLength : x = -areaLength
		if y > areaLength : y = areaLength
		if y < -areaLength : y = -areaLength
		points.append({'x': x, 'y': y})
	# print points
	return points

def createPolygons(resetTables, sizes, areaLength):
	polygons = list()
	for i, polygonSize in enumerate(sizes):
		for x in range(0, int(math.ceil(numPolygons / len(sizes)))):
			polygonIsValid = False
			polygon = list()
			while not polygonIsValid:
				polygon = createPolygon(polygonSize, areaLength)
				# check whether they intersect:
				hanaDB = hana.Hana()
				polygonIsValid = hanaDB.isPolygonValid(polygon)
				hanaDB.disconnect()
				# postgisDB = postgis.Postgis()
				# polygonIsValid = postgisDB.isPolygonValid(polygon1)
				# postgisDB.disconnect()
				# spatialiteDB = spatialite.Spatialite(':memory:')
				# polygonIsValid = spatialiteDB.isPolygonValid(polygon1)
				# spatialiteDB.disconnect()
			polygons.append(polygon)
			if (x + i * int(math.ceil(numPolygons / len(sizes)))) % 1000 == 999:
				print "finished: " + str(x + i * int(math.ceil(numPolygons / len(sizes))) + 1)

	print "Created valid Polygons"

	postgisDB = postgis.Postgis()
	postgisDB.dropCreateTable('POLYGONS')
	postgisDB.insertPolygons(polygons)
	postgisDB.disconnect()

	hanaDB = hana.Hana()
	hanaDB.dropCreateTable('BENCHMARK.POLYGONS')
	hanaDB.insertPolygons(polygons)
	hanaDB.disconnect()

	spatialiteDB = spatialite.Spatialite('benchmark.db')
	if resetTables: spatialiteDB.dropCreateTable('POLYGONS')
	spatialiteDB.insertPolygons(polygons)
	spatialiteDB.disconnect()

def createPoints(areaLength):
	points = list()
	for i in range(0, numPoints):
		x = random.random() * 2 * areaLength - areaLength
		y = random.random() * 2 * areaLength - areaLength
		points.append({'x' : x, 'y': y})
		if i % 1000 == 999:
			print "finished: " + str(i+1)

	postgisDB = postgis.Postgis()
	postgisDB.dropCreateTable('POINTS')
	postgisDB.insertPoints(points)
	postgisDB.disconnect()

	hanaDB = hana.Hana()
	hanaDB.dropCreateTable('BENCHMARK.B_POINTS')
	hanaDB.insertPoints(points)
	hanaDB.disconnect()

	spatialiteDB = spatialite.Spatialite('benchmark.db')
	spatialiteDB.dropCreateTable('B_POINTS')
	spatialiteDB.insertPoints(points)
	spatialiteDB.disconnect()