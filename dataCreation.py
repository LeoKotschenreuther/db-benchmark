from db import mysql, postgis, spatialite, hana
import random
import math

resetTables = True
offset = 175000
polygonSizes = [500]
lineSizes = [10]
numPoints = 1000000
numLines = 100000
numPolygons = 7500

def createData(areaLength):
	# removeData(500)

	# createPoints(areaLength)
	createPolygons(resetTables, polygonSizes, areaLength)
	# createLines(resetTables, lineSizes, areaLength)

def createPolygon(size, areaLength):
	if size < 3:
		return False

	points = list()
	midX = random.random() * 2 * areaLength * 3 / 4 - areaLength * 3.0 / 4.0
	midY = random.random() * 2 * areaLength * 3 / 4 - areaLength * 3.0 / 4.0
	step = 2 * math.pi / size
	for i in range(0, size):
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

def createLine(size, areaLength):
	if size < 2:
		return False

	points = list()
	startX = random.random() * 2 * areaLength * 3 / 4 - areaLength
	startY = random.random() * 2 * areaLength * 3 / 4 - areaLength
	points.append({'x': startX, 'y': startY})
	endX = random.random() * 2 * areaLength * 3 / 4 - areaLength
	endY = random.random() * 2 * areaLength * 3 / 4 - areaLength
	dx = endX - startX
	dy = endY - startY
	stepX = dx / (size - 1)
	stepY = dy / (size - 1)
	for i in range(0, size - 2):
		x = startX + (i + 1) * stepX + random.random() * 2 * stepX - stepX
		y = startY + (i + 1) * stepY + random.random() * 2 * stepY - stepY
		points.append({'x': x, 'y': y})
	points.append({'x': endX, 'y': endY})
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
				# polygonIsValid = postgisDB.isPolygonValid(polygon)
				# postgisDB.disconnect()
				# spatialiteDB = spatialite.Spatialite(':memory:')
				# polygonIsValid = spatialiteDB.isPolygonValid(polygon)
				# spatialiteDB.disconnect()
			polygons.append(polygon)
			if (x + i * int(math.ceil(numPolygons / len(sizes)))) % 1000 == 999:
				print "finished: " + str(x + i * int(math.ceil(1000 / len(sizes))) + 1)

	print "Created valid Polygons"

	postgisDB = postgis.Postgis()
	if resetTables: postgisDB.dropCreateTable('POLYGONS')
	postgisDB.insertPolygons(polygons, offset)
	postgisDB.disconnect()

	hanaDB = hana.Hana()
	if resetTables: hanaDB.dropCreateTable('BENCHMARK.POLYGONS')
	hanaDB.insertPolygons(polygons, offset)
	hanaDB.disconnect()

	spatialiteDB = spatialite.Spatialite('benchmark.db')
	if resetTables: spatialiteDB.dropCreateTable('POLYGONS')
	spatialiteDB.insertPolygons(polygons, offset)
	spatialiteDB.disconnect()

	# print "Finished: " + str((a + 1) * num * 100 / numPolygons) + "%"

def createLines(resetTables, sizes, areaLength):
	lines = list()
	for i, lineSize in enumerate(sizes):
		for x in range(0, int(math.ceil(numLines / len(sizes)))):
			line = createLine(lineSize, areaLength)
			lines.append(line)
			if (x + i * int(math.ceil(numLines / len(sizes)))) % 1000 == 999:
				print "finished: " + str(x + i * int(math.ceil(1000 / len(sizes))) + 1)

	print "Created valid Lines"

	postgisDB = postgis.Postgis()
	if resetTables: postgisDB.dropCreateTable('LINES')
	postgisDB.insertLines(lines, offset)
	postgisDB.disconnect()

	hanaDB = hana.Hana()
	if resetTables: hanaDB.dropCreateTable('BENCHMARK.LINES')
	hanaDB.insertLines(lines, offset)
	hanaDB.disconnect()

	spatialiteDB = spatialite.Spatialite('benchmark.db')
	if resetTables: spatialiteDB.dropCreateTable('LINES')
	spatialiteDB.insertLines(lines, offset)
	spatialiteDB.disconnect()

	# print "Finished: " + str((a + 1) * num * 100 / numPolygons) + "%"

def createPoints(areaLength):
	points = list()
	for i in range(0, numPoints):
		x = random.random() * 2 * areaLength - areaLength
		y = random.random() * 2 * areaLength - areaLength
		points.append({'x' : x, 'y': y})
		if i % 10000 == 9999:
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

def removeData(size):
	postgisDB = postgis.Postgis()
	postgisDB.removeLines(size)
	postgisDB.disconnect()

	# hanaDB = hana.Hana()
	# hanaDB.removePolygons(size)
	# hanaDB.disconnect()