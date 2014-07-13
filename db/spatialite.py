import csv
import time
from pyspatialite import dbapi2 as db
import math
from StringIO import StringIO

class Spatialite:

	def __init__(self, database):
		self.db = database
		self.connection = self.connect(database)
		self.cursor = self.connection.cursor()

	def connect(self, database):
		return db.connect(database)

	def disconnect(self):
		self.connection.close()

	def loadDiskData(self, diskFile):
		init = 'SELECT InitSpatialMetadata()'
		self.cursor.execute(init)
		attachDB = "ATTACH DATABASE '" + diskFile + "' AS 'benchmark'"
		self.cursor.execute(attachDB)

	def setUpSoccerData(self, enableIndex):
		init = 'SELECT InitSpatialMetadata()'
		self.cursor.execute(init)

		createTable = "CREATE TABLE test (x INTEGER, y INTEGER)"
		self.cursor.execute(createTable)
		addPointColumn = "SELECT AddGeometryColumn ('test', 'point', 4326, 'POINT', 2)"
		self.cursor.execute(addPointColumn)

		if enableIndex:
			createIndex = "SELECT CreateSpatialIndex('test', 'point')"
			self.cursor.execute(createIndex)
			print "created Index"

		print "Table is created!"

		with open('data.csv','rb') as csvfile:
			reader = csv.reader(csvfile)
			to_db = [(i[0], i[1]) for i in reader]

		self.cursor.executemany("INSERT INTO test (x, y) VALUES (?, ?);", to_db)
		self.connection.commit()
		print "Inserted points into the table"
		self.cursor.execute("UPDATE test set point = MakePoint(x, y, 4326)")
		self.connection.commit()
		print "Did set up the db"

	def dropCreateTable(self, table):
		# try:
		# 	init = 'SELECT InitSpatialMetadata()'
		# 	self.cursor.execute(init)
		# except:
		# 	print "\tDid already run InitSpatialMetadata"
		dropTable = "DROP TABLE IF EXISTS " + table
		self.cursor.execute(dropTable)
		print("\tDropped Table")
		createTable = ""
		addColumn = ""
		if table == 'POLYGONS':
			createTable = "CREATE TABLE POLYGONS (ID integer, size integer)"
			addColumn = "SELECT AddGeometryColumn ('POLYGONS', 'polygon', 4326, 'POLYGON', 2)"
		elif table == 'LINES':
			createTable = "CREATE TABLE LINES (ID INTEGER, size INTEGER)"
			addColumn = "SELECT AddGeometryColumn ('LINES', 'line', 4326, 'LINESTRING', 2)"
		elif table == 'B_POINTS':
			createTable = "CREATE TABLE B_POINTS (ID INTEGER, X FLOAT, Y FLOAT)"
			addColumn = "SELECT AddGeometryColumn ('B_POINTS', 'point', 4326, 'POINT', 2)"
		self.cursor.execute(createTable)
		self.cursor.execute(addColumn)

		self.connection.commit()
		print("\tCreated Table")

	def polygonString(self, polygon):
		string = "Polygon(("
		for point in polygon:
			string += str(point['x']) + " " + str(point['y']) + ","
		string += str(polygon[0]['x']) + " " + str(polygon[0]['y']) + "))"
		return string

	def lineString(self, line):
		string = "LineString("
		for point in line:
			string += str(point['x']) + " " + str(point['y']) + ","
		string = string[:-1] + ")"
		return string


	def isPolygonValid(self, polygon):
		try:
			init = 'SELECT InitSpatialMetadata()'
			self.cursor.execute(init)
		except:
			print "Did already run InitSpatialMetadata"
		query = "SELECT IsValid(" + self.polygonString(polygon) + ")"
		self.cursor.execute(query)
		rows = self.cursor.fetchall()
		isValid = False
		for row in rows:
			# print(row[0])
			if row[0] == 1: isValid = True
		return isValid

	def insertPolygons(self, polygons, offset):
		for i, polygon in enumerate(polygons):
			size = len(polygon)
			insert = '''INSERT INTO POLYGONS (ID, size, polygon) VALUES (?, ?, PolygonFromText(?, 4326))'''
			self.cursor.execute(insert, (i+offset, size, self.polygonString(polygon)))
			if i % 1000 == 999:
				print "finished: " + str(i+1)
				self.connection.commit()
		self.connection.commit()
		print("\tInserted Polygons into polygons table")

	def insertLines(self, lines, offset):
		for i, line in enumerate(lines):
			size = len(line)
			insert = '''INSERT INTO LINES (ID, size, line) VALUES (?, ?, LineStringFromText(?, 4326))'''
			self.cursor.execute(insert, (i+offset, size, self.lineString(line)))
			if i % 1000 == 999:
				print "finished: " + str(i+1)
				self.connection.commit()
		self.connection.commit()
		print("\tInserted Lines into lines table")

	def insertPoints(self, points):
		for i, point in enumerate(points):
			# print self.pointString(point)
			insert = '''INSERT INTO B_POINTS (ID, X, Y, POINT) VALUES (?, ?, ?, MakePoint(?, ?, 4326))'''
			self.cursor.execute(insert, (i, point['x'], point['y'], point['x'], point['y']))
			# self.cursor.execute(insert, (i, point['x'], point['y'], 'POINT(1 2)'))
			if i % 1000 == 999:
				print "finished: " + str(i+1)
				self.connection.commit()
		self.connection.commit()
		print("\tInserted Points into Points table")
		# query = "select X(point) from B_POINTS WHERE ID = 0"
		# result = self.cursor.execute(query)
		# for row in result:
		# 	print row

	def checkIntersection(self, polygons):
		query = "SELECT Intersects(" + self.polygonString(polygons[0]) + ", " + self.polygonString(polygons[1]) + ")"
		self.cursor.execute(query)
		rows = self.cursor.fetchall()
		flag = False
		for row in rows:
			# print row[0]
			if row[0] == 1: flag = True
		return flag

	def runQueries(self, queries, numberOfExecutions):
		results = {'database': 'spatialite  - ' + self.db, 'queries': list()}
		allQueries = len(queries) * numberOfExecutions
		n = 0
		for query in queries:
			queryObject = {'name': query, 'times': list(), 'avg': 0}
			for x in range(0, numberOfExecutions):
				startTime = time.time()
				result = self.cursor.execute(query)
				endTime = time.time()
				executionTime = 1000 * (endTime - startTime) # milliseconds
				# for row in result:
				# 	print row[0]
				queryObject['times'].append(executionTime)
				n = n + 1
				if n % (math.ceil(allQueries/10.0)) == 0:
					print('\tFinished: ' + str(n * 100.0 / allQueries) + '%')
			results['queries'].append(queryObject)

		for query in results['queries']:
			avg = 0
			x = 0.0
			for val in query['times']:
				avg = avg + val
				x = x + 1

			avg = avg / x
			query['avg'] = avg

		return results

	def test(self):
		try:
			init = 'SELECT InitSpatialMetadata()'
			self.cursor.execute(init)
		except:
			print "Did already run InitSpatialMetadata"

		createTable = "CREATE TABLE test (x INTEGER, y INTEGER)"
		self.cursor.execute(createTable)

		self.cursor.execute("INSERT INTO test (x, y) VALUES (1,2)")
		self.cursor.execute("INSERT INTO test (x, y) VALUES (2,6)")
		self.connection.commit()

		query = "SELECT x,y FROM test WHERE x = 1"
		start = time.time()
		result = self.cursor.execute(query)
		end = time.time()
		for row in result:
			print row
		deltatime = 1000 * (end - start)
		print "Time: " + str(deltatime) + " ms"

