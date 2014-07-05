import psycopg2
import math
import time

class Postgis:

	def __init__(self):
		self.connection = self.connect()
		self.cursor = self.connection.cursor()

	def connect(self):
		conn_string = "host='192.168.30.136' dbname='benchmark' user='gis' password='benchmark'"
		return psycopg2.connect(conn_string) 

	def disconnect(self):
		self.connection.close()

	def polygonString(self, polygon):
		# ST_GeomFromText('Polygon((-0.8 0.7,-0.6 0.7,-0.6 0.4,-0.8 0.4,-0.8 0.7))', 4326)
		string = "ST_GeomFromText('Polygon(("
		for point in polygon:
			string += str(point['x']) + " " + str(point['y']) + ","
		string += str(polygon[0]['x']) + " " + str(polygon[0]['y']) + "))', 4326)"
		return string

	def isPolygonValid(self, polygon):
		query = "SELECT ST_ISVALID(" + self.polygonString(polygon) + ")"
		self.cursor.execute(query)
		rows = self.cursor.fetchall()
		isValid = False
		for row in rows:
			isValid = row[0]
		return isValid

	def dropCreateTable(self, table):
		dropPolygons = "DROP TABLE IF EXISTS " + table
		self.cursor.execute(dropPolygons)
		print("\tDropped Table")
		createPolygonTable = "CREATE TABLE " + table + " (ID integer, size integer, polygon geometry(POLYGON, 4326))"
		self.cursor.execute(createPolygonTable)
		self.connection.commit()
		print("\tCreated Table")

	def insertPolygons(self, polygons, polygonSize):
		for i, polygon in enumerate(polygons):
			insert = "INSERT INTO POLYGONS (ID, SIZE, polygon) VALUES (" + str(i) + ", " + str(polygonSize) + ", " + self.polygonString(polygon) + ")"
			self.cursor.execute(insert)
			self.connection.commit()
		print("\tInserted Polygons into polygons table")

	def checkIntersection(self, polygons):
		query = "SELECT ST_Intersects(" + self.polygonString(polygons[0]) + ", " + self.polygonString(polygons[1]) + ")"
		self.cursor.execute(query)
		rows = self.cursor.fetchall()
		flag = False
		for row in rows:
			flag = row[0]
		return flag

	def runQueriesPoly(self, queries, numberOfExecutions, polygonSize):
		print '\tPolygonsize: ' + str(polygonSize)
		results = self.runQueries(queries, numberOfExecutions)
		results['polygonSize'] = polygonSize
		return results

	def runQueries(self, queries, numberOfExecutions):
		results = {'database': 'postgis', 'queries': list()}
		allQueries = len(queries) * numberOfExecutions
		n = 0
		for query in queries:
			queryObject = {'name': query, 'times': list(), 'avg': 0}
			# self.cursor.execute(query)
			# for row in self.cursor:
			# 	print row
			for x in range(0, numberOfExecutions):
				query_string = 'EXPLAIN ANALYZE ' + query
				# try:
				self.cursor.execute(query_string)
				for row in self.cursor:
					if row[0][0:15] == 'Total runtime: ':
						# row looks like: "Total runtime: 123.456 ms"
						# Remove the first 15 chars and the last 3 chars to get only the measured time
						# Time is already in milliseconds
						queryObject['times'].append(float(row[0][15:len(row[0]) - 3]))
				n = n + 1
				if n % (math.ceil(allQueries/10.0)) == 0:
					print('\tFinished: ' + str(n * 100.0 / allQueries) + '%')
				# except psycopg2.InternalError:
				# 	print query 

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
		dropTable = "DROP TABLE IF EXISTS testSelect"
		self.cursor.execute(dropTable)
		createTable = "CREATE TABLE testSelect (x INTEGER, y INTEGER)"
		self.cursor.execute(createTable)

		self.cursor.execute("INSERT INTO testSelect (x, y) VALUES (1,2)")
		self.cursor.execute("INSERT INTO testSelect (x, y) VALUES (2,6)")
		self.connection.commit()

		query = "SELECT x,y FROM testSelect WHERE x = 1"
		start = time.time()
		self.cursor.execute(query)
		end = time.time()
		for row in self.cursor:
			print row
		deltatime = 1000 * (end - start)
		print "Time: " + str(deltatime) + " ms"
		timeQuery = "EXPLAIN " + query
		start = time.time()
		self.cursor.execute(timeQuery)
		end = time.time()
		deltatime = 1000 * (end - start)
		print "Time: " + str(deltatime) + " ms"
		for row in self.cursor:
			print row