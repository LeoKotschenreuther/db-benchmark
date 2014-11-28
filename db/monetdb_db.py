import monetdb.sql
import math

class Monetdb:

	def __init__(self):
		self.db = self.connect()
		self.cursor = self.db.cursor()
		# self.cursor.execute('SET PROFILING = 1')

	def connect(self):
		return monetdb.sql.connect( username="monetdb",
									password="monetdb",
									hostname="192.168.30.92",
									database="benchmark")

	def disconnect(self):
		self.db.close()

	def pointString(self, point):
		return "Point(" + str(point['x']) + " " + str(point['y']) + ")"

	def polygonString(self, polygon):
		string = "Polygon(("
		for point in polygon:
			string += str(point['x']) + " " + str(point['y']) + ","
		string += str(polygon[0]['x']) + " " + str(polygon[0]['y']) + "))"
		return string

	def lineString(self, line):
		string = "Linestring("
		for point in line:
			string += str(point['x']) + " " + str(point['y']) + ","
		string = string[:-1] + ")"
		return string

	# def isPolygonValid(self, polygon):
	# 	query = "SELECT ISVALID(GeomFromText('" + self.polygonString(polygon) + "', 4326)"
	# 	try:
	# 		self.cursor.execute(query)
	# 		rows = self.cursor.fetchall()
	# 		isValid = False
	# 		for row in rows:
	# 			isValid = row[0]
	# 		return isValid
	# 	except Exception as e:
	# 		print e
	# 		return False

	def dropCreateTable(self, table):
		checkTable = "SELECT name FROM tables WHERE name like '" + table + "'"
		result = self.cursor.execute(checkTable)
		if result > 0:
			dropTable = "DROP TABLE " + table
			self.cursor.execute(dropTable)
			self.db.commit()
			print("\tDropped Table")
		createTable = ""
		if table == 'polygons':
			createTable = "CREATE TABLE polygons (ID INT, size INT, polygon GEOMETRY)"
		elif table == 'lines':
			createTable = "CREATE TABLE lines (ID INT, size INT, line GEOMETRY)"
		elif table == 'points':
			createTable = "CREATE TABLE points (ID INT, x FLOAT, y FLOAT, point POINT)"
		self.cursor.execute(createTable)
		self.db.commit()
		print("\tCreated Table")

	def insertPolygons(self, polygons, offset):
		for i, polygon in enumerate(polygons):
			size = len(polygon)
			insert = '''INSERT INTO polygons (ID, SIZE, polygon) VALUES (%s, %s, PolygonFromText(%s, 4326))'''
			self.cursor.execute(insert, (i + offset, size, self.polygonString(polygon)))
			if i % 1000 == 999:
				print "finished: " + str(i+1)
		self.db.commit()
		print("\tInserted Polygons into polygons table")

	def insertLines(self, lines, offset):
		for i, line in enumerate(lines):
			size = len(line)
			insert = '''INSERT INTO lines (ID, SIZE, line) VALUES (%s, %s, LineFromText(%s, 4326))'''
			self.cursor.execute(insert, (i + offset, size, self.lineString(line)))
			if i % 1000 == 999:
				print "finished: " + str(i+1)
		self.db.commit()
		print("\tInserted Lines into lines table")

	def insertPoints(self, points):
		for i, point in enumerate(points):
			# print self.pointString(point)
			insert = '''INSERT INTO points (ID, X, Y, POINT) VALUES (%s, %s, %s, PointFromText(%s, 4326))'''
			self.cursor.execute(insert, (i, point['x'], point['y'], self.pointString(point)))
			if i % 1000 == 999:
				print "finished: " + str(i+1)
		self.db.commit()
		print("\tInserted Points into Points table")

	def runQueries(self, queries, numberOfExecutions):
		allQueries = len(queries) * numberOfExecutions
		n = 0
		results = {'database': 'Monetdb', 'queries': list()}
		# self.cursor.execute("call sys.querylog_enable()")
		for query in queries:
			results['queries'].append({'name': query, 'times': list(), 'avg': 0})
			for x in range(0, numberOfExecutions):
				n = n + 1
				self.cursor.execute(query)
				if n % (math.ceil(allQueries/10.0)) == 0:
					print('\tFinished: ' + str(n * 100.0 / allQueries) + '%')

		# self.cursor.execute("select id, query, avg(run) from sys.querylog_history group by id,query")
		# self.cursor.execute("call sys.querylog_disable()")
		# self.cursor.execute("call sys.querylog_empty()")
		# print self.cursor.fetchall()
				# if n % 15 == 0:
				# 	self.cursor.execute('SHOW PROFILES')
				# 	for row in self.cursor:
				# 		queryObject = {}
				# 		for x in results['queries']:
				# 			if x['name'] == row[2]:
				# 				queryObject = x
				# 				break
				# 		queryObject['times'].append(1000 * row[1])	# Time in Milliseconds
				# if n % (math.ceil(allQueries/10.0)) == 0:
				# 	print('\tFinished: ' + str(n * 100.0 / allQueries) + '%')
		# if n % 15 != 0:
		# 	self.cursor.execute('SHOW PROFILES')
		# 	for x, row in enumerate(self.cursor):
		# 		if n < 15 or x >= 15 - (n % 15):
		# 			queryObject = {}
		# 			for x in results['queries']:
		# 				if x['name'] == row[2]:
		# 					queryObject = x
		# 					break
		# 			queryObject['times'].append(1000 * row[1])		# Time in Milliseconds

		# for query in results['queries']:
		# 	avg = 0
		# 	x = 0.0
		# 	for val in query['times']:
		# 		avg = avg + val
		# 		x = x + 1

		# 	avg = avg / x
		# 	query['avg'] = avg

		return results