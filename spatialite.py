import sqlite3
import csv
import time
from pyspatialite import dbapi2 as db

class Spatialite:

	def __init__(self):
		self.connection = self.connect()
		self.cursor = self.connection.cursor()

	def connect(self):
		return db.connect(':memory:')

	def disconnect(self):
		self.connection.close()

	def setUpDB(self):

		init = 'SELECT InitSpatialMetadata()'
		self.cursor.execute(init)

		createTable = "CREATE TABLE test (x INTEGER, y INTEGER)"
		self.cursor.execute(createTable)
		addPointColumn = "SELECT AddGeometryColumn ('test', 'point', 4326, 'POINT', 2)"
		self.cursor.execute(addPointColumn)

		with open('data.csv','rb') as csvfile:
			reader = csv.reader(csvfile)
			to_db = [(i[0], i[1]) for i in reader]

		self.cursor.executemany("INSERT INTO test (x, y) VALUES (?, ?);", to_db)
		self.connection.commit()
		self.cursor.execute("UPDATE test set point = MakePoint(x, y, 4326)")
		self.connection.commit()

	def runQueries(self, queries, numberOfExecutions):
		results = {'database': 'spatialite', 'queries': {}}
		for query in queries:
			results['queries'][query] = {'times': list(), 'avg': 0}
			for x in range(0, numberOfExecutions):
				startTime = time.clock()
				self.cursor.execute(query)
				executionTime = 1000 * (time.clock() - startTime) # milliseconds
				results['queries'][query]['times'].append(executionTime)

		for query in results['queries']:
			avg = 0
			x = 0.0
			for val in results['queries'][query]['times']:
				avg = avg + val
				x = x + 1

			avg = avg / x
			results['queries'][query]['avg'] = avg

		return results