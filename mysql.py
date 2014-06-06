import MySQLdb

class Mysql:

	def __init__(self):
		self.db = self.connect()
		self.cursor = self.db.cursor()
		self.cursor.execute('SET PROFILING = 1')

	def connect(self):
		return MySQLdb.connect(host="192.168.30.136", # your host, usually localhost
						 port=3306,
	                     user="gis", # your username
	                      passwd="benchmark", # your password
	                      db="benchmark") # name of the data base

	def disconnect(self):
		self.db.close()

	def runQueries(self, queries, numberOfExecutions):
		n = 0
		results = {'database': 'MySQL', 'queries': {}}
		for query in queries:
			results['queries'][query] = {'times': list(), 'avg': 0}
			for x in range(0, numberOfExecutions):
				n = n + 1
				self.cursor.execute(query)
				if n % 15 == 0:
					self.cursor.execute('SHOW PROFILES')
					for row in self.cursor:
						results['queries'][row[2]]['times'].append(1000 * row[1])	# Time in Milliseconds
		if n % 15 != 0:
			self.cursor.execute('SHOW PROFILES')
			for x, row in enumerate(self.cursor):
				if n < 15:
					results['queries'][row[2]]['times'].append(1000 * row[1])		# Time in Milliseconds
				elif x >= 15 - (n % 15):
					results['queries'][row[2]]['times'].append(1000 * row[1])		# Time in Milliseconds

		for query in results['queries']:
			avg = 0
			x = 0.0
			for val in results['queries'][query]['times']:
				avg = avg + val
				x = x + 1

			avg = avg / x
			results['queries'][query]['avg'] = avg

		return results