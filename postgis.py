import psycopg2


def run_query(query):
	conn_string = "host='192.168.30.136' dbname='benchmark' user='gis' password='benchmark'"
	conn = psycopg2.connect(conn_string)
	cur = conn.cursor()

	query = 'EXPLAIN ANALYZE ' + query

	cur.execute(query)

	# return cur.fetchall()

class Postgis:

	def __init__(self):
		self.db = self.connect()
		self.cursor = self.db.cursor()

	def connect(self):
		conn_string = "host='192.168.30.136' dbname='benchmark' user='gis' password='benchmark'"
		return psycopg2.connect(conn_string) 

	def disconnect(self):
		self.db.close()

	def runQueries(self, queries, numberOfExecutions):
		results = {}
		for query in queries:
			results[query] = {'times': list(), 'avg': 0}
			for x in range(0, numberOfExecutions):
				query_string = 'EXPLAIN ANALYZE ' + query
				self.cursor.execute(query_string)
				n = 0
				for row in self.cursor:
					if n == 4:
						# row looks like: "Total runtime: 123.456 ms"
						# Remove the first 15 chars and the last 3 chars to get only the measured time
						results[query]['times'].append(float(row[0][15:len(row[0]) - 3]))

					n = n + 1

		for query in results:
			avg = 0
			x = 0.0
			for val in results[query]['times']:
				avg = avg + val
				x = x + 1

			avg = avg / x
			results[query]['avg'] = avg

		return results
