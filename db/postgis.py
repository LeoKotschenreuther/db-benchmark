import psycopg2

class Postgis:

	def __init__(self):
		self.connection = self.connect()
		self.cursor = self.connection.cursor()

	def connect(self):
		conn_string = "host='192.168.30.92' dbname='benchmark' user='gis' password='benchmark'"
		return psycopg2.connect(conn_string) 

	def disconnect(self):
		self.connection.close()

	def dropTable(self, table):
		drop = "DROP TABLE IF EXISTS " + table
		self.cursor.execute(drop)
		self.connection.commit()