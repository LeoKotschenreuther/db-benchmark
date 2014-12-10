import MySQLdb

class MySQL:
	def __init__(self):
		self.connection = self.connect()
		self.cursor = self.connection.cursor()

	def connect(self):
		return MySQLdb.connect(host="192.168.30.92", # your host, usually localhost
						 port=3306,
	                     user="gis", # your username
	                      passwd="benchmark", # your password
	                      db="benchmark") # name of the data base

	def disconnect(self):
		self.connection.close()

	def dropTable(self, table):
		drop = "DROP TABLE IF EXISTS " + table
		self.cursor.execute(drop)
		self.connection.commit()