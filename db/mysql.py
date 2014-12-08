import MySQLdb

class MySQL:
	def __init__(self):
		self.db = self.connect()
		self.cursor = self.db.cursor()

	def connect(self):
		return MySQLdb.connect(host="192.168.30.92", # your host, usually localhost
						 port=3306,
	                     user="gis", # your username
	                      passwd="benchmark", # your password
	                      db="benchmark") # name of the data base

	def disconnect(self):
		self.db.close()