import monetdb.sql as monetdblib

class Monet_db:

	def __init__(self):
		self.connection = self.connect()
		self.cursor = self.connection.cursor()

	def connect(self):
		return monetdblib.connect( username="monetdb",
									password="monetdb",
									hostname="192.168.30.92",
									database="benchmark")

	def disconnect(self):
		self.connection.close()

	def dropTable(self, table):
		checkTable = "SELECT name FROM tables WHERE name like '" + table + "'"
		result = self.cursor.execute(checkTable)
		if result > 0:
			dropTable = "DROP TABLE " + table
			self.cursor.execute(dropTable)
			self.connection.commit()