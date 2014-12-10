from pyspatialite import dbapi2 as db

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