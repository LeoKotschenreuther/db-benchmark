import argparse
import sys
from db import mysql, postgis, spatialite, monet_db

supported_databases = ("mysql", "postgis", "postgresql", "sqlite", "spatialite", "monetdb")
chunk_size = 10000

def loadTableNames(tablesfile):
	return [line.rstrip() for line in tablesfile]

def initDb(db):
	if db == "mysql":
		return mysql.MySQL(), False
	elif db == "postgis" or db == "postgresql":
		return postgis.Postgis(), False
	elif db == "sqlite" or db == "spatialite":
		return spatialite.Spatialite('benchmark.db'), True
	elif db == "monetdb":
		return monet_db.Monet_db(), False

def retrieveTableCardinality(db, tableNames):
	my_db, resultObject = initDb(db)
	print "Table cardinalities for database " + db

	for table in tableNames:
		select = "SELECT COUNT(*) FROM " + table
		result = None
		if resultObject:
			result = my_db.cursor.execute(select)
		else:
			my_db.cursor.execute(select)
			result = my_db.cursor
		for row in result:
			print table + "\t\t" + str(row[0])

if __name__ == "__main__":
	# Create an ArgumentParser and set the arguments we would like to have:
	parser = argparse.ArgumentParser(description='Retrieve the table cardinalities for a list of tables in a database')
	parser.add_argument('db', help="The name of the database you want to use")
	parser.add_argument('file', type=argparse.FileType('r'),
						help="A file that contains all table names you want to check.")
	args = parser.parse_args()

	# Test whether the given databases are not in the list of supported databases, if so stop the program
	if args.db not in supported_databases:
		print "Your database is not supported. Try one of the following ones:"
		for database in supported_databases:
			print " * " + database
		sys.exit(2)
	tableNames = loadTableNames(args.file)

	retrieveTableCardinality(args.db, tableNames)