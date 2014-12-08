import argparse
import sys
import re
from db import mysql, postgis

supported_databases = ("mysql", "postgis", "postgresql")

def loadTableNames(tablesfile):
	return [line.rstrip() for line in tablesfile]

def importExport(source_db, destination_db, tableNames):
	in_db = None
	out_db = None
	if source_db == "mysql":
		in_db = mysql.MySQL()
	# elif input_db == "postgis" or input_db == "postgresql":
	# 	in_db = postgis.Postgis()
	else:
		sys.exit(2)

	if destination_db == "mysql":
		# out_db = mysql.MySQL()
		sys.exit(2)
	elif destination_db == "postgis" or destination_db == "postgresql":
		out_db = postgis.Postgis()
	else:
		sys.exit(2)

	for table in tableNames:
		show_columns = "SHOW COLUMNS FROM " + table
		in_db.cursor.execute(show_columns)
		columns = []
		for row in in_db.cursor:
			columns.append({'name': row[0], 'type': row[1], 'null': row[2], 'key': row[3]})

		try:
			dropTable = "DROP TABLE " + table
			out_db.cursor.execute(dropTable)
			out_db.connection.commit()
			
		except:
			"table " + table + " doesn't exist"

		createTable = "CREATE TABLE " + table + " ("
		for column in columns:
			createTable += column['name']
			int_pattern = re.compile("int")
			double_pattern = re.compile("double")
			if int_pattern.match(column['type']):
				createTable += ' ' + 'integer'
			elif double_pattern.match(column['type']):
				createTable += ' decimal'
			else:
				createTable += ' ' + column['type']
			if column['key'] == 'PRI': createTable += ' ' + 'PRIMARY KEY'
			if column['null'] == 'NO': createTable += ' ' + 'NOT NULL'
			createTable += ', '
		createTable = createTable[:-2] + ")"
		# print createTable

		out_db.cursor.execute(createTable)
		out_db.connection.commit()

		selectAll = "SELECT "
		for column in columns:
			if column['type'] == "geometry":
				selectAll += "AsText(" + column['name'] + "), "
			else:
				selectAll += column['name'] + ', '
		selectAll = selectAll[:-2] + " from " + table
		in_db.cursor.execute(selectAll)
		counter = 0
		for row in in_db.cursor:
			insert = "INSERT INTO " + table + " ("
			for column in columns:
				insert += column['name'] + ", "
			insert = insert[:-2] + ") VALUES ("
			for column in columns:
				if column['type'] == "geometry":
					insert += "ST_GeomFromText(%s, 4326), "
				else:
					insert += "%s, "
			insert = insert[:-2] + ")"
			out_db.cursor.execute(insert, row)
			counter += 1
			if counter % 1000 == 0:
				print counter
				out_db.connection.commit()

		out_db.connection.commit()
		print "Finished table " + table 

if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Export geometry tables from one database to another.')
	parser.add_argument('source_db', help="The name of the database where you want to export the tables from.")
	parser.add_argument('destination_db', help="The name of the database where you want to export the tables to.")
	parser.add_argument('file', type=argparse.FileType('r'), help="A file that contains all table names you want to export.")
	args = parser.parse_args()
	if args.source_db not in supported_databases or args.destination_db not in supported_databases:
		print "One of your databases is not supported. Try one of the following ones:"
		for database in supported_databases:
			print " * " + database
		sys.exit(2)
	tableNames = loadTableNames(args.file)
	importExport(args.source_db, args.destination_db, tableNames)