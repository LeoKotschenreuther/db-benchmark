import argparse
import sys
import re
from db import mysql, postgis, spatialite
import decimal

supported_databases = ("mysql", "postgis", "postgresql", "sqlite", "spatialite")
chunk_size = 10000

def loadTableNames(tablesfile):
	return [line.rstrip() for line in tablesfile]

def importExport(source_db, destination_db, tableNames):
	in_db = None
	out_db = None
	out_param = "%s"
	spatial_column_extra = False

	if source_db == "mysql":
		in_db = mysql.MySQL()
	else:
		print "Input database not supported"
		sys.exit(2)

	if destination_db == "postgis" or destination_db == "postgresql":
		out_db = postgis.Postgis()
		out_param = "%s"
		spatial_column_extra = False
	elif destination_db == "spatialite" or destination_db == "sqlite":
		out_db = spatialite.Spatialite('benchmark.db')
		out_param = "?"
		spatial_column_extra = True
		init = 'SELECT InitSpatialMetadata()'
		out_db.cursor.execute(init)
	else:
		print "Output database not supported"
		sys.exit(2)


	for table in tableNames:
		# List all columns and save them
		show_columns = "SHOW COLUMNS FROM " + table
		in_db.cursor.execute(show_columns)
		columns = []
		for row in in_db.cursor:
			columns.append({'name': row[0], 'type': row[1], 'null': row[2], 'key': row[3]})

		# Now let's drop the old table if it exists so we are shure it's empty
		try:
			dropTable = "DROP TABLE " + table
			out_db.cursor.execute(dropTable)
			out_db.connection.commit()
		except:
			"table " + table + " doesn't exist"

		# Let's build the create table string with all columns we need
		createTable = "CREATE TABLE " + table + " ("
		for column in columns:
			if spatial_column_extra and column['type'] == "geometry":
				continue
			createTable += column['name']
			int_pattern = re.compile("int")
			double_pattern = re.compile("double")
			if int_pattern.match(column['type']):
				createTable += ' integer'
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

		# If we need to add the spatial column extra, do it here
		if spatial_column_extra:
			try:
				addColumn = "SELECT AddGeometryColumn (" + out_param + ", 'SHAPE', 4326, 'GEOMETRY', 2)"
				out_db.cursor.execute(addColumn, [str(table)])
			except:
				print "Could not add geometry column"
		out_db.connection.commit()


		id_min = 0
		id_max = 0

		selectminmax = "SELECT min(OGR_FID), max(OGR_FID) FROM " + table
		in_db.cursor.execute(selectminmax)
		for row in in_db.cursor:
			id_min = row[0]
			id_max = row[1]
		start = id_min
		end = id_min + chunk_size

		# let's build a do while loop:
		while True:
			selectAll = "SELECT "
			for column in columns:
				if column['type'] == "geometry":
					selectAll += "AsText(" + column['name'] + "), "
				else:
					selectAll += column['name'] + ', '
			selectAll = selectAll[:-2] + " FROM " + table + " WHERE OGR_FID >= " + str(start) + " AND OGR_FID < " + str(end)
			in_db.cursor.execute(selectAll)
			for row in in_db.cursor:
				insert = "INSERT INTO " + table + " ("
				for column in columns:
					insert += column['name'] + ", "
				insert = insert[:-2] + ") VALUES ("
				for column in columns:
					if column['type'] == "geometry":
						insert += "ST_GeomFromText(" + out_param + ", 4326), "
					else:
						insert += out_param + ", "
				insert = insert[:-2] + ")"
				# print insert
				values = list()
				for value in row:
					if type(value) is decimal.Decimal:
						values.append(float(value))
					else:
						values.append(value)
				out_db.cursor.execute(insert, tuple(values))
				out_db.connection.commit()
			if end > id_max:
			# if end > 100:			Only for debugging
				break
			print end
			start = end
			end += chunk_size

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