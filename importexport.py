import argparse
import sys
import re
from db import mysql, postgis, spatialite, monet_db
import decimal

supported_databases = ("mysql", "postgis", "postgresql", "sqlite", "spatialite", "monetdb")
chunk_size = 10000

def loadTableNames(tablesfile):
	return [line.rstrip() for line in tablesfile]

def initSourceDb(source_db):
	if source_db == "mysql":
		return mysql.MySQL()
	else:
		print "The input database is not supported!"
		sys.exit(2)

def initDestinationDb(destination_db):
	if destination_db == "postgis" or destination_db == "postgresql":
		db = postgis.Postgis()
		param = "%s"
		spatial_column_extra = False
		buffer_insteadof_string = False
		geomFunction = "ST_GeomFromText"
		return db, {'param': param,
				'spatial_column_extra': spatial_column_extra,
				'buffer_insteadof_string': buffer_insteadof_string,
				'geomFunction': geomFunction}
	elif destination_db == "spatialite" or destination_db == "sqlite":
		db = spatialite.Spatialite('benchmark.db')
		param = "?"
		spatial_column_extra = True
		buffer_insteadof_string = True
		init = 'SELECT InitSpatialMetadata()'
		db.cursor.execute(init)
		geomFunction = "ST_GeomFromText"
		return db, {'param': param,
				'spatial_column_extra': spatial_column_extra,
				'buffer_insteadof_string': buffer_insteadof_string,
				'geomFunction': geomFunction}
	elif destination_db == "monetdb":
		db = monet_db.Monet_db()
		param = "%s"
		spatial_column_extra = False
		buffer_insteadof_string = False
		geomFunction = "GeomFromText"
		return db, {'param': param,
				'spatial_column_extra': spatial_column_extra,
				'buffer_insteadof_string': buffer_insteadof_string,
				'geomFunction': geomFunction}
	else:
		print "The output database is not supported!"
		sys.exit(2)

def retrieveColumns(db, table):
	show_columns = "SHOW COLUMNS FROM " + table
	db.cursor.execute(show_columns)
	columns = []
	for row in db.cursor:
		columns.append({'name': row[0], 'type': row[1], 'null': row[2], 'key': row[3]})
	return columns

def dropTable(db, table):
	db.dropTable(table)
	

def createTable(db, settings, table, columns):
	createTable = "CREATE TABLE " + table + " ("
	for column in columns:
		if settings['spatial_column_extra'] and column['type'] == "geometry":
			continue
		createTable += column['name']
		int_pattern = re.compile("int")
		double_pattern = re.compile("double")
		if int_pattern.match(column['type']):
			createTable += ' integer'
		elif double_pattern.match(column['type']):
			createTable += ' double'
		else:
			createTable += ' ' + column['type']
		if column['key'] == 'PRI': createTable += ' ' + 'PRIMARY KEY'
		if column['null'] == 'NO': createTable += ' ' + 'NOT NULL'
		createTable += ', '
	createTable = createTable[:-2] + ")"
	# print createTable
	db.cursor.execute(createTable)
	db.connection.commit()

def createSpatialColumn(db, settings, table):
	try:
		addColumn = "SELECT AddGeometryColumn (" + settings['param'] + ", 'SHAPE', 4326, 'GEOMETRY', 2)"
		db.cursor.execute(addColumn, [str(table)])
		db.connection.commit()
	except:
		print "Could not add geometry column"

def prepareInsertion(db, table, chunk_size):
	id_min = 0
	id_max = 0

	selectminmax = "SELECT min(OGR_FID), max(OGR_FID) FROM " + table
	db.cursor.execute(selectminmax)
	for row in db.cursor:
		id_min = row[0]
		id_max = row[1]
	start = id_min
	end = id_min + chunk_size
	return start, end, id_max

def prepareSelectChunk(table, columns, start, end):
	select = "SELECT "
	for column in columns:
		if column['type'] == "geometry":
			select += "AsText(" + column['name'] + "), "
		else:
			select += column['name'] + ', '
	return select[:-2] + " FROM " + table + " WHERE OGR_FID >= " + str(start) + " AND OGR_FID < " + str(end)

def prepareInsertStmt(table, columns, settings):
	insert = "INSERT INTO " + table + " ("
	for column in columns:
		insert += column['name'] + ", "
	insert = insert[:-2] + ") VALUES ("
	for column in columns:
		if column['type'] == "geometry":
			insert += settings['geomFunction'] + "(" + settings['param'] + ", 4326), "
		else:
			insert += settings['param'] + ", "
	insert = insert[:-2] + ")"
	return insert

def prepareInsertValues(row_values, settings):
	values = list()
	for value in row_values:
		# print type(value)
		if type(value) is decimal.Decimal:
			values.append(float(value))
		elif settings['buffer_insteadof_string'] and type(value) is str:
			values.append(buffer(value))
		elif type(value) is long:
			values.append(int(value))
		else:
			values.append(value)
	return values

def selectDataChunkAndInsert(in_db, out_db, table, columns, start, end, out_settings):
	select = prepareSelectChunk(table, columns, start, end)
	in_db.cursor.execute(select)

	insert = prepareInsertStmt(table, columns, out_settings)
	# print insert

	for row in in_db.cursor:
		values = prepareInsertValues(row, out_settings)
		out_db.cursor.execute(insert, tuple(values))
		out_db.connection.commit()

def importExport(source_db, destination_db, tableNames):
	in_db = initSourceDb(source_db)
	out_db, out_settings = initDestinationDb(destination_db)

	for table in tableNames:
		columns = retrieveColumns(in_db, table)

		dropTable(out_db, table)
		createTable(out_db, out_settings, table, columns)

		if out_settings['spatial_column_extra']:
			createSpatialColumn(out_db, out_settings, table)

		start, end, id_max = prepareInsertion(in_db, table, chunk_size)

		# let's build a do while loop:
		while True:
			selectDataChunkAndInsert(in_db, out_db, table, columns, start, end, out_settings)
			if end > id_max:
			# if end > 100:			# Only for debugging
				break
			print end
			start = end
			end += chunk_size

		print "Finished table " + table

if __name__ == "__main__":
	# Create an ArgumentParser and set the arguments we would like to have:
	parser = argparse.ArgumentParser(description='Export geometry tables from one database to another.')
	parser.add_argument('source_db', help="The name of the database where you want to export the tables from.")
	parser.add_argument('destination_db', help="The name of the database where you want to export the tables to.")
	parser.add_argument('file', type=argparse.FileType('r'),
						help="A file that contains all table names you want to export.")
	args = parser.parse_args()

	# Test whether the given databases are not in the list of supported databases, if so stop the program
	if args.source_db not in supported_databases or args.destination_db not in supported_databases:
		print "One of your databases is not supported. Try one of the following ones:"
		for database in supported_databases:
			print " * " + database
		sys.exit(2)
	tableNames = loadTableNames(args.file)

	importExport(args.source_db, args.destination_db, tableNames)