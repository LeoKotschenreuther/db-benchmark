import subprocess
import sys, getopt

supported_databases = ("mysql", "postgresql")

def importFile(database, options, location, zipfile, shpfile, table):
	subprocess.call(['sh', 'import.sh', database, options, location, zipfile, shpfile, table])

def run_import(database):
	filedirectory = 'shapefiles'
	options = "drop"

	print "Importing edges_merge"
	for x in range(48001, 48508, 2):
		print x
		if x != 48001:
			options = "append"
		importFile(database, options, filedirectory + '/edges_merge', 'tl_2014_' + str(x) + '_edges.zip', 'tl_2014_' + str(x) + '_edges.shp', 'edges_merge')
	options = "drop"

	print "Importing pointlm_merge"
	importFile(database, options, filedirectory + '/pointlm_merge', 'tl_2014_48_pointlm.zip', 'tl_2014_48_pointlm.shp', 'pointlm_merge')


	print "Importing arealm_merge"
	importFile(database, options, filedirectory + '/arealm_merge', 'tl_2014_48_arealm.zip', 'tl_2014_48_arealm.shp', 'arealm_merge')

	print "Importing areawater_merge"
	for x in range(48001, 48508, 2):
		print x
		if x != 48001:
			options = "append"
		importFile(database, options,  filedirectory + '/areawater_merge', 'tl_2014_' + str(x) + '_areawater.zip', 'tl_2014_' + str(x) + '_areawater.shp', 'areawater_merge')
	options = "drop"

	print "Importing gnis_names09"
	importFile(database, options, filedirectory, 'GNIS-2009.zip', 'GNIS-2009/gnis_names09.shp', 'gnis_names09')

	print "Importing DFIRM Database"
	importFile(database, options, filedirectory, 'DFIRM_DB.zip', 'DFIRM_DB/S_FLD_HAZ_AR.shp', 's_fld_haz_ar')
	importFile(database, options, filedirectory, 'DFIRM_DB.zip', 'DFIRM_DB/S_GEN_STRUCT.shp', 's_gen_struct')
	importFile(database, options, filedirectory, 'DFIRM_DB.zip', 'DFIRM_DB/S_WTR_AR.shp', 'a')

	print "Importing land_use_2012"
	importFile(database, options, filedirectory, 'land_use_2012.zip', 'land_use_2012.shp', 'land_use_2012')

	# print "Importing hospitals"
	# importFile(database, options, filedirectory, 'hospitals.zip', 'hospitals.shp', 'hospitals')

	print "Importing landfills"
	importFile(database, options, filedirectory, 'landfills.zip', 'landfills.shp', 'landfills')

def main(argv):
	database = ''

	usage = 'usage: import.py -d <database>'

	try:
		opts, args = getopt.getopt(argv, "hd:", ["database="])
		if len(opts) == 0 or len(opts) < len(args):
			print usage
			sys.exit(2)
	except getopt.GetoptError:
		print usage
		sys.exit(2)
	for opt, arg in opts:
		if opt == '-h':
			print usage
			sys.exit()
		elif opt in ("-d", "--database"):
			if arg in supported_databases:
				database = arg
			else:
				print "Given database is not supported."
				print "The following databases are supported:"
				for db in supported_databases:
					print " * " + str(db)
				sys.exit(2)
	run_import(database)

if __name__ == "__main__":
	main(sys.argv[1:])