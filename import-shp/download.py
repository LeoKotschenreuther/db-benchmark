import urllib

filedirectory = './shapefiles/'

print "Downloading edges_merge"
for x in range(48001, 48508, 2):
	print x
	urllib.urlretrieve('ftp://ftp2.census.gov/geo/tiger/TIGER2014/EDGES/tl_2014_' + str(x) + '_edges.zip', filedirectory + 'edges_merge/tl_2014_' + str(x) + '_edges.zip')

print "Downloading pointlm_merge"
urllib.urlretrieve('ftp://ftp2.census.gov/geo/tiger/TIGER2014/POINTLM/tl_2014_48_pointlm.zip', filedirectory + 'pointlm_merge/tl_2014_48_pointlm.zip')

print "Downloading arealm_merge"
urllib.urlretrieve('ftp://ftp2.census.gov/geo/tiger/TIGER2014/AREALM/tl_2014_48_arealm.zip', filedirectory + 'arealm_merge/tl_2014_48_arealm.zip')

print "Downloading areawater_merge"
for x in range(48001, 48508, 2):
	print x
	urllib.urlretrieve('ftp://ftp2.census.gov/geo/tiger/TIGER2014/AREAWATER/tl_2014_' + str(x) + '_areawater.zip', filedirectory + 'areawater/tl_2014_' + str(x) + '_areawater.zip')

print "Already downloaded gnis_names09"

print "Downloading DFIRM Database"
urllib.urlretrieve('ftp://ftp.ci.austin.tx.us/GIS-Data/Regional/environmental/Travis_County_DFIRM_DB.zip', filedirectory + 'Travis_County_DFIRM_DB.zip')

print "Downloading land_use_2012"
urllib.urlretrieve('ftp://ftp.ci.austin.tx.us/GIS-Data/Regional/landuse/land_use_2012.zip', filedirectory + 'land_use_2012.zip')

print "Downloading hospitals"
urllib.urlretrieve('ftp://ftp.ci.austin.tx.us/GIS-Data/Regional/regional/hospitals.zip', filedirectory + 'hospitals.zip')

print "Downloading landfills"
urllib.urlretrieve('ftp://ftp.ci.austin.tx.us/GIS-Data/Regional/landuse/landfills.zip', filedirectory + 'landfills.zip')
