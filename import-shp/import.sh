#!/bin/bash
# $0 is the name of the shell-script
# 
# $1 is the folder where to find the zip-file
# $2 is the name of the zipfile
# $3 is the tablename where you want to store the data

databases="mysql postgresql"

if [ "$#" -ne 5 ]; then
	echo "usage: sh import.sh <database> <directory> <zipfile> <shapefile> <tablename>"
	exit 2
fi

if ! [[ $databases =~ $1 ]]; then
	echo "Given database is not supported."
	echo "The following databases are supported:"
	for database in $databases; do
		echo " * "$database
	done
	exit 2
fi

mkdir tmp

source_file="$1/$2"
destination="./tmp/$2"

cp "$source_file" "$destination"

cd tmp
unzip "$2" -d ./files

directory=`echo $2 | cut -d'.' -f 1`
cd files

if [ $1 == "mysql" ]; then
	# import to mysql
	ogr2ogr -f "MySQL" MySQL:"mysql,user=root,host=localhost,password=admin" -lco engine=MYISAM "$4" -append -skipfailures -nln "$5"
elif [ $1 == "postgresql" ]; then
	# import to postgresql
	shp2pgsql -I -s 4326 "$4" benchmark."$5-" | psql -d benchmark
fi

cd ../../
rm -rf tmp