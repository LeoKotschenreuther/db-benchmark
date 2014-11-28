#!/bin/bashsq

databases="mysql postgresql"

if [ "$#" -ne 6 ]; then
	echo "usage: sh import.sh <database> <options> <directory> <zipfile> <shapefile> <tablename>"
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

source_file="$3/$4"
destination="./tmp/$4"

cp "$source_file" "$destination"

cd tmp
unzip "$4" -d ./files

directory=`echo $4 | cut -d'.' -f 1`
cd files

if [ $1 == "mysql" ]; then
	# import to mysql
	ogr2ogr -f "MySQL" MySQL:"mysql,user=root,host=localhost,password=admin" -lco engine=MYISAM "$5" -append -skipfailures -nln "$6"
elif [ $1 == "postgresql" ]; then
	# import to postgresql
	if [ $2 == "append" ]; then
		shp2pgsql -a -I -s 4326 "$5" "$6" | psql -U gis -d benchmark
	elif [ $2 == "drop" ]; then
		shp2pgsql -d -I -s 4326 "$5" "$6" | psql -U gis -d benchmark
	fi
fi

cd ../../
rm -rf tmp