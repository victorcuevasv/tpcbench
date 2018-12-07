#!/bin/bash

# $1 host $2 port $3 tries
wait_for_server() {
	if [ $# -lt 3 ]; then
    	echo 'ERROR: not enough arguments in function wait_for_server.'
    	exit 1
	fi
	printf "\nConnecting to $1:$2.\n"
	i=0
	while ! nc -z $1 $2 >/dev/null 2>&1 < /dev/null; do
  		if [ $i -ge $3 ]; then
    	exit 1
  	fi
  	i=$((i+1))
  	printf "$1:$2 is unreachable, retrying.\n"
  	sleep 2
	done
	printf "$1:$2 is reachable.\n"
}

if [ ! -f /metastore/metastorecreated ]; then
   schematool -dbType postgres -initSchema --verbose
   echo "metastorecreated" > /metastore/metastorecreated
fi

hive --service metastore &
wait_for_server localhost 9083 5
hiveserver2 


#if [ ! -f /metastore/metastorecreated ]; then
#   (hiveserver2) & (schematool -dbType postgres -initSchema --verbose; \
#   echo "metastorecreated" > /metastore/metastorecreated ; sleep infinity)
#else
#   hiveserver2
#fi
