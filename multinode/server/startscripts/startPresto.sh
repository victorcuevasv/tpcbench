#!/bin/bash

#Start the ssh server.
/etc/init.d/ssh start

#Start the Hadoop daemons.
start-dfs.sh
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver

#Create the Hive warehouse directory.
#Create a hive user and a supergroup group with hive as a member.
#Add the temporal directory holding the data to hdfs. 
hadoop fs -mkdir -p    /user/hive/warehouse  && \
hadoop fs -chmod g+w   /user/hive/warehouse && \
useradd hive && \
groupadd supergroup && \
usermod -a -G supergroup hive

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
  	sleep 5
	done
	printf "$1:$2 is reachable.\n"
}

if [ ! -f /metastore/metastorecreated ]; then
   schematool -dbType postgres -initSchema --verbose
   echo "metastorecreated" > /metastore/metastorecreated
fi

hive --service metastore &
wait_for_server localhost 9083 24
hive --service hiveserver2 &
wait_for_server localhost 10000 24
/opt/presto-server-0.214/bin/launcher run

#sleep infinity

 

