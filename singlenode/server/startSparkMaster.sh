#!/bin/bash

#Start the ssh server.
/etc/init.d/ssh start

#Start the Hadoop daemons.
start-dfs.sh
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver

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

#The metastorecreated file is used to indicate if the metastore has been
#created previously.
if [ ! -f /metastore/metastorecreated ]; then
   schematool -dbType postgres -initSchema --verbose
   #Due to permission issues, the command needs to be run with sudo and inside a bash command.
   sudo -u $USER_NAME_DC bash -c 'echo "metastorecreated" > /metastore/metastorecreated'
fi

hive --service metastore &
wait_for_server localhost 9083 24
hive --service hiveserver2 &
wait_for_server localhost 10000 24
bash /opt/spark-2.4.0-bin-hadoop2.7/sbin/start-all.sh
wait_for_server localhost 8080 24 
bash /opt/spark-2.4.0-bin-hadoop2.7/sbin/start-history-server.sh
wait_for_server localhost 18080 24 
if [[ $RUN_THRIFT_SERVER -eq 1 ]]; then
	#Must force the logs to be created inside the USER_NAME_DC home directory.   
    echo "SPARK_LOG_DIR=/home/$USER_NAME_DC/logs" >> $SPARK_HOME/conf/spark-env.sh 
    #Execute the thrift server the user USER_NAME_DC.
    #The jars should be downloaded to /home/USER_NAME_DC/.ivy2
    #Note that the eventLog.dir parameter uses the previously created directory.
    sudo -u $USER_NAME_DC mkdir /home/$USER_NAME_DC/tmp          
	sudo -u $USER_NAME_DC bash /opt/spark-2.4.0-bin-hadoop2.7/sbin/start-thriftserver.sh \
		--master spark://namenodecontainer:7077  \
		--hiveconf hive.server2.thrift.port=10015 \
		--conf spark.eventLog.dir=/home/$USER_NAME_DC/tmp \
		--packages org.apache.zookeeper:zookeeper:3.4.6     
	wait_for_server localhost 10015 72       
fi
sleep infinity




 

