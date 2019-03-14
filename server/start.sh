#!/bin/bash

#Start the ssh server.
/etc/init.d/ssh start

#Format the hdfs namenode.
hdfs namenode -format

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
usermod -a -G supergroup hive && \
hadoop fs -put /temporal /temporal

#Initialize the metastore if it has not been created.

if [ ! -f /metastore/metastore_db/db.lck ]; then
   #Run the hiveserver in a subshell and the schematool in another. Add a sleep
   #infinity to the second subshell to prevent the exit of the container after
   #the completion of schematool.
   (hiveserver2) & (schematool -dbType derby -initSchema --verbose; sleep infinity)
else
   hiveserver2
fi



