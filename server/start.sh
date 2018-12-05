#!/bin/bash

/etc/init.d/ssh start
mr-jobhistory-daemon.sh stop historyserver 
stop-yarn.sh 
stop-dfs.sh
start-dfs.sh 
start-yarn.sh
mr-jobhistory-daemon.sh start historyserver 

if [ ! -f /metastore_db/db.lck ]; then
   (hiveserver2) & (schematool -dbType derby -initSchema --verbose)
else
   hiveserver2
fi
