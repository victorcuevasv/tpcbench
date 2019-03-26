#!/bin/bash      

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

if [ $# -lt 1 ]; then
    echo "Usage bash runclient_executequeriesconcurrentspark.sh <number of streams>."
    exit 0
fi

#Run as a subshell inside the USER_NAME home directory because the metastore is stored there.
#Note that the master used is local.

docker exec --user $USER_ID:$GROUP_ID -ti  namenodecontainer  /bin/bash -c \
"( cd /home/$USER_NAME ; /opt/spark-2.4.0-bin-hadoop2.7/bin/spark-submit --conf spark.eventLog.enabled=true  \
--packages org.apache.logging.log4j:log4j-api:2.8.2,org.apache.logging.log4j:log4j-core:2.8.2,\
org.apache.zookeeper:zookeeper:3.4.6 \
--conf spark.local.dir=/home/$USER_NAME/tmp \
--conf spark.eventLog.dir=/home/$USER_NAME/tmp \
--class org.bsc.dcc.vcv.ExecuteQueriesConcurrentSpark \
--master local \
/project/targetspark/client-1.0-SNAPSHOT.jar \
/data results plans /project/targetspark/client-1.0-SNAPSHOT.jar spark $1 1954 )" 


                     

