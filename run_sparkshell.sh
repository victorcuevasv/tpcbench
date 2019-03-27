#!/bin/bash

#Get the user name of the user executing this script.
USER_NAME=$(whoami)
#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Specify explicitly the metastore uri.

#docker exec --user $USER_ID:$GROUP_ID -it namenodecontainer bash -c \
#"spark-shell --driver-java-options -Dhive.metastore.uris=thrift://localhost:9083 \
#--packages org.apache.zookeeper:zookeeper:3.4.6"    


#Use the metastore uri specified in the hive-site.xml file inside the spark conf directory.

docker exec --user $USER_ID:$GROUP_ID -it namenodecontainer bash -c \
"spark-shell --packages org.apache.zookeeper:zookeeper:3.4.6"

