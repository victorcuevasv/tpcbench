#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Generate the Presto coordinator node configuration files.

#First generate a new node.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_coordinator/node.properties.base > $DIR/presto_etc_coordinator/etc/node.properties

#Add to the previously generated file the node.id line.
echo "node.id="$(cat $DIR/presto_etc_coordinator/node.id)"" >> $DIR/presto_etc_coordinator/etc/node.properties

#Then generate the config.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_coordinator/config.properties.base > $DIR/presto_etc_coordinator/etc/config.properties

#Add to the previously generated file the discovery.uri line.
echo "discovery.uri="$(cat $DIR/presto_etc_coordinator/discovery.uri)"" >> $DIR/presto_etc_coordinator/etc/config.properties   

mkdir $DIR/presto_etc_coordinator/etc/catalog

#Finally generate a new catalog/hive.properties file.
cat $DIR/presto_etc_coordinator/hive.properties.base > $DIR/presto_etc_coordinator/etc/catalog/hive.properties

#Add to the previously generated file the hive.metastore.uri line.
echo "hive.metastore.uri="$(cat $DIR/presto_etc_coordinator/hive.metastore.uri)"" >> $DIR/presto_etc_coordinator/etc/catalog/hive.properties   

#Use the default mirrors.
#docker build -t prestohiveservermult:dev $DIR -f $DIR/DockerfilePresto 

#Use the nginx mirror server.
docker build --network="host" -t prestohiveservermult:dev $DIR -f $DIR/DockerfilePresto \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:8888 \
	--build-arg PRESTO_MIRROR=localhost:8888


