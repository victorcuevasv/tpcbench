#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

exitCode=0

#First generate the Presto worker node configuration files.
#The node.properties files have to be generated later since the node.id values have to be different.

#Then generate the config.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_worker/config.properties.base > $DIR/presto_etc_worker/etc/config.properties

#Add to the previously generated file the discovery.uri line.
echo "discovery.uri="$(cat $DIR/presto_etc_worker/discovery.uri)"" >> $DIR/presto_etc_worker/etc/config.properties    

mkdir $DIR/presto_etc_worker/etc/catalog

#Finally generate a new catalog/hive.properties file.
cat $DIR/presto_etc_worker/hive.properties.base > $DIR/presto_etc_worker/etc/catalog/hive.properties

#Add to the previously generated file the hive.metastore.uri line.
echo "hive.metastore.uri="$(cat $DIR/presto_etc_worker/hive.metastore.uri)"" >> $DIR/presto_etc_worker/etc/catalog/hive.properties    


#Generate one node.id for each worker to complete the node.properties file.


#Generate a new node.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_worker/node.properties.base > $DIR/presto_etc_worker/etc/node.properties

#Add to the previously generated file the node.id line using the first line in the node.id.list file.
echo "node.id="$(sed '1q;d' $DIR/presto_etc_worker/node.id.list)"" >> $DIR/presto_etc_worker/etc/node.properties

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" --force-rm -t prestoslave1mult:dev $DIR -f $DIR/DockerfileSlavePresto \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:443 \
	--build-arg PRESTO_MIRROR=localhost:443
else
	docker build --force-rm -t prestoslave1mult:dev $DIR -f $DIR/DockerfileSlavePresto
fi

if [[ $? -eq 1 ]]; then
	exitCode=1;
fi

#Generate a new node.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_worker/node.properties.base > $DIR/presto_etc_worker/etc/node.properties

#Add to the previously generated file the node.id line using the second line in the node.id.list file.
echo "node.id="$(sed '2q;d' $DIR/presto_etc_worker/node.id.list)"" >> $DIR/presto_etc_worker/etc/node.properties

#Check if the local mirror can be used.
nc -z localhost 8888 && nc -z localhost 443
mirror=$?
if [[ $mirror -eq 0 ]]; then
	docker build --network="host" --force-rm -t prestoslave2mult:dev $DIR -f $DIR/DockerfileSlavePresto \
	--build-arg APACHE_MIRROR=localhost:8888 \
	--build-arg POSTGRES_DRIVER_MIRROR=localhost:443 \
	--build-arg PRESTO_MIRROR=localhost:443
else
	docker build --force-rm -t prestoslave2mult:dev $DIR -f $DIR/DockerfileSlavePresto
fi

if [[ $? -ne 0 ]]; then
	exitCode=1;
fi

if [[ $exitCode -ne 0 ]]; then
	exit 1
else
	exit 0
fi




