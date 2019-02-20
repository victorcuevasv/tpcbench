#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#First generate the Presto worker node configuration files.
#The node.properties files have to be generated later since the node.id values have to be different.

#Then generate the config.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_worker/config.properties.base > $DIR/presto_etc_worker/etc/config.properties

#Add to the previously generated file the discovery.uri line.
echo "discovery.uri="$(cat $DIR/presto_etc_worker/discovery.uri)"" >> $DIR/presto_etc_worker/etc/config.properties    

#Finally generate a new catalog/hive.properties file.
cat $DIR/presto_etc_worker/hive.properties.base > $DIR/presto_etc_worker/etc/catalog/hive.properties

#Add to the previously generated file the hive.metastore.uri line.
echo "hive.metastore.uri="$(cat $DIR/presto_etc_worker/hive.metastore.uri)"" >> $DIR/presto_etc_worker/etc/catalog/hive.properties    


#Generate one node.id for each worker to complete the node.properties file.


#Generate a new node.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_worker/node.properties.base > $DIR/presto_etc_worker/etc/node.properties

#Add to the previously generated file the node.id line using the first line in the node.id.list file.
echo "node.id="$(sed '1q;d' $DIR/presto_etc_worker/node.id.list)"" >> $DIR/presto_etc_worker/etc/node.properties

docker build -t hadoopslave1:dev $DIR -f $DIR/DockerfileSlavePresto 

#Generate a new node.properties file inside the etc directory from the base file.
cat $DIR/presto_etc_worker/node.properties.base > $DIR/presto_etc_worker/etc/node.properties

#Add to the previously generated file the node.id line using the first line in the node.id.list file.
echo "node.id="$(sed '2q;d' $DIR/presto_etc_worker/node.id.list)"" >> $DIR/presto_etc_worker/etc/node.properties

docker build -t hadoopslave2:dev $DIR -f $DIR/DockerfileSlavePresto 





