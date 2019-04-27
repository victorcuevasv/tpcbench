#!/bin/bash

PRESTO_CONFIG_SCRIPT=$(cat <<EOF
#while [ ! -f /var/run/presto/presto-server.pid ]; do
while [ ! -f /etc/presto/conf/catalog/hive.properties ]; do
	sleep 1
done
#DO stuff you need to do after presto is up and running
echo "Configuring Presto." > /tmp/outPrestoHiveConfig.txt
echo "hive.allow-drop-table=true" >> /etc/presto/conf/catalog/hive.properties 
echo "hive.compression-codec = SNAPPY" >> /etc/presto/conf/catalog/hive.properties
cat /etc/presto/conf/catalog/hive.properties >> /tmp/outPrestoHiveConfig.txt
sudo stop presto-server
sleep 15
sudo start presto-server
#End Presto stuff
exit 0
EOF
)
echo "${PRESTO_CONFIG_SCRIPT}" | tee -a /tmp/presto_config.sh
chmod u+x /tmp/presto_config.sh
sudo /tmp/presto_config.sh &
exit 0


