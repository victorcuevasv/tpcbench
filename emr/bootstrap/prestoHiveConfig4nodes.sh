#!/bin/bash

#Use EOF within quotes in the here doc due to the curly braces in the sed commands.

PRESTO_CONFIG_SCRIPT=$(cat <<'EOF'
#while [ ! -f /var/run/presto/presto-server.pid ]; do
while [ ! -f /etc/presto/conf/catalog/hive.properties ]; do
	sleep 1
done
#DO stuff you need to do after presto is up and running
echo "Configuring Presto Hive." > /tmp/outPrestoHiveConfig.txt
echo "hive.allow-drop-table=true" >> /etc/presto/conf/catalog/hive.properties 
echo "hive.compression-codec = SNAPPY" >> /etc/presto/conf/catalog/hive.properties
cat /etc/presto/conf/catalog/hive.properties >> /tmp/outPrestoHiveConfig.txt
while [ ! -f /etc/presto/conf/config.properties ]; do
	sleep 1
done
while ! grep -q query.max-memory /etc/presto/conf/config.properties ; do
	sleep 1
done
sleep 15
replaceString1=120GB
sed -i "s/\(query\.max-memory=\).*\$/\1${replaceString1}/" /etc/presto/conf/config.properties
replaceString2=45752623907B
sed -i "s/\(query\.max-memory-per-node=\).*\$/\1${replaceString2}/" /etc/presto/conf/config.properties
replaceString3=50903148689B
sed -i "s/\(query\.max-total-memory-per-node=\).*\$/\1${replaceString3}/" /etc/presto/conf/config.properties
echo "Configuring Presto." > /tmp/outPrestoConfig.txt
cat /etc/presto/conf/config.properties >> /tmp/outPrestoConfig.txt
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


