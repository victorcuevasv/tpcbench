#!/bin/bash

#Check if the metastore exists, if not, then create it.
#The commands are run in a subshell within the USER_NAME_DC home directory,
#this in order to avoid permission conflicts.
#Note that the -E parameter is passed to sudo to run the schematool in order to 
#preserve the configuration variables like HIVE_HOME.

if [ ! -f /home/$USER_NAME_DC/metastore/metastore_db/db.lck ]; then
   (cd /home/$USER_NAME_DC ; \
   sudo -E -u $USER_NAME_DC $HIVE_HOME/bin/schematool -dbType derby -initSchema --verbose )
fi

#The hive server has to be running to create the database and to run  beeline.
#On the other hand, it should not be running to use spark.
if [[ $RUN_HIVE_SERVER -eq 1 ]]; then
   (cd /home/$USER_NAME_DC ; hiveserver2)
else
	sleep infinity
fi

