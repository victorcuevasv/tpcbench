#!/bin/bash

#Execute the runclient_createdb.sh script of the parent directory.
#If the multinode docker-compose setup is running, the database will be created in this setup.

#Create and populate the database from the .dat files. The scale factor is passed as an argument
#and used to identify the folder that holds the data.
#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "Usage: bash runclient_createdb.sh <scale factor>."
    exit 0
fi

#Pass the first argument to the script in the parent directory.
bash ../runclient_createdb.sh $1 namenodecontainer


