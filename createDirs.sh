#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

printf "\n\n%s\n\n" "${mag}Creating volume directories.${end}"

#Create the vols directory for all volumes if it does not exist.

if [ ! -d $DIR/vols ]; then
   mkdir $DIR/vols
fi

#Create the datavol directory if it does not exist.

if [ ! -d $DIR/vols/data ]; then
   mkdir $DIR/vols/data
fi

#Create the hivevol directory if it does not exist.

if [ ! -d $DIR/vols/hive ]; then
   mkdir $DIR/vols/hive
fi

#Create the metastorevol directory if it does not exist.

if [ ! -d $DIR/vols/metastore ]; then
   mkdir $DIR/vols/metastore
fi
   
#Create the warehousevol directory if it does not exist.

if [ ! -d $DIR/vols/warehouse ]; then
   mkdir $DIR/vols/warehouse
fi

#Create the ivyrootvol directory if it does not exist.

if [ ! -d $DIR/vols/ivyroot ]; then
   mkdir $DIR/vols/ivyroot
fi

#Create the ivyrootvol directory if it does not exist.

if [ ! -d $DIR/vols/ivyuser ]; then
   mkdir $DIR/vols/ivyuser
fi



