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

#Create the datavol directory if it does not exist.

if [ ! -d $DIR/datavol ]; then
   mkdir $DIR/datavol
fi

#Create the hivevol directory if it does not exist.

if [ ! -d $DIR/hivevol ]; then
   mkdir $DIR/hivevol
fi

#Create the metastorevol directory if it does not exist.

if [ ! -d $DIR/metastorevol ]; then
   mkdir $DIR/metastorevol
fi
   
#Create the warehousevol directory if it does not exist.

if [ ! -d $DIR/warehousevol ]; then
   mkdir $DIR/warehousevol
fi

#Create the ivyrootvol directory if it does not exist.

if [ ! -d ivyrootvol ]; then
   mkdir ivyrootvol
fi

#Create the ivyrootvol directory if it does not exist.

if [ ! -d ivyuservol ]; then
   mkdir ivyuservol
fi



