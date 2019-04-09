#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash copy_datfiles_to_dbfs.sh <scale factor>.${end}"
    exit 0
fi

#Copy the .dat files inside each subdirectory.
for d in $DIR/../vols/hive/$1GB/* ; do 
   dirName=$(basename "$d") #This invocation removes the front path.
   dbfs mkdirs dbfs:/temporal/$dirName
   dbfs cp $DIR/../vols/hive/$1GB/$dirName/$dirName.dat dbfs:/temporal/$dirName/$dirName.dat
   echo "${cyn}Copied $dirName.${end}"
done



