#!/bin/bash

#$1 scale factor that determines the directory in which the files are stored.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#For each .dat file, create a directory with that name and move the .dat file to
#that directory.

for d in $DIR/vols/hive/$1GB/* ; do 
   echo $d/
   echo $(basename "$d")
   aws s3 cp --recursive $d/ s3://benchmarking-datasets-1664540209/tpcds-data-100gb-csv-1667301078/$(basename "$d")
done


