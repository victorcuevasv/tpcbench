#!/bin/bash

#$1 scale factor that determines the directory in which the files are stored.

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#For each .dat file, create a directory with that name and move the .dat file to
#that directory.

for f in $DIR/vols/hive/$1GB/*.dat ; do 
   fileName=$(basename "$f" .dat) #This invocation removes the .dat extension.
   mkdir $DIR/vols/hive/$1GB/$fileName
   mv $DIR/vols/hive/$1GB/"$fileName".dat $DIR/vols/hive/$1GB/$fileName
done


