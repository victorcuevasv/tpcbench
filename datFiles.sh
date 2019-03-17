#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#For each .dat file, create a directory with that name and move the .dat file to
#that directory.

for f in $DIR/hivevol/$1GB/*.dat ; do 
   fileName=$(basename "$f" .dat) #This invocation removes the .dat extension.
   mkdir $DIR/hivevol/$1GB/$fileName
   mv $DIR/hivevol/$1GB/"$fileName".dat $DIR/hivevol/$1GB/$fileName
done


