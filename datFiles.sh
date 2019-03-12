#!/bin/bash

#For each .dat file, create a directory with that name and move the .dat file to
#that directory.

for f in ./hivevol/$1GB/*.dat ; do 
   fileName=$(basename "$f" .dat) #This invocation removes the .dat extension.
   mkdir ./hivevol/$1GB/$fileName
   mv ./hivevol/$1GB/"$fileName".dat ./hivevol/$1GB/$fileName
done


