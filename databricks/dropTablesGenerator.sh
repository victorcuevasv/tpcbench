#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Generate drop table statements for all tables.
for d in $DIR/../vols/hive/1GB/* ; do 
   dirName=$(basename "$d") #This invocation removes the front path.
   printf "spark.sql(\"drop table $dirName\").collect().foreach(println)\n"
   printf "spark.sql(\"drop table $dirName"
   printf "_ext\").collect().foreach(println)\n"
done



