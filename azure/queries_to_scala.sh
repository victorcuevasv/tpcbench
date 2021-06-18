#!/bin/bash   

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

printf "\n\n%s\n\n" "${mag}Generating scala files.${end}"

q1to10file = "q1to10.scala"
printf "" > $q1to10file
printf "//START 3TB queries 1 to 10" >> $q1to10file
printf "\n\n" >> $q1to10file
for i in {1..10};
do 
	printf "TPCDSQueries3TB += (\"q${i}\" -> \"\"\"\n" >> $q1to10file
	cat q${i}.sql >> $q1to10file
	printf "\"\"\")\n" >> $q1to10file
	printf "\n\n" >> $q1to10file;
done
printf "//END 3TB queries 1 to 10" >> $q1to10file
