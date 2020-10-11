#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 number of steps to process

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash processLogsSteps.sh <n steps> <experiment name>.${end}"
    exit 0
fi

#$1 file
#$2 step number
#$3 experiment name
processStep() {
	#Separate the added files from the removed files.
	grep '{"add"' "$f" > added.txt
	grep '{"remove"' "$f" > removed.txt
	 
	while read -r line || [[ -n "$line" ]]; do
		#To convert the stats element text to json, remove with tr the \ character
		#and then the start and end quotes.    
		records=$(echo $line | jq '.add.stats' | tr -d '\\')
	    records=${records:1}
	    records=${records%?}
	    records=$(echo $records | jq -r '.numRecords')
	    path=$(echo $line | jq -r '.add.path')
		partition=$(echo $path | cut -c17-23)
		size=$(echo $line | jq -r '.add.size')
		printf "$3|$2|add|${path}|${records}|${size}|${partition}\n" >> $DIR/log.csv 
	done < added.txt
	rm added.txt
	
	while read -r line || [[ -n "$line" ]]; do 
		path=$(echo $line | jq -r '.remove.path')
		partition=$(echo $path | cut -c17-23)
		printf "$3|$2|remove|${path}|||${partition}\n" >> $DIR/log.csv
	done < removed.txt
	rm removed.txt
}

printf "experiment|step|operation|path|records|size|partition\n" >> $DIR/log.csv
i=0
for f in $DIR/*.json ; do 
   printf "Processing %s.\n" $f
   processStep $f $i $2
   if [ $i -eq $1 ]; then
   	break
   fi
   i=$((i+1))
done






