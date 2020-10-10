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

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash processLogsSteps.sh <n steps>.${end}"
    exit 0
fi

i=1
for f in $DIR/*.json ; do 
   mkdir $i
   mv $DIR/$f $DIR/$i.json
   processStep $i
   i=$((i+1))
done

#$1 step number
processStep() {
	#Separate the added files from the removed files.
	grep '{"add"' "$DIR/$1.json" > added.txt
	grep '{"remove"' "$DIR/$1.json" > removed.txt
	
	printf "path|records|size|partition\n" >> $DIR/$1/added.csv 
	while read -r line || [[ -n "$line" ]]; do
		#To convert the stats element text to json, remove with sed the \ character, then the first character, and finally the last character.
		records=$(echo $line | jq '.add.stats'  |  sed 's/\\//g' | sed '1s/^.//' | sed '$ s/.$//' | jq '.numRecords')
	    path=$(echo $line | jq '.add.path' | sed '1s/^.//' | sed '$ s/.$//')
		partition=$(echo $path | cut -c17-23)
		size=$(echo $line | jq '.add.size' | sed '1s/^.//' | sed '$ s/.$//')
		printf "${path},${records},${size},${partition}\n" >> $DIR/$1/added.csv 
	done < added.txt
	
	printf "path|records|size|partition\n" >> $DIR/$1/removed.csv 
	while read -r line || [[ -n "$line" ]]; do 
		path=$(echo $line | jq '.remove.path' | sed '1s/^.//' | sed '$ s/.$//')
		partition=$(echo $path | cut -c17-23)
		printf "${path}|||${partition}\n" >> removedRecords.txt
	done < removed.txt
}




