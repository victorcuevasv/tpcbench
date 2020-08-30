#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#$1 filename

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash processLog.sh <filename>.${end}"
    exit 0
fi

#Separate the added files from the removed files.

grep '{"add"' "$1" > added.txt

grep '{"remove"' "$1" > removed.txt


rm -f addedRecords.txt

while read -r line || [[ -n "$line" ]]; do
	#To convert the stats element text to json, remove with sed the \ character, then the first character, and finally the last character.
	records=$(echo $line | jq '.add.stats'  |  sed 's/\\//g' | sed '1s/^.//' | sed '$ s/.$//' | jq '.numRecords')
    path=$(echo $line | jq '.add.path' | sed '1s/^.//' | sed '$ s/.$//')
	partition=$(echo $path | cut -c17-23)
	printf "${path},${records},${partition}\n" >> addedRecords.txt 
done < added.txt

rm -f removedRecords.txt

while read -r line || [[ -n "$line" ]]; do 
	path=$(echo $line | jq '.remove.path' | sed '1s/^.//' | sed '$ s/.$//')
	partition=$(echo $path | cut -c17-23)
	printf "${path},${partition}\n" >> removedRecords.txt
done < removed.txt



