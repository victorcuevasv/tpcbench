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

if [ $# -lt 2 ]; then
    echo "${yel}Usage: bash merge_files.sh <input directory> <output file>${end}"
    exit 0
fi 

printf "\n\n%s\n\n" "${mag}Merging the files.${end}"

for f in "$1"/*
do
	if [ -f "$f" ]; then
		echo "Adding: $f"
  		cat "$f" >> $2
		printf "\n\n" >> $2
  	fi
done


