#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Create .dat files to populate the database. The scale factor is passed as an argument.
#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "Usage bash createDataFiles.sh <scale factor>."
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Generating the data files.${end}"
bash dqgen/generateData.sh $1

printf "\n\n%s\n\n" "${mag}Moving the generated files to subdirectories.${end}"
bash datFiles.sh $1

