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

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Create .dat files to populate the database. The scale factor is passed as an argument.
#$1 scale factor (positive integer)

if [ $# -lt 1 ]; then
    echo "Usage bash createDataFiles.sh <scale factor>."
    exit 0
fi

printf "\n\n%s\n\n" "${mag}Generating the data files.${end}"
bash $DIR/dqgen/generateData.sh $1 $USER_ID $GROUP_ID

printf "\n\n%s\n\n" "${mag}Moving the generated files to subdirectories.${end}"
bash $DIR/datFiles.sh $1

