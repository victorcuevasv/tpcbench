#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Generate the SQL files in the parent directory, overwriting the existing files, if any.
printf "${red}Generating SQL files overwriting the files in the parent directory.${end}\n"
read -p "${red}Are you sure? (y/n)${end} " -n 2 -r
echo    ""
if [[ $REPLY =~ ^[Yy]$ ]]
then
    bash ../createSQLFiles.sh
    echo "Files generated."
fi

cp -r ../datavol/tables datavol
cp -r ../datavol/QueriesSpark datavol
cp -r ../datavol/QueriesPresto datavol
echo "Files copied."

