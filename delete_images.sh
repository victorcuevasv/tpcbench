#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Delete dangling container images.
#docker rmi $(docker images --filter "dangling=true" -q --no-trunc)

#Delete container images with the <dev> tag, but ask the user first.
printf "${red}Deleting all container images with the <dev> tag.${end}\n"
read -p "${red}Are you sure? (y/n)${end} " -n 2 -r
echo    ""
if [[ $REPLY =~ ^[Yy]$ ]]
then
    #docker rmi $(docker images --filter=reference="*:dev" -q)
    echo "Images deleted."
fi


