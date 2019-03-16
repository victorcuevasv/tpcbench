#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Generate the unused Netezza queries.
printf "\n\n%s\n\n" "${cyn}Executing the buildAll.sh script.${end}"
bash buildAll.sh

#Set permissions for data volume and client project.
chmod -R 777 metastorevol
#sudo chmod -R 777 metastorevol
#chmod -R 777 client/project
#sudo chmod -R 777 client/project
#chmod -R 777 datavol
#sudo chmod -R 777 datavol
#chmod -R 777 warehousevol
#sudo chmod -R 777 warehousevol
#chmod -R 777 hivevol
#sudo chmod -R 777 hivevol


