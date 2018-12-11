#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Restore the postgres database.
printf "\n\n%s\n\n" "${blu}Backing up postgres database.${end}"
docker exec -i postgrescontainer psql -U root -d metastore < dump_metastore.sql

