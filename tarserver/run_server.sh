#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

printf "\n\n%s\n\n" "${yel}Running the nginx server.${end}"
docker run --name nginx-tarserver-container -d -p 8888:80 nginx-tarserver:dev

