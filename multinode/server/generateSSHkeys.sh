#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Generating ssh keys.
printf "\n\n%s\n\n" "${blu}Generating ssh keys.${end}"
ssh-keygen -t rsa -P '' -f ssh/id_rsa

