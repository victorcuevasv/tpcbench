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
printf "\n\n%s\n\n" "${cyn}Tunneling the ports for Presto (8088, 19888, 8080).${end}"

ssh -l vcuevas -N -L 8088:localhost:8088 bscdc07 & 
ssh -l vcuevas -N -L 19888:localhost:19888 bscdc07 & 
ssh -l vcuevas -N -L 8080:localhost:8080 bscdc07 &

#The trap will kill the background processed started above when the script
#execution is terminated (ctrl+c).
trap 'jobs -p | xargs kill' EXIT

#Necessary to avoid immediate script termination.
sleep infinity
