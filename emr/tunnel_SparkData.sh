#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#PARAMETERS.
#
#$1 Master node Public DNS name.
#$2 Master node Private DNS name.

if [ $# -lt 2 ]; then
    echo "${yel}Usage bash tunnel_Spark.sh <Public DNS name> <Private DNS name>.${end}"
    exit 0
fi

#Tunnel the ports for Spark web GUIs.
printf "\n\n%s\n\n" "${cyn}Tunneling the ports for Spark data node (4042).${end}"

ssh -i id_rsa -l ec2-user -N -L 4042:localhost:4042 $1 & #Application logs


#The trap will kill the background processed started above when the script
#execution is terminated (ctrl+c).
trap 'jobs -p | xargs kill' EXIT

#Necessary to avoid immediate script termination.
sleep infinity


