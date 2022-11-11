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

if [ $# -lt 1 ]; then
    echo "${yel}Usage bash tunnelGanglia.sh <Public DNS name>.${end}"
    exit 0
fi

#Tunnel the port for the Ganglia GUI.
printf "\n\n%s\n\n" "${cyn}Ganglia GUI: http://localhost:8900/ganglia${end}"

ssh -i /home/victor/dev/devtest_qbeast_nvirginia_keypair.pem -l hadoop -N -L 8900:localhost:80 $1 & #Ganglia

#The trap will kill the background processed started above when the script
#execution is terminated (ctrl+c).
trap 'jobs -p | xargs kill' EXIT

#Necessary to avoid immediate script termination.
sleep infinity


