#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Parameters.
#
#$1 master node hostname

if [ $# -lt 1 ]; then
    echo "${yel}Usage bash tunnel_Presto.sh <Public DNS name>${end}"
    exit 0
fi

#Tunnel through ssh the presto web interfaces.
printf "\n\n%s\n\n" "${cyn}Tunneling the ports for Presto (8889).${end}"
printf "\n\n%s\n\n" "${cyn}Presto GUI: http://localhost:8889${end}"
printf "\n\n%s\n\n" "${cyn}Ganglia GUI: http://localhost:8900/ganglia${end}"

ssh -i id_rsa -l hadoop -N -L 8889:localhost:8889 $1 &
ssh -i id_rsa -l hadoop -N -L 8900:localhost:80 $1 &

#The trap will kill the background processed started above when the script
#execution is terminated (ctrl+c).
trap 'jobs -p | xargs kill' EXIT

#Necessary to avoid immediate script termination.
sleep infinity
