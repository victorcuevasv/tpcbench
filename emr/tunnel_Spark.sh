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
    echo "Usage bash tunnel_Spark.sh <Public DNS name> <Private DNS name>."
    exit 0
fi

#Tunnel the ports for Spark web GUIs.
printf "\n\n%s\n\n" "${cyn}Tunneling the ports for Spark (18080, 19888, 20888).${end}"

ssh -i id_rsa -l ec2-user -N -L 18080:localhost:18080 $1 &
ssh -i id_rsa -l ec2-user -N -L 20888:$2:20888 $1  &
ssh -i id_rsa -l ec2-user -N -L 19888:$2:19888 $1  &
	
#History server web gui url.
#http://localhost:18080/
	
#Example command with explicit parameter values.
#ssh -i id_rsa -l ec2-user -N -L 18080:localhost:18080 ec2-18-237-94-60.us-west-2.compute.amazonaws.com &
#ssh -i id_rsa -l ec2-user -N -L 20888:ip-172-31-28-132.us-west-2.compute.internal:20888 \
#	ec2-18-237-94-60.us-west-2.compute.amazonaws.com  &
#ssh -i id_rsa -l ec2-user -N -L 19888:ip-172-31-28-132.us-west-2.compute.internal:19888 \
#	ec2-18-237-94-60.us-west-2.compute.amazonaws.com  &


#The trap will kill the background processed started above when the script
#execution is terminated (ctrl+c).
trap 'jobs -p | xargs kill' EXIT

#Necessary to avoid immediate script termination.
sleep infinity


