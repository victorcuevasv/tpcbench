#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 Name of the security group to be created.

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash create_security_group_EC2.sh <name of security group>${end}"
    exit 0
fi

#Create a security group for a virtual machine to run the benchmark.

aws ec2 create-security-group --group-name $1 \
--description "Security group for the virtual machine running the benchmark."

aws ec2 authorize-security-group-ingress \
--group-name $1 --protocol tcp --port 22 --cidr 0.0.0.0/0

