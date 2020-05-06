#!/bin/bash

#Create a security group for a virtual machine to run the benchmark.

GroupName="benchmark-sg"

aws ec2 create-security-group --group-name $GroupName \
--description "Security group for the virtual machine running the benchmark."

aws ec2 authorize-security-group-ingress \
--group-name $GroupName --protocol tcp --port 22 --cidr 0.0.0.0/0

