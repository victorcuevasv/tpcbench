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
printf "\n\n%s\n\n" "${cyn}Tunneling the ports for Presto.${end}"

ssh -l vcuevas -N -L 4040:localhost:4040 bscdc07 &
ssh -l vcuevas -N -L 4041:localhost:4041 bscdc07 &
ssh -l vcuevas -N -L 8088:localhost:8088 bscdc07 & 
ssh -l vcuevas -N -L 18080:localhost:18080 bscdc07 & 
ssh -l vcuevas -N -L 8080:localhost:8080 bscdc07 &


