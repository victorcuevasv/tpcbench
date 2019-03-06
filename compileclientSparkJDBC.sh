#!/bin/bash

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#Build the client project.
printf "\n\n%s\n\n" "${blu}Compiling the client project.${end}"
bash client/compileSparkJDBC.sh

