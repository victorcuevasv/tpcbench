#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Compile the Presto/Hive JDBC client project.
printf "\n\n%s\n\n" "${blu}Compiling the Presto/Hive JDBC client project.${end}"
bash $DIR/client/compile.sh $USER_ID $GROUP_ID

#Compile the Spark (spark-submit) client project.
printf "\n\n%s\n\n" "${blu}Compiling the Spark client project.${end}"
bash $DIR/client/compileSpark.sh $USER_ID $GROUP_ID

#Compile the Spark (spark-submit) client project.
printf "\n\n%s\n\n" "${blu}Compiling the Spark JDBC client project.${end}"
bash $DIR/client/compileSparkJDBC.sh $USER_ID $GROUP_ID


