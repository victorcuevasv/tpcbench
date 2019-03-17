#!/bin/bash

#Execute the createDataFiles.sh script of the parent directory.
#The .dat files generated will be inside a folder of the parent directory.

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

#Pass the scale factor first parameter to the script in the parent directory,
#along with the user and group id.s
bash ../dqgen/generateData.sh $1 $USER_ID $GROUP_ID



