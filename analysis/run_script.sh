#!/bin/bash

#Get the user id of the user executing this script.
USER_ID=$(id -u)
#Get the user id of the user executing this script.
GROUP_ID=$(id -g)

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"


docker run --rm --user $USER_ID:$GROUP_ID -v $DIR/Documents:/home/rstudio/Documents  \
--entrypoint Rscript rstudio:dev --version   

