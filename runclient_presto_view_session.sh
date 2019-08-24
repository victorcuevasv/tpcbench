#!/bin/bash

#Variables for console output with colors.

red=$'\e[1;31m'
grn=$'\e[1;32m'
yel=$'\e[1;33m'
blu=$'\e[1;34m'
mag=$'\e[1;35m'
cyn=$'\e[1;36m'
end=$'\e[0m'

#$1 Receives the database (schema) name as a parameter.

if [ $# -lt 1 ]; then
    echo "${yel}Usage: bash runclient_presto_view_session.sh <dbname>.${end}"
    exit 0
fi

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"export PRESTO_PAGER= ; /opt/presto --server namenodecontainer:8080 --catalog hive --schema $1 --execute 'SHOW SESSION;'"     

