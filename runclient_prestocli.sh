#!/bin/bash

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"/opt/presto --server namenodecontainer:8080 --catalog hive --schema default"     

