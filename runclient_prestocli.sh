#!/bin/bash

docker exec -ti  clientbuildercontainer  /bin/bash -c \
	"/opt/presto --server mastercontainer:8080 --catalog hive --schema default"     

