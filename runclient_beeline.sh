#!/bin/bash

docker exec -ti  namenodecontainer  /bin/bash -c \
	"beeline -u jdbc:hive2://namenodecontainer:10000/default -n hive -p ''"     

