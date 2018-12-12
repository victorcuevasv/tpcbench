#!/bin/bash

docker exec -ti  hiveclientcontainer  /bin/bash -c "/opt/presto --server hiveservercontainer:8080 --catalog hive --schema default"     

