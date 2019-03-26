#!/bin/bash

docker exec -it namenodecontainer bash -c "beeline -u jdbc:hive2://localhost:10000/default"

