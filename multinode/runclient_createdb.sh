#!/bin/bash

#Execute the runclient_createdb.sh script of the parent directory.
#If the multinode docker-compose setup is running, the database will be created in this setup.

#Pass the first argument to the script in the parent directory.
bash ../runclient_createdb.sh $1 namenodecontainer


