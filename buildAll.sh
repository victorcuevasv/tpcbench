#!/bin/bash

#Create the hivevol directory if it does not exist.

if [ ! -d hivevol ]; then
   mkdir hivevol
fi

#Create the metastorevol directory if it does not exist.

if [ ! -d metastorevol ]; then
   mkdir metastorevol
fi
   
#Create the warehousevol directory if it does not exist.

if [ ! -d warehousevol ]; then
   mkdir warehousevol
fi

#Create the datavol directory and its subdirectories if it does not exist.

if [ ! -d datavol ]; then
   mkdir datavol
   mkdir datavol/tables
   mkdir datavol/tablestext
   mkdir datavol/tablesparquet
   mkdir datavol/results
   mkdir datavol/logs
   #Copy the queries file into the datavol directory.
   cp benchmark/tpcds.sql datavol
fi

#Build base Ubuntu with java image.
bash ubuntujava/build.sh

#Build the server.
bash server/build.sh

#Build the client project.
bash client/build.sh

