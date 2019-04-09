#!/bin/bash

#This script is destined to be located in dbfs:/databricks/scripts

echo "Waiting for the FUSE to be ready."

sleep 20

echo "Copying the Hive jars."

mkdir -p /databricks/hive_metastore_jars
cp /dbfs/hive_metastore_jar/* /databricks/hive_metastore_jars

