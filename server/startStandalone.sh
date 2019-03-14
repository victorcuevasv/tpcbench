#!/bin/bash
if [ ! -f /metastore/metastore_db/db.lck ]; then
   (hiveserver2) & (schematool -dbType derby -initSchema --verbose; sleep infinity)
else
   hiveserver2
fi
