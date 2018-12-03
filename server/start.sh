#!/bin/bash
if [ ! -f /metastore_db/db.lck ]; then
   (hiveserver2) & (schematool -dbType derby -initSchema --verbose)
else
   hiveserver2
fi
