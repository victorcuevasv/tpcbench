#!/bin/bash

docker exec -ti  mastercontainer  /bin/bash -c "/opt/spark-2.4.0-bin-hadoop2.7/bin/run-example --master spark://mastercontainer:7077 SparkPi 10"       

#Run locally.

#docker exec -ti  sparkhiveclientcontainer  /bin/bash -c "/opt/spark-2.4.0-bin-hadoop2.7/bin/run-example SparkPi 10"

