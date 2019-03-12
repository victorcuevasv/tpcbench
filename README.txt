
To prepare the application, SQL files, and data follow the next steps.

1) To build the containers and create the required directories use the command:

bash buildAll.sh

2) To generate the SQL files (create table statement files and queries) use the command:

bash createSQLFiles.sh

3) To generate the data (.dat files) use the command (note the scale factor parameter):

bash createData.sh <scale factor>



To execute the benchmark follow the steps below.

A) For Presto

A.1) Start the infrastructure with the following command:

docker-compose -f docker-composeExternalMetastore.yml up

A.2) In case the database has not been created from the .dat files, use the command:

bash runclient_createdb.sh <scale factor>

If the database was created previously and was saved to the warehousevol directory,
use the following command to copy it to hdfs

bash copy_db_to_hdfs.sh

In turn, once created the database can be saved in the warehouse volume with the command:

bash copy_db_from_hdfs.sh

A.3) To execute the power test (execute each query once in succession) use the command:

bash runclient_executequeries.sh

A.4) To shutdown the infrastructure deployment use the command:

docker-compose -f docker-composeExternalMetastore.yml down



