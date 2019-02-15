docker run --name tpc --volume /$(pwd)/output:/TPC-DS/v2.10.1rc3/output --entrypoint /TPC-DS/v2.10.1rc3/tools/dsdgen tpcds:dev -scale 1 -dir ../output/1GB  -terminate n   

