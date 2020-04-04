#!/bin/bash


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"


docker run --rm --privileged \
-v $DIR/Documents:/home/rstudio/Documents  \
-v $DIR/Output:/home/rstudio/Output  \
--entrypoint /bin/bash rstudio:dev -c \
"mkdir -p /home/rstudio/s3buckets/tpcds-results-test; s3fs tpcds-results-test /home/rstudio/s3buckets/tpcds-results-test; Rscript /home/rstudio/Documents/stacked_chart.R"        
 
