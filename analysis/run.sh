#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

docker run -e PASSWORD=1234 --rm -p 8787:8787 \
-v $DIR/Documents:/home/rstudio/Documents \
-v $DIR/Output:/home/rstudio/Output  \
-v $HOME/tpcds-results-test:/home/rstudio/tpcds-results-test  \
rstudio:dev

