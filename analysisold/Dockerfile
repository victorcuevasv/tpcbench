FROM rocker/rstudio

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libz-dev \
    libxml2-dev

RUN install2.r haven xml2 rio ggplot2 dplyr gridExtra

COPY conf/install_packages.R /install_packages.R

RUN Rscript /install_packages.R


