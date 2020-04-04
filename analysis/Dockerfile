FROM rocker/rstudio

ARG AWS_ACCESS_KEY=
ARG AWS_SECRET_KEY=

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    libz-dev \
    libxml2-dev

RUN install2.r haven xml2 rio ggplot2 dplyr

COPY conf/install_packages.R /install_packages.R

RUN Rscript /install_packages.R

RUN apt-get install build-essential git libfuse-dev libcurl4-openssl-dev libxml2-dev mime-support automake libtool -y
RUN apt-get install pkg-config libssl-dev -y
RUN git clone https://github.com/s3fs-fuse/s3fs-fuse
RUN cd s3fs-fuse/ && ./autogen.sh && ./configure --prefix=/usr --with-openssl && make && make install
RUN echo $AWS_ACCESS_KEY:$AWS_SECRET_KEY > ~/.passwd-s3fs && chmod 600 ~/.passwd-s3fs

