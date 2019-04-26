FROM ubuntujava:dev

ARG UNAME=anyuser
ARG UID=1000
ARG GID=100

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

#Install make
RUN apt-get update && apt-get install -y \
	g++ \
    make \
    wget \
    libboost-all-dev \
    libssl-dev

RUN wget -q -P /opt http://archive.apache.org/dist/thrift/0.12.0/thrift-0.12.0.tar.gz && \
    tar xzf /opt/thrift-0.12.0.tar.gz -C /opt && \
    rm -f /opt/thrift-0.12.0.tar.gz

ARG APACHE_MIRROR=archive.apache.org
ARG PRESTO_MIRROR=repo1.maven.org
ARG MAVEN_VERSION=3.6.0

#Download and decompress Maven.

RUN wget -q -P /opt http://$APACHE_MIRROR/dist/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.tar.gz && \
    tar xzf /opt/apache-maven-$MAVEN_VERSION-bin.tar.gz -C /opt && \
    rm -f /opt/apache-maven-$MAVEN_VERSION-bin.tar.gz

ENV PATH="${PATH}:/opt/apache-maven-$MAVEN_VERSION/bin"

#Compile the project.
WORKDIR /opt/thrift-0.12.0/

RUN chmod +x configure

RUN ./configure --disable-gen-erl --disable-gen-hs --without-ruby --without-haskell --without-erlang --without-php --without-nodejs     

RUN make install

#Copy the parquet-mr code.
COPY parquet-mr /opt/parquet-mr

#Compile the project.
WORKDIR /opt/parquet-mr/

RUN LC_ALL=C mvn clean install

#Compile the project.
WORKDIR /opt/parquet-mr/parquet-tools

RUN mvn clean package -Plocal

USER $UNAME

CMD ["bash"]



