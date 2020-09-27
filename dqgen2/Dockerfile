FROM ubuntujava:dev

ARG UNAME=anyuser
ARG UID=1000
ARG GID=100
ARG TPCDS_VERSION=v2.13.0rc1

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

#Install make
RUN apt-get update && apt-get install -y \
	gcc \
    make

#Copy the TPC-DS code.
COPY $TPCDS_VERSION /TPC-DS/$TPCDS_VERSION

#Copy the scripts used to generate the queries.
COPY containerScripts/* /TPC-DS/$TPCDS_VERSION/tools/

#Make the scripts executable.
RUN chmod +x /TPC-DS/$TPCDS_VERSION/tools/*.sh

#Copy the modified netezza and db2 templates.
#Template used to generate queries.
COPY netezza.tpl /TPC-DS/$TPCDS_VERSION/query_templates
COPY db2.tpl /TPC-DS/$TPCDS_VERSION/query_templates
#Templates used to generate query stream files to extract
#the query sequences.
COPY netezzaLong.tpl /TPC-DS/$TPCDS_VERSION
COPY db2Long.tpl /TPC-DS/$TPCDS_VERSION

#Create the output directory.
RUN mkdir /TPC-DS/$TPCDS_VERSION/output 

#Compile the project.
WORKDIR /TPC-DS/$TPCDS_VERSION/tools
RUN make

#Change the owner of the TPC-DS folder recursively to 
#the user and group id supplied.
RUN chown -R $UID:$GID /TPC-DS

USER $UNAME

CMD ["bash"]


