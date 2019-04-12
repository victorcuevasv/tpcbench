FROM ubuntujava:dev

ARG UNAME=anyuser
ARG UID=1000
ARG GID=100

RUN groupadd -g $GID -o $UNAME
RUN useradd -m -u $UID -g $GID -o -s /bin/bash $UNAME

#Install make
RUN apt-get update && apt-get install -y \
	gcc \
    make

#Copy the TPC-DS code.
COPY v2.10.1rc3 /TPC-DS/v2.10.1rc3

#Copy the scripts used to generate the queries.
COPY containerScripts/* /TPC-DS/v2.10.1rc3/tools/

#Make the scripts executable.
RUN chmod +x /TPC-DS/v2.10.1rc3/tools/*.sh

#Copy the modified netezza template.
COPY netezza.tpl /TPC-DS/v2.10.1rc3/query_templates

#Create the output directory.
RUN mkdir /TPC-DS/v2.10.1rc3/output 

#Compile the project.
WORKDIR /TPC-DS/v2.10.1rc3/tools
RUN make

#Change the owner of the TPC-DS folder recursively to 
#the user and group id supplied.
RUN chown -R $UID:$GID /TPC-DS

USER $UNAME

CMD ["bash"]



