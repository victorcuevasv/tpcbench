FROM ubuntujava:dev

#Install make
RUN apt-get update && apt-get install -y \
	gcc \
    make

#Copy the TPC-DS code.
COPY v2.10.1rc3 /TPC-DS/v2.10.1rc3

#Copy the scripts used to generate the queries.
COPY containerScripts/* /TPC-DS/v2.10.1rc3/tools/

#Copy the modified netezza template.
COPY netezza.tpl /TPC-DS/v2.10.1rc3/query_templates

#Create the output directory.
RUN mkdir /TPC-DS/v2.10.1rc3/output 

#Compile the project.
WORKDIR /TPC-DS/v2.10.1rc3/tools
RUN make

CMD ["bash"]



