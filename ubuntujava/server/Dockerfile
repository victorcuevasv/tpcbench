FROM ubuntujava:dev

USER root

#Install ssh and python.

RUN apt-get update && apt-get install -y \
    ssh \
    python2.7 \
    python-pip \
    python-dev

#Generate a key and add it to the authorized set.

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    
#The configuration file will enable passwordless login to nodes
#and without confirmation prompt.
    
COPY ssh_config /root/.ssh/config

#Download and decompress Hadoop

RUN wget -q -P /opt http://apache.rediris.es/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz && \
    tar xzf /opt/hadoop-2.7.7.tar.gz -C /opt && \
    rm -f /opt/hadoop-2.7.7.tar.gz

#Add the Hadoop installation directory variable and update the path.

ENV HADOOP_HOME /opt/hadoop-2.7.7
ENV PATH="${PATH}:/opt/hadoop-2.7.7/bin:/opt/hadoop-2.7.7/sbin"

#Copy the Hadoop configuration files.

COPY etc_hadoop/*.* /opt/hadoop-2.7.7/etc/hadoop/

#Download and decompress Hive.

RUN wget -q -P /opt http://apache.rediris.es/hive/hive-2.3.4/apache-hive-2.3.4-bin.tar.gz && \
    tar xzf /opt/apache-hive-2.3.4-bin.tar.gz -C /opt && \
    rm -f /opt/apache-hive-2.3.4-bin.tar.gz
    
#Add the Hive installation directory variable and update the path.

ENV HIVE_HOME /opt/apache-hive-2.3.4-bin
ENV PATH="${PATH}:/opt/apache-hive-2.3.4-bin/bin"

#Copy the Hive configuration file which will override selected defaults.

COPY hive-site.xml /opt/apache-hive-2.3.4-bin/conf/

#Copy the initialization script for Hadoop and Hive and make it executable.

COPY start.sh /start.sh

RUN chmod +x /start.sh 

CMD ["bash"]

ENTRYPOINT [ "bash", "-c", "/start.sh" ]

