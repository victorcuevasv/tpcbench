FROM ubuntujava:dev

#Install ssh

RUN apt-get update && apt-get install -y ssh

RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa && \
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    
COPY ssh_config /root/.ssh/config

#Download and decompress Hadoop

RUN wget -q -P /opt http://apache.rediris.es/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz && \
    tar xzf /opt/hadoop-2.7.7.tar.gz -C /opt && \
    rm -f /opt/hadoop-2.7.7.tar.gz

ENV HADOOP_HOME /opt/hadoop-2.7.7
ENV PATH="${PATH}:/opt/hadoop-2.7.7/bin:/opt/hadoop-2.7.7/sbin"

#Copy the Hadoop configuration files.

COPY etc_hadoop/*.* /opt/hadoop-2.7.7/etc/hadoop/

#Download and decompress Hive

RUN wget -q -P /opt http://apache.rediris.es/hive/hive-2.3.4/apache-hive-2.3.4-bin.tar.gz && \
    tar xzf /opt/apache-hive-2.3.4-bin.tar.gz -C /opt && \
    rm -f /opt/apache-hive-2.3.4-bin.tar.gz

ENV HIVE_HOME /opt/apache-hive-2.3.4-bin
ENV PATH="${PATH}:/opt/apache-hive-2.3.4-bin/bin"

COPY start.sh /start.sh

RUN chmod +x /start.sh

#Start Hadoop, initialize hadoop directories for Hive, initialize metastore

RUN /etc/init.d/ssh start && \
    hdfs namenode -format
    
RUN /etc/init.d/ssh start && \
    start-dfs.sh && \
    start-yarn.sh && \
    mr-jobhistory-daemon.sh start historyserver && \ 
    hadoop fs -mkdir -p    /user/hive/warehouse  && \
    hadoop fs -chmod g+w   /user/hive/warehouse && \
    hadoop fs -chmod -R 777 /user/hive && \
    hadoop fs -chmod -R 777 /tmp  

CMD ["bash"]

ENTRYPOINT [ "bash", "-c", "/start.sh" ]

