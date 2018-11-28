FROM ubuntujava:dev

#Download and decompress Hadoop

RUN wget -q -P /opt http://apache.rediris.es/hadoop/common/hadoop-2.7.7/hadoop-2.7.7.tar.gz && \
    tar xzf /opt/hadoop-2.7.7.tar.gz -C /opt && \
    rm -f /opt/hadoop-2.7.7.tar.gz

ENV HADOOP_HOME /opt/hadoop-2.7.7
ENV PATH="${PATH}:/opt/hadoop-2.7.7/bin:/opt/hadoop-2.7.7/sbin"

#Download and decompress Hive

RUN wget -q -P /opt http://apache.rediris.es/hive/hive-2.3.4/apache-hive-2.3.4-bin.tar.gz && \
    tar xzf /opt/apache-hive-2.3.4-bin.tar.gz -C /opt && \
    rm -f /opt/apache-hive-2.3.4-bin.tar.gz

ENV HIVE_HOME /opt/apache-hive-2.3.4-bin
ENV PATH="${PATH}:/opt/apache-hive-2.3.4-bin/bin"

#Initialize Hadoop directories for Hive

RUN hadoop fs -mkdir -p    /user/hive/warehouse  && \
    hadoop fs -chmod g+w   /user/hive/warehouse
    
#Initialize Metastore (must run as subshells)
    
RUN (hiveserver2) & (schematool -dbType derby -initSchema)

CMD ["bash"]

ENTRYPOINT [ "sh", "-c", "hiveserver2" ]

