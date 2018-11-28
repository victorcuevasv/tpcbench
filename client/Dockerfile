FROM ubuntujava:dev

#Download and decompress Maven

RUN wget -q -P /opt http://apache.rediris.es/maven/maven-3/3.6.0/binaries/apache-maven-3.6.0-bin.tar.gz && \
    tar xzf /opt/apache-maven-3.6.0-bin.tar.gz -C /opt && \
    rm -f /opt/apache-maven-3.6.0-bin.tar.gz

ENV PATH="${PATH}:/opt/apache-maven-3.6.0/bin"

#Download the Maven dependencies.

ADD project/pom.xml /opt

RUN mvn  dependency:resolve -f /opt/pom.xml && \
    rm -f /opt/pom.xml

CMD ["bash"]


