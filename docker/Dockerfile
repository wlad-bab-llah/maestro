# Use an official Java runtime as a parent image
FROM openjdk:8-jdk


# Set environment variables for Hadoop and Hive
ENV HADOOP_VERSION 3.3.4
ENV HIVE_VERSION 3.1.3
ENV HADOOP_HOME /opt/hadoop
ENV HIVE_HOME /opt/hive-metastore
ENV PATH $PATH:$HADOOP_HOME/bin:$HIVE_HOME/bin


RUN apt-get update \
 && apt-get install -y wget netcat tar procps postgresql-client net-tools curl \
 && rm -rf /var/lib/apt/lists/*

# 2) Unpack Hadoop into /opt/hadoop
RUN wget https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz -P /tmp \
 && mkdir -p /opt/hadoop \
 && tar -xzf /tmp/hadoop-${HADOOP_VERSION}.tar.gz -C /opt/hadoop --strip-components=1 \
 && rm /tmp/hadoop-${HADOOP_VERSION}.tar.gz


# 3) Unpack Hive standalone-metastore into /opt/hive-metastore
RUN wget https://repo1.maven.org/maven2/org/apache/hive/hive-standalone-metastore/${HIVE_VERSION}/hive-standalone-metastore-${HIVE_VERSION}-bin.tar.gz  -P /tmp \
 && mkdir -p /opt/hive-metastore \
 && tar -xzf /tmp/hive-standalone-metastore-${HIVE_VERSION}-bin.tar.gz -C /opt/hive-metastore --strip-components=1 \
 && rm /tmp/hive-standalone-metastore-${HIVE_VERSION}-bin.tar.gz


# Set up Hive configuration
COPY config/* $HIVE_HOME/conf/

# Expose necessary ports
EXPOSE 50070 8080 10000

# Start Hadoop and Hive services
CMD ["bash", "-c", "$HADOOP_HOME/bin/start-dfs.sh && $HIVE_HOME/bin/hive --service metastore & $HIVE_HOME/bin/hive"]



# 4) Copy in JDBC, AWS-SDK & Iceberg jars
RUN wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.6.0/postgresql-42.6.0.jar -O $HIVE_HOME/lib/postgresql.jar \
   && wget https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.599/aws-java-sdk-bundle-1.12.599.jar -O $HIVE_HOME/lib/aws-java-sdk-bundle.jar \
   && wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.7.2/iceberg-hive-runtime-1.7.2.jar -O $HIVE_HOME/lib/iceberg-hive-runtime.jar

RUN cp $HADOOP_HOME/share/hadoop/tools/lib/hadoop-aws-${HADOOP_VERSION}.jar \
        $HIVE_HOME/lib/
    
# 5) Copy in configs and entrypoint
COPY config/core-site.xml        $HIVE_HOME/conf/core-site.xml
COPY config/hive-site.xml        $HIVE_HOME/conf/hive-site.xml
COPY config/iceberg.properties   $HIVE_HOME/conf/iceberg.properties
COPY scripts/entrypoint.sh       /scripts/entrypoint.sh

RUN chmod +x /scripts/entrypoint.sh

ENTRYPOINT ["/scripts/entrypoint.sh"]


