FROM apache/spark:3.5.3

ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.1026/aws-java-sdk-bundle-1.11.1026.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.7.2/iceberg-spark-runtime-3.5_2.12-1.7.2.jar /opt/spark/jars/

ENV SPARK_HOME=/opt/spark
ENV PATH=${SPARK_HOME}/bin:$PATH

USER root



