# ./airflow/Dockerfile
FROM apache/airflow:2.8.1

# Install Java (JDK) — Java 17 is available in Debian bookworm
USER root
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk-headless ca-certificates; \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME (typical path on Debian amd64 for JDK 17)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Remote logging to MinIO/S3 (connection AIRFLOW_CONN_MINIO_S3 is provided at runtime)
ENV AIRFLOW__LOGGING__REMOTE_LOGGING="True"
ENV AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER="s3://logging/airflow/logs"
ENV AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="minio_s3"

COPY apps/CONFIG/airflow.cfg /opt/airflow/airflow.cfg

# Install Spark provider
USER airflow
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark apache-airflow-providers-amazon pyspark==3.5.3

