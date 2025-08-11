#!/bin/bash

# Script to download dependencies manually if needed
# This script sources the .env file and downloads all required dependencies

set -e

# Load environment variables
if [ -f ".env" ]; then
    export $(cat .env | grep -v '#' | xargs)
else
    echo "Error: .env file not found!"
    exit 1
fi

# Create directories
mkdir -p docker/drivers

echo "Starting dependency downloads..."

# Download Hadoop
if [ ! -f "docker/hadoop-${HADOOP_VER}.tar.gz" ]; then
    echo "Downloading Hadoop ${HADOOP_VER}..."
    wget -q "${HADOOP_DOWNLOAD_URL}" -O "docker/hadoop-${HADOOP_VER}.tar.gz"
    echo "✓ Hadoop downloaded"
else
    echo "✓ Hadoop already exists"
fi

# Download Hive Metastore
if [ ! -f "docker/hive-standalone-metastore-${HIVE_VER}-bin.tar.gz" ]; then
    echo "Downloading Hive Metastore ${HIVE_VER}..."
    wget -q "${HIVE_DOWNLOAD_URL}" -O "docker/hive-standalone-metastore-${HIVE_VER}-bin.tar.gz"
    echo "✓ Hive Metastore downloaded"
else
    echo "✓ Hive Metastore already exists"
fi

# Download PostgreSQL JDBC Driver
if [ ! -f "docker/drivers/postgresql-${POSTGRES_JDBC_VERSION}.jar" ]; then
    echo "Downloading PostgreSQL JDBC ${POSTGRES_JDBC_VERSION}..."
    wget -q "${POSTGRES_JDBC_URL}" -O "docker/drivers/postgresql-${POSTGRES_JDBC_VERSION}.jar"
    echo "✓ PostgreSQL JDBC downloaded"
else
    echo "✓ PostgreSQL JDBC already exists"
fi

# Download AWS SDK Bundle
if [ ! -f "docker/drivers/aws-java-sdk-bundle-1.12.599.jar" ]; then
    echo "Downloading AWS SDK Bundle..."
    wget -q "${AWS_SDK_URL}" -O "docker/drivers/aws-java-sdk-bundle-1.12.599.jar"
    echo "✓ AWS SDK Bundle downloaded"
else
    echo "✓ AWS SDK Bundle already exists"
fi

# Download Iceberg Hive Runtime
if [ ! -f "docker/drivers/iceberg-hive-runtime-1.7.2.jar" ]; then
    echo "Downloading Iceberg Hive Runtime..."
    wget -q "${ICEBERG_RUNTIME_URL}" -O "docker/drivers/iceberg-hive-runtime-1.7.2.jar"
    echo "✓ Iceberg Hive Runtime downloaded"
else
    echo "✓ Iceberg Hive Runtime already exists"
fi

echo ""
echo "All dependencies are ready!"
echo "You can now run: docker-compose up --build"