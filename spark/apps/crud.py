from pyspark.sql import SparkSession
import pandas as pd
# Configuration Spark avec Iceberg
spark = SparkSession.builder \
    .appName("IcebergApp") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog.uri", "thrift://hive-metastore:9083") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()

spark.sparkContext.setLogLevel("OFF")

result = spark.sql("insert into default.ma_table_iceberg values ('hello',1)")
result = spark.sql("SELECT * FROM default.ma_table_iceberg")
result.show()


result = spark.sql("update default.ma_table_iceberg set name='abdo' where age=1")
result = spark.sql("SELECT * FROM default.ma_table_iceberg")
result.show()


result = spark.sql("delete from default.ma_table_iceberg where age=1")
result = spark.sql("SELECT * FROM default.ma_table_iceberg")
result.show()