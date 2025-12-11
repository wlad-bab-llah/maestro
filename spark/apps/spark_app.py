from pyspark.sql import SparkSession
# Configuration Spark avec Iceberg
spark = SparkSession.builder \
    .appName("IcebergApp") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.defaultCatalog", "spark_catalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.spark_catalog.uri", "thrift://hivemetastore:9083") \
    .config("spark.sql.catalog.spark_catalog.warehouse", "s3a://iceberg-warehouse/warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "password") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .getOrCreate()



spark.sparkContext.setLogLevel("OFF")
spark.sql("SHOW databases").show()


# spark.sql("""
# CREATE SCHEMA  test_database
# LOCATION 's3a://bronze/database'
# """).show()
# spark.sql("SHOW databases").show()

# spark.sql("SHOW tables in test_database").show()


# spark.sql("""
# CREATE SCHEMA IF NOT EXISTS spark_catalog.test_database
# LOCATION 's3a://bronze/database'
# """).show()
# spark.sql("SHOW databases").show()
# spark.sql("""
# CREATE NAMESPACE IF NOT EXISTS test_database
# LOCATION 's3a://iceberg-warehouse/warehouse/test_database'
# """)
# spark.sql("SHOW databases").show()
# spark.sql("""
#   CREATE TABLE if not exists test_database.ma_table_iceberg (
#     name STRING,
#     age INT
#   )
#   USING iceberg
#   LOCATION 's3a://bronze/database/ma_table_iceberg'
# """)

# data = [("soufiane", 30), ("alex", 25)]
# columns = ["name", "age"]
# spark.sql("SHOW TABLES IN test_database").show()

# df = spark.createDataFrame(data, columns)
# df.write.format("iceberg").mode("overwrite").save("test_database.ma_table_iceberg")
# # Création d'une table Iceberg
# #df.writeTo("test_database.ma_table_iceberg").using("iceberg").createOrReplace()

# # Lecture des données
# result = spark.sql("SELECT * FROM test_database.ma_table_iceberg")
# result.show()

# # Fonctionnalités Iceberg
# spark.sql("SELECT * FROM test_database.ma_table_iceberg.history").show()
# spark.sql("SELECT * FROM test_database.ma_table_iceberg.snapshots").show()

spark.sql("create database if not exists test_db location 's3a://iceberg-warehouse/warehouse/test_db'").show()

spark.sql("show databases").show()

spark.sql("""create table if not exists test_db.iceberg_table (
    id bigint,
    data string
) using iceberg""").show()
