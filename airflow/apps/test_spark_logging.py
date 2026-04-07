import random
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestRemoteLogging") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("SPARK TEST - Remote Logging Verification")
print("=" * 60)

data = [("Alice", 30), ("Bob", 25), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])
df.show()

status = random.choice(["OK", "KO"])
print(f"Spark job status: {status}")

print("Spark version:", spark.version)
spark.stop()
