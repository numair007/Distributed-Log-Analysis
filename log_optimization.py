# log_optimization.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark Session with MongoDB
spark = SparkSession.builder \
    .appName("Log Optimization with PySpark") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/logsdb.access_logs") \
    .getOrCreate()

# Load logs from MongoDB
logs_df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .load()

# Cache the logs DataFrame for faster repeated access
logs_df.cache()

# Partitioning logs based on status code for performance optimization
def partition_logs():
    partitioned_logs = logs_df.repartition(5, col("status"))
    partitioned_logs.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .mode("overwrite") \
        .option("database", "logsdb") \
        .option("collection", "partitioned_logs") \
        .save()

if __name__ == "__main__":
    partition_logs()
    print("Logs successfully partitioned and cached.")
    spark.stop()
