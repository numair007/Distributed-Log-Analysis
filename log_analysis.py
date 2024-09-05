# log_analysis.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window

# Initialize Spark Session with MongoDB
spark = SparkSession.builder \
    .appName("Log Analysis with PySpark") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/logsdb.access_logs") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/logsdb.processed_logs") \
    .getOrCreate()

# Load logs from MongoDB
logs_df = spark.read \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .load()

# Log Aggregation: Count requests by host
def log_aggregation():
    host_aggregation = logs_df.groupBy("host").count().orderBy(col("count").desc())
    host_aggregation.show()
    return host_aggregation

# Traffic Analysis: Analyze traffic based on time window (e.g., every hour)
def traffic_analysis():
    traffic_analysis = logs_df.groupBy(window(col("datetime"), "1 hour")).count().orderBy(col("count").desc())
    traffic_analysis.show()
    return traffic_analysis

# Error Tracking: Filter logs with HTTP error codes (status 4xx or 5xx)
def error_tracking():
    error_logs = logs_df.filter((col("status") >= 400) & (col("status") < 600))
    error_logs.show()
    return error_logs

if __name__ == "__main__":
    # Run log analytics
    host_agg = log_aggregation()
    traffic_analysis = traffic_analysis()
    error_tracking = error_tracking()

    # Save aggregation results back to MongoDB
    host_agg.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .mode("overwrite") \
        .option("database", "logsdb") \
        .option("collection", "host_aggregations") \
        .save()

    # Stop Spark session
    spark.stop()
