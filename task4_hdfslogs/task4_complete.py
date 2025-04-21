import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, concat_ws, to_timestamp, col, window, count

if __name__ == "__main__":
    # Start Spark session
    session = SparkSession.builder \
        .appName("StreamingHDFSLogs_Complete") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()

    session.sparkContext.setLogLevel("ERROR")

    # Read host and port from environment variables with defaults
    hdfs_host = os.getenv("STREAMING_SERVER_HDFS", "localhost")
    hdfs_port = int(os.getenv("STREAMING_SERVER_HDFS_PORT", "9999"))

    print(f"Using host: {hdfs_host}")
    print(f"Using port: {hdfs_port}")

    # Read streaming data from the socket
    stream_input = session.readStream \
        .format("socket") \
        .option("host", hdfs_host) \
        .option("port", hdfs_port) \
        .option("includeTimestamp", True) \
        .load()

    ###########################################################################
    # TASK 1: Basic log parsing with timestamp extraction
    # Extract timestamp and other fields from raw log line
    log_df = stream_input \
        .withColumn("log_date", regexp_extract("value", r'^(\d{6})', 1)) \
        .withColumn("log_time", regexp_extract("value", r'^\d{6}\s(\d{6})', 1)) \
        .withColumn("combined_dt", concat_ws(' ', col("log_date"), col("log_time"))) \
        .withColumn("timestamp", to_timestamp("combined_dt", "yyMMdd HHmmss")) \
        .select("value", "timestamp")

    # Output to console in append mode
    query1 = log_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # Wait for initial batches
    time.sleep(30)
    query1.stop()

    ###########################################################################
    # TASK 2: Add watermarking
    # Define watermark with 5-second delay
    watermarked_df = log_df.withWatermark("timestamp", "5 seconds")

    query2 = watermarked_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    time.sleep(30)
    query2.stop()

    ###########################################################################
    # TASK 3: Pattern and anomaly detection
    # Extract more fields for pattern analysis
    pattern_df = watermarked_df \
        .withColumn("component", regexp_extract("value", r'(\w+):', 1)) \
        .withColumn("log_level", regexp_extract("value", r'(\bINFO\b|\bWARN\b|\bERROR\b)', 1)) \
        .withColumn("message", regexp_extract("value", r':\s(.*)', 1))

    # Analyze patterns in append mode
    query3 = pattern_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    # specific batches (51 and 52)
    batch_count = 0
    while batch_count < 53:
        time.sleep(5)
        progress = query3.lastProgress
        if progress:
            batch_count = progress['batchId'] + 1
            print(f"Processed batch {batch_count}")

    time.sleep(10)
    query3.stop()

    # Count events by type for more detailed analysis
    count_query = pattern_df \
        .groupBy(
            window(col("timestamp"), "10 seconds"),
            "component",
            "log_level"
        ) \
        .agg(count("*").alias("event_count")) \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    time.sleep(30)
    count_query.stop()

    ###########################################################################
    # TASK 4: DataNode activity monitoring
    # Filter for "DataNode" in component field
    datanode_logs = pattern_df.filter(col("component").contains("DataNode"))

    # Count logs per window (60s window, sliding every 30s)
    datanode_activity = datanode_logs \
        .groupBy(window("timestamp", "60 seconds", "30 seconds")) \
        .count() \
        .withColumnRenamed("count", "datanode_count")

    query4 = datanode_activity.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .start()

    time.sleep(60)
    query4.stop()

    ###########################################################################
    # TASK 5: Host aggregation by block size
    # Extract hostname and block size from the raw log value
    log_with_fields = stream_input \
        .withColumn("source", regexp_extract("value", r'(/[\d\.]+:\d+)', 1)) \
        .withColumn("block_size", regexp_extract("value", r'size\s+(\d+)', 1)) \
        .withColumn("block_size", col("block_size").cast("long")) \
        .withColumn("hostname", regexp_extract("source", r'/([\d\.]+)', 1)) \
        .select("hostname", "block_size")

    # Aggregate by hostname and sum block sizes
    host_aggregation = log_with_fields \
        .groupBy("hostname") \
        .sum("block_size") \
        .withColumnRenamed("sum(block_size)", "total_bytes") \
        .orderBy(col("total_bytes").desc())

    # Output to console in complete mode
    query5 = host_aggregation.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start()

    time.sleep(60)
    query5.stop()

    ###########################################################################
    # TASK 6: Block ID filtering and host counting
    # Enhanced log parsing with better pattern matching
    parsed_logs = stream_input \
        .withColumn("log_level", regexp_extract(col("value"), r'(\bINFO\b|\bWARN\b|\bERROR\b)', 1)) \
        .withColumn("block_id", regexp_extract(col("value"), r'(blk_[-\d]+)', 1)) \
        .withColumn("hostname", regexp_extract(col("value"), r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})', 1)) \
        .select("value", "log_level", "block_id", "hostname")

    # Filter for INFO logs with block IDs
    filtered_logs = parsed_logs \
        .filter((col("log_level") == "INFO") & 
                (col("block_id").isNotNull()) & 
                (col("hostname").isNotNull()))

    # Count by hostname and sort
    host_counts = filtered_logs \
        .groupBy("hostname") \
        .agg(count("*").alias("block_operations")) \
        .orderBy(col("block_operations").desc())

    # Configure streaming output with 15-second trigger
    query6 = host_counts.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 100) \
        .trigger(processingTime="15 seconds") \
        .start()

    # Monitor processing for batch 5
    batches_processed = 0
    while batches_processed < 6:
        time.sleep(15)
        status = query6.lastProgress
        if status and status['batchId'] >= batches_processed:
            batches_processed = status['batchId'] + 1
            print(f"Processed batch {batches_processed}")

    query6.stop()
    session.stop()