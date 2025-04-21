import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, unix_timestamp, round, concat, lit, dayofweek, hour, when, avg, expr
from pyspark.sql.types import FloatType, IntegerType

if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("twitterdata")\
        .getOrCreate()
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL']+':'+os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # Task 1: Load Data
    print("TASK 1")

    df = spark.read.option("header", "true").csv("s3a://data-repository-bkt/ECS765/Twitter/twitter.csv")
    print(f"Total number of entries: {df.count()}")
    df.show(10, truncate=False)

    # Task 2: Filter Data 
    print("TASK 2")
    # Reference timestamp conversion approach
    df = df.withColumn("timestamp_corrected", to_timestamp(col("timestamp").cast("string"), "yyyyMMddHHmmss"))

    # Timezone handling
    df = df.withColumn("timezone", col("timezone").cast("int"))
    df = df.filter(col("timezone").isNotNull())

    df = df.withColumn("utc_timestamp",
        from_unixtime(unix_timestamp("timestamp_corrected") - col("timezone") * 3600))

    # Date extraction
    df = df.withColumn("date", date_format(col("utc_timestamp"), "yyyy-MM-dd"))
    df = df.withColumn("day_of_week", date_format(col("utc_timestamp"), "EEEE"))

    # Filter weekdays and sort
    df_filtered = df.filter(~col("day_of_week").isin(["Saturday", "Sunday"]))
    df_filtered = df_filtered.orderBy("date")

    print("\nFirst 10 Filtered Rows:")
    df_filtered.select("longitude","latitude","timestamp","timezone","utc_timestamp","date","day_of_week").show(10, truncate=False)

    # Task 3: Geographical Distribution 
    print("TASK 3")
    
    # Geographical analysis
    geo_df = (df
             .withColumn("lon_rounded", round("longitude", 1))
             .withColumn("lat_rounded", round("latitude", 1))
             .groupBy("lon_rounded", "lat_rounded")
             .agg(count("*").alias("tweet_count")))
    
    # Save results for visualization
    output_path = f"s3a://{s3_bucket}/geo_analysis_results" 
    geo_df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
    
    # Task 4
    print("TASK 4")
    # Add hour column
    df_time = df.withColumn("hour", hour(col("utc_timestamp")))
    
    # Add time_of_day categorization
    df_time = df_time.withColumn("time_of_day",
        when((col("hour") >= 5) & (col("hour") <= 11), "Morning")
        .when((col("hour") >= 12) & (col("hour") <= 16), "Afternoon")
        .when((col("hour") >= 17) & (col("hour") <= 21), "Evening")
        .otherwise("Night"))
    
    # Show sample results
    print("\nSample rows with new columns:")
    df_time.select("utc_timestamp", "hour", "time_of_day").show(10, truncate=False)
    
    # Save time distribution data for visualization
    time_dist_path = f"s3a://{s3_bucket}/time_distribution"
    (df_time.groupBy("time_of_day").count().orderBy("count", ascending=False).coalesce(1)
     .write.option("header", "true").mode("overwrite").csv(time_dist_path))

    #Task 5
    print("TASK 5")
    
    # Get the full dataset including weekends
    df_all_days = df.orderBy("date")
    
    # Define the correct day order (including weekends)
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    
    # Group and aggregate with manual ordering
    tweets_per_day = (df_all_days
        .groupBy("day_of_week")
        .agg(count("*").alias("total_tweets"))
        .withColumn("tweets_k", (col("total_tweets")/1000).cast("int"))
        .orderBy(expr("array_position(array('Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'), day_of_week)")))
    
    print("Tweets per day of week (including weekends):")
    tweets_per_day.show()

    #Save daily distribution data for visualization
    '''
    daily_output_path = f"s3a://{s3_bucket}/daily_tweets"
    (tweets_per_day
        .coalesce(1)  # Combine into single file for easier handling
        .write
        .option("header", "true")
        .mode("overwrite")
        .csv(daily_output_path))

    #print(f"\nDaily tweet distribution results saved to: {daily_output_path}")
    '''
   
    # Task 6: Filter Aggregates 
    print("TASK 6")
    
    # 1. Calculate mean tweets per day
    mean_tweets = tweets_per_day.agg(avg("total_tweets")).collect()[0][0]
    
    # 2. Filter days with above-average activity
    unusual_days = tweets_per_day.filter(col("total_tweets") > mean_tweets)
    
    # 3. Show results
    print(f"Mean tweets per weekday: {mean_tweets:,.0f}")
    print("\nDays with unusually high tweet volume:")
    unusual_days.show()
    
    # Task 7
    print("TASK 7")

    # 1. Get top 10 locations by tweet count
    top_locations = (geo_df
        .orderBy(col("tweet_count").desc())
        .limit(10)
        .select(
            concat_ws(", ", 
                col("lon_rounded").cast("string"), 
                col("lat_rounded").cast("string")
            ).alias("location"),
            col("tweet_count").alias("num_tweets")))
    
    # 2. Show results
    print("Top 10 Locations by Tweet Count:")
    top_locations.show(10, truncate=False)
  
    spark.stop()