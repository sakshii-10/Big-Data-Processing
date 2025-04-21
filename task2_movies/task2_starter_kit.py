import sys
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import from_unixtime, date_format, col, to_date, concat_ws, sum, month, to_timestamp, count, length, when, dayofweek, to_csv, round, year, split, explode, avg, desc
from pyspark.sql.types import FloatType, IntegerType
    
if __name__ == "__main__":

    spark = SparkSession \
        .builder \
        .appName("movieratings") \
        .getOrCreate()

    # Load environment variables
    s3_data_repository_bucket = os.environ.get('DATA_REPOSITORY_BUCKET', '')
    s3_endpoint_url = os.environ.get('S3_ENDPOINT_URL', '') + ':' + os.environ.get('BUCKET_PORT', '')
    s3_access_key_id = os.environ.get('AWS_ACCESS_KEY_ID', '')
    s3_secret_access_key = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    s3_bucket = os.environ.get('BUCKET_NAME', '')

    # Configure Hadoop for S3 access
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    # Write to s3 bucket
    # rating_distribution.coalesce(1).write.csv("s3a://" + s3_bucket + "/rating_distribution_.csv", header=True, mode="overwrite")
    #Task1
    # Load the ratings dataset
    ratings_path = "s3a://" + s3_data_repository_bucket + "/ECS765/MovieLens/ratings.csv"
    ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
    movies_path = "s3a://" + s3_data_repository_bucket + "/ECS765/MovieLens/movies.csv"   
    movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
 
    print("\nSample Data:")
    ratings_df.show(5)

    # Count the number of unique users
    unique_users_count = ratings_df.select("userId").distinct().count()
    print(f"\nNumber of unique users who have rated movies: {unique_users_count}")

    #Task2
    # Convert Unix timestamp to date format (YYYY-MM-DD)
    ratings_with_date = ratings_df.withColumn("date", from_unixtime(col("timestamp")).cast("date"))

    # Sort by date
    sorted_ratings = ratings_with_date.orderBy("date")

    # Show the results (first 20 rows)
    print("Ratings data with converted dates (sorted):")
    sorted_ratings.select("userId", "movieId", "rating","timestamp", "date").show(10, truncate=False)

    # Task 3: Rating Distribution Analysis (new implementation)
    print("\n=== TASK 3: RATING DISTRIBUTION ANALYSIS ===")
    
    # Categorize ratings using Spark SQL functions only
    rating_distribution = ratings_df.withColumn(
        "rating_category", 
        when((col("rating") >= 0.5) & (col("rating") <= 2.5), "Low")
        .when((col("rating") >= 3.0) & (col("rating") <= 4.5), "Medium")
        .when(col("rating") == 5.0, "High")
        .otherwise("Unknown")
    ).groupBy("rating_category").agg(
        count("*").alias("count"),
        (count("*") / ratings_df.count() * 100).alias("percentage")
    ).orderBy("count", ascending=False)


    # Show results
    print("\nRating Distribution by Category:")
    rating_distribution.show(truncate=False)
    
    # Save results for visualization (using your original method)
    #output_path = f"s3a://{s3_bucket}/rating_distribution.csv"
    #rating_distribution.coalesce(1).write.csv(output_path,header=True,mode="overwrite")
    #print(f"\nRating distribution results saved to: {output_path}")

    #Task4
    # Add time-based columns
    processed_df = ratings_df.withColumn("date", from_unixtime(col("timestamp")).cast("timestamp")) \
        .withColumn("year", year(col("date"))) \
        .withColumn("month", month(col("date"))) \
        .withColumn("time_of_year",
            when((col("month") >= 1) & (col("month") <= 6), "Early Year")
            .otherwise("Late Year"))
    
    # Show sample
    print("Sample processed data:")
    processed_df.select("userId", "movieId", "rating", "date", "time_of_year").show(5)
    
    # Get counts by time period
    time_counts = processed_df.groupBy("time_of_year").agg(
        count("*").alias("rating_count")
    ).orderBy("rating_count", ascending=False)
    
    # Save results
    output_path2 = f"s3a://{s3_bucket}/time_results"
    time_counts.coalesce(1).write.mode("overwrite").csv(
        f"{output_path2}/time_counts", header=True)
    
    spark.stop()
    '''
    # Save results for visualization
    output_path2 = f"s3a://{s3_bucket}/time_of_year_results"
    processed_df.coalesce(1).write.mode("overwrite").csv(output_path2, header=True)
    
    #print(f"Results saved to: {output_path2}")
    
    # Task 5: Genre Analysis
    print("\n=== TASK 5: GENRE ANALYSIS ===")
    
    # Valid genres from the problem statement
    valid_genres = [
        "Action", "Adventure", "Animation", "Children", "Comedy","Crime", "Documentary", "Drama", "Fantasy", "Film-Noir",
        "Horror", "Musical", "Mystery", "Romance", "Sci-Fi","Thriller", "War", "Western"]
    
    # Load movies data
    movies_path = "s3a://" + s3_data_repository_bucket + "/ECS765/MovieLens/movies.csv"
    movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
    
    # Process genres
    genre_analysis = (
        ratings_df.join(movies_df, "movieId")
        .withColumn("genre", explode(split(col("genres"), "\|")))
        .filter(col("genre").isin(valid_genres))
        .groupBy("genre")
        .agg(count("*").alias("rating_count"))
        .orderBy("rating_count", ascending=False))
    
    # Show results
    print("Top genres by rating count:")
    genre_analysis.show(10, truncate=False)
    
    # Save results for visualization
    genre_output_path = f"s3a://{s3_bucket}/genre_analysis"
    genre_analysis.coalesce(1).write.mode("overwrite").csv(genre_output_path, header=True)
    print(f"Genre results saved to: {genre_output_path}")
    
    #Task6
    # Load ratings data
    ratings_path = "s3a://data-repository-bkt/ECS765/MovieLens/ratings.csv"
    ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
    
    # Extract year and aggregate
    yearly_ratings = (ratings_df
        .withColumn("year", year(from_unixtime(col("timestamp"))))
        .groupBy("year")
        .agg(
            count("*").alias("total_ratings"),
            (count("*") / ratings_df.count() * 100).alias("percentage_of_total"))
        .orderBy("year"))
    
    # Show sample results
    print("Yearly Ratings Summary:")
    yearly_ratings.show(10, truncate=False)
    
    # Save results
    output_path =  f"s3a://{s3_bucket}/yearly_ratings"
    yearly_ratings.coalesce(1).write.mode("overwrite").csv(output_path, header=True)
    print(f"Results saved to: {output_path}")
    
    #Task7
    # Calculate top movies
    top_movies = (
        ratings_df.groupBy("movieId")
        .agg(
            avg("rating").alias("avg_rating"),
            count("*").alias("num_ratings")
        )
        .join(movies_df, "movieId")
        .filter(col("num_ratings") >= 100)  # Minimum 100 ratings for significance
        .select(
            "title",
            round("avg_rating", 2).alias("average_rating"),
            "num_ratings")
        .orderBy(desc("average_rating"), desc("num_ratings"))
        .limit(10))
    
    # Show results
    print("Top 10 Highest Rated Movies (with ≥100 ratings):")
    top_movies.show(10, truncate=False)
    
    
    #Task8
    # Categorize users
    user_activity = (
        ratings_df.groupBy("userId")
        .agg(count("*").alias("rating_count"))
        .withColumn("user_type",
            when(col("rating_count") > 50, "Frequent Rater")
            .otherwise("Infrequent Rater")))
    
    # Get top 10 active users
    top_users = (
        user_activity
        .orderBy(desc("rating_count"))
        .limit(10))
    
    # Get distribution counts
    user_distribution = (
        user_activity
        .groupBy("user_type")
        .agg(count("*").alias("user_count")))
    
    # Show results
    print("Top 10 Most Active Users:")
    top_users.show(10, truncate=False)
    
    print("\nUser Type Distribution:")
    user_distribution.show()
    
    # Save results
    user_activity.coalesce(1).write.mode("overwrite").csv(
        "s3a://object-bucket-ec24578-dd245d22-d551-419d-9bc5-4c77d038ce84/user_analysis", header=True)
    print("Results saved to S3")
    '''
    #spark.stop()