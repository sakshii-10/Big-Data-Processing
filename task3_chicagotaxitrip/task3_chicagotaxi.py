import sys, string
import os
import socket
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from graphframes import GraphFrame
from pyspark.sql.functions import col, hour, to_timestamp, sum as spark_sum, when, size, array, lit, avg, round


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("ChicagoTaxiTripsAnalysis")\
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
    
    
    # step 1: loading the data
    file_path = "s3a://data-repository-bkt/ECS765/Chicago_Taxitrips/chicago_taxi_trips.csv"
    df = spark.read.option("header", True).csv(file_path, inferSchema=True)
    
    print("Total number of entries in the DataFrame:", df.count())
    
    # step 2: define schemas and construct dataframes
    vertexSchema = StructType([
        StructField("id", IntegerType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True),
        StructField("CensusTract", StringType(), True)])
    
    edgeSchema = StructType([
        StructField("src", IntegerType(), True),
        StructField("dst", IntegerType(), True),
        StructField("TripMiles", DoubleType(), True),
        StructField("TripSeconds", IntegerType(), True),
        StructField("Fare", DoubleType(), True) ])
    
    # Extract pickup and dropoff vertices and prepare edge dataframe
    pickup_vertices = df.selectExpr(
        "CAST(`Pickup Community Area` AS INT) AS id",
        "`Pickup Centroid Latitude` AS Latitude",
        "`Pickup Centroid Longitude` AS Longitude",
        "`Pickup Census Tract` AS CensusTract")
    
    dropoff_vertices = df.selectExpr(
        "CAST(`Dropoff Community Area` AS INT) AS id",
        "`Dropoff Centroid Latitude` AS Latitude",
        "`Dropoff Centroid Longitude` AS Longitude",
        "`Dropoff Census Tract` AS CensusTract")
    
    vertices = pickup_vertices.union(dropoff_vertices).dropDuplicates(["id"])
    vertices = spark.createDataFrame(vertices.rdd, schema=vertexSchema)
    
    edges = df.selectExpr(
        "CAST(`Pickup Community Area` AS INT) AS src",
        "CAST(`Dropoff Community Area` AS INT) AS dst",
        "CAST(`Trip Miles` AS DOUBLE) AS TripMiles",
        "CAST(`Trip Seconds` AS INT) AS TripSeconds",
        "CAST(`Fare` AS DOUBLE) AS Fare"
    ).dropna(subset=["src", "dst"])
    edges = spark.createDataFrame(edges.rdd, schema=edgeSchema)
    
    # Show sample data
    print("Vertices sample:")
    vertices.show(5, truncate=False)
    
    print("Edges sample:")
    edges.show(5, truncate=False)
    '''
    # step 3: Create GraphFrame and analyze
    g = GraphFrame(vertices, edges)
    
    # Join to add src and dst metadata
    graph_df = g.edges \
        .join(g.vertices, g.edges.src == g.vertices.id, "left") \
        .withColumnRenamed("Latitude", "src_Latitude") \
        .withColumnRenamed("Longitude", "src_Longitude") \
        .withColumnRenamed("CensusTract", "src_CensusTract") \
        .drop("id") \
        .join(g.vertices, g.edges.dst == g.vertices.id, "left") \
        .withColumnRenamed("Latitude", "dst_Latitude") \
        .withColumnRenamed("Longitude", "dst_Longitude") \
        .withColumnRenamed("CensusTract", "dst_CensusTract") \
        .drop("id")
    
    graph_df.select(
        "src", "dst", "TripMiles", "TripSeconds", "Fare",
        "src_Latitude", "src_Longitude", "src_CensusTract",
        "dst_Latitude", "dst_Longitude", "dst_CensusTract"
    ).dropDuplicates(["src", "dst"]).show(10, truncate=False)
    
    # step 4: Same-area trips
    same_area_edges = g.edges.filter("src = dst")
    print("Number of trips within the same community area:", same_area_edges.count())
    same_area_edges.show(10, truncate=False)
    
    
    
    # step 5: Shortest paths to area 49
    shortest_paths_df = g.bfs(fromExpr="id != 49", toExpr="id = 49", maxPathLength=10)
    shortest_paths_df.show(10, truncate=False)
    
    # Step 5: Shortest paths to area 49 (BFS) - Fixed version
    #hello
    shortest_paths_df = g.bfs(
    fromExpr="id != 49",
    toExpr="id = 49",
    maxPathLength=10)
    
    # Get all edge columns (e0, e1, etc.)
    edge_columns = [c for c in shortest_paths_df.columns if c.startswith("e")]
    distance_expr = size(array([when(col(c).isNotNull(), 1).otherwise(0) for c in edge_columns]))
    
    # Create the final result DataFrame
    result_df = shortest_paths_df.select(
        col("from.id").alias("id"),
        lit(49).alias("landmark"),
        distance_expr.alias("distance")
    ).distinct()
    
    print("Shortest paths to community area 49:")
    result_df.orderBy("distance").show(10, truncate=False)

    top_5.coalesce(1).write.csv(output_path_unweighted, header=True, mode="overwrite")
    output_path_unweighted = f"s3a://{s3_bucket}/pagerank_top5_unweighted"
     output_path_weighted = f"s3a://{s3_bucket}/pagerank_top5_weighted"
 top_5_weighted.coalesce(1).write.csv(output_path_weighted, header=True, mode="overwrite")
    
    
    # step 6: Unweighted PageRank 
    vertices_df = pickup_vertices.union(dropoff_vertices) \
        .dropna(subset=["id"]).dropDuplicates(["id"]) \
        .withColumn("id", col("id").cast("string"))
    
    edges_df = df.select(
        col("Pickup Community Area").cast("string").alias("src"),
        col("Dropoff Community Area").cast("string").alias("dst"),
        col("Trip Miles").cast("double"),
        col("Trip Seconds").cast("int"),
        col("Fare").cast("double")
    ).dropna(subset=["src", "dst"])
    
    g_pagerank = GraphFrame(vertices_df, edges_df)
    results = g_pagerank.pageRank(resetProbability=0.15, tol=0.01)
    
    top_5 = results.vertices.select("id", "pagerank") \
        .orderBy(col("pagerank").desc()).limit(5)
    top_5.show(truncate=False)
    
    # Weighted PageRank using Fare 
    weighted_edges_df = edges_df.groupBy("src", "dst").agg(
        spark_sum("Fare").alias("weight"))
    
    g_weighted = GraphFrame(vertices_df, weighted_edges_df)
    results_weighted = g_weighted.pageRank(resetProbability=0.15, maxIter=10)
    
    top_5_weighted = results_weighted.vertices.select("id", "pagerank") \
        .orderBy(col("pagerank").desc()).limit(5)
    top_5_weighted.show(truncate=False)
    
    '''
    # step 7: Fare Analysis 
    print("Question 7")


    # Prepare DataFrame with required columns and proper types
    fareMetricsDF = df \
        .withColumn("fare", col("Fare").cast("double")) \
        .withColumn("trip_miles", col("Trip Miles").cast("double")) \
        .withColumn("trip_seconds", col("Trip Seconds").cast("int")) \
        .withColumn("HourOfDay", hour(to_timestamp("Trip Start Timestamp", "MM/dd/yyyy hh:mm:ss a"))) \
        .select("fare", "trip_miles", "trip_seconds", "HourOfDay") \
        .dropna()

    # Show sample records
    print("Sample fare-related trip metrics:")
    fareMetricsDF.show(10, truncate=False)

    # Summary statistics
    print("Summary statistics of fare, trip miles, and duration:")
    fareMetricsDF.describe(["fare", "trip_miles", "trip_seconds"]).show()

    # Group 1: Average fare by Trip Miles
    print("Average fare grouped by Trip Miles:")
    fareMetricsDF.groupBy("trip_miles") \
        .agg(avg("fare").alias("Avg Fare")) \
        .orderBy("trip_miles") \
        .show()

    # Group 2: Average fare by Trip Seconds
    print("Average fare grouped by Trip Seconds:")
    fareMetricsDF.groupBy("trip_seconds") \
        .agg(avg("fare").alias("Avg Fare")) \
        .orderBy("trip_seconds") \
        .show()

    # Group 3: Average fare by Hour of Day
    print("Average fare grouped by Start Hour:")
    fareMetricsDF.groupBy("HourOfDay") \
        .agg(avg("fare").alias("Avg Fare")) \
        .orderBy("HourOfDay") \
        .show()

    # Optional: Save the cleaned DataFrame to S3
    fare_output_path = f"s3a://{s3_bucket}/fare_analysis_data_latest"
    fareMetricsDF.coalesce(1).write.csv(fare_output_path, header=True, mode="overwrite")
    spark.stop()
    