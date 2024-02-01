# import findspark
# findspark.init()
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# # Set up Spark session
# spark = SparkSession.builder \
#     .appName("Shots_Streaming") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,")\
#     .config("es.nodes.wan.only", "true") \
#     .getOrCreate()

# # Define the schema for the incoming data
# schema = StructType([
#     StructField("X", DoubleType()),
#     StructField("Y", DoubleType()),
#     StructField("a_goals", DoubleType()),
#     StructField("a_team", StringType()),
#     StructField("date", StringType()),
#     StructField("h_goals", DoubleType()),
#     StructField("h_team", StringType()),
#     StructField("id", DoubleType()),
#     StructField("league", StringType()),
#     StructField("match_id", DoubleType()),
#     StructField("result", StringType()),
#     StructField("season", StringType()),
# ])

# # Read data from Kafka topic
# df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "shots") \
#     .load() \
#     .selectExpr("CAST(value AS STRING)")

# # Parse JSON data
# parsed_df = df.select(from_json(df.value, schema).alias("data")).select("data.*")

# # Drop specified columns
# columns_to_delete = ['shotType', 'minute', 'situation', 'player_assisted', 'lastAction', 'xG', 'player', 'h_a', 'player_id']
# parsed_df = parsed_df.drop(*columns_to_delete)

# # Multiply 'X' and 'Y' columns by 100
# parsed_df = parsed_df.withColumn("X", col("X") * 100)
# parsed_df = parsed_df.withColumn("Y", col("Y") * 100)

# # Display the streaming data
# query = parsed_df \
#     .writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .start()

# # Wait for the streaming to finish
# query.awaitTermination()

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Set up Spark session
spark = SparkSession.builder \
    .appName("Shots_Streaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1")\
    .getOrCreate()

# Define the schema for the incoming data
schema = StructType([
    StructField("X", DoubleType()),
    StructField("Y", DoubleType()),
    StructField("a_goals", DoubleType()),
    StructField("a_team", StringType()),
    StructField("date", StringType()),
    StructField("h_goals", DoubleType()),
    StructField("h_team", StringType()),
    StructField("id", DoubleType()),
    StructField("league", StringType()),
    StructField("match_id", DoubleType()),
    StructField("result", StringType()),
    StructField("season", StringType()),
])

# Read data from Kafka topic
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "shots") \
    .load() \
    .selectExpr("CAST(value AS STRING)")

# Parse JSON data
parsed_df = df.select(from_json(df.value, schema).alias("data")).select("data.*")

# Drop specified columns
columns_to_delete = ['shotType', 'minute', 'situation', 'player_assisted', 'lastAction', 'xG', 'player', 'h_a', 'player_id']
parsed_df = parsed_df.drop(*columns_to_delete)

# Multiply 'X' and 'Y' columns by 100
parsed_df = parsed_df.withColumn("X", col("X") * 100)
parsed_df = parsed_df.withColumn("Y", col("Y") * 100)

mongo_uri = "mongodb://localhost:27017"  
mongo_db_name = "test"
collection_name = "test"

# Save the data to MongoDB
query =parsed_df \
    .writeStream \
    .foreachBatch(lambda batchDF, batchId: batchDF.write \
        .format("mongo") \
        .option("uri", mongo_uri) \
        .option("database", mongo_db_name) \
        .option("collection", collection_name) \
        .mode("append") \
        .save()
    ) \
    .outputMode("append") \
    .start()

# Start the Spark session
spark.streams.awaitAnyTermination()