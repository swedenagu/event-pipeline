from pyspark.sql import SparkSession
from pyspark.sql.functions import from_avro, col, current_timestamp
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("EventPipeline") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Avro schema
with open("schema/user_event_v1.avsc") as f:
    schema = f.read()

parsed = df.select(from_avro(col("value"), schema).alias("data")).select("data.*")
# Add processing timestamp
enriched = parsed.withColumn("processed_at", current_timestamp())
# Write to ClickHouse (via JDBC or custom connector)
query = enriched.writeStream \
    .outputMode("append") \
    .format("jdbc") \
    .option("url", "jdbc:clickhouse://localhost:8123/default") \
    .option("dbtable", "events") \
    .option("checkpointLocation", "/tmp/checkpoints/kafka-to-clickhouse") \
    .trigger(processingTime='5 seconds') \
    .start()
