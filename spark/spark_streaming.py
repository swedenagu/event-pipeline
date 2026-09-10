from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import *
import os
kafka_broker = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

spark = SparkSession.builder \
    .appName("EventPipeline") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/opt/spark/spark/checkpoints") \
    .getOrCreate()

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", "user-events") \
    .option("startingOffsets", "latest") \
    .load()

# Parse Avro schema
with open("user_event_v1.avsc") as f:
    schema = f.read()

parsed = df.select(from_avro(col("value"), schema).alias("data")).select("data.*")
# Add processing timestamp
enriched = parsed.withColumn("processed_at", current_timestamp())

def write_to_clickhouse(batch_df, batch_id):
    # Remove "processed-at" column populated natively via Clickhouse init.sql
    # JDBC driver tries to incorrectly drop-create the table otherwise

    payload_df = batch_df.drop("processed_at") if "processed_at" in batch_df.columns else batch_df

    payload_df.write \
        .format("jdbc") \
        .option("url", "jdbc:clickhouse://default:clickhouse@clickhouse:8123/event_pipeline") \
        .option("dbtable", "events_queue") \
        .option("driver", "com.clickhouse.jdbc.ClickHouseDriver") \
        .option("user", "default") \
        .option("password", "clickhouse") \
        .mode("append") \
        .save()

# Write to ClickHouse (via JDBC or custom connector)
query = enriched.writeStream \
    .foreachBatch(write_to_clickhouse) \
    .option("checkpointLocation", "/opt/spark/spark/checkpoints/kafka-to-clickhouse") \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
