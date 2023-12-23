import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import pyspark
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
from pyspark.sql.functions import from_unixtime, from_json, col, window, sum, expr, lit

# Load environment variables
dotenv_path = Path("/opt/app/.env")
load_dotenv(dotenv_path=dotenv_path)

# Configuration variables
config = {
    "spark_master_host_name": os.getenv("SPARK_MASTER_HOST_NAME"),
    "spark_master_port": os.getenv("SPARK_MASTER_PORT"),
    "kafka_host": os.getenv("KAFKA_HOST"),
    "kafka_topic": os.getenv("KAFKA_TOPIC_NAME"),
}

# Set Spark packages
os.environ["PYSPARK_SUBMIT_ARGS"] = "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 org.postgresql:postgresql:42.2.18"

# Set additional Spark configuration for force deleting temp checkpoint location
os.environ["spark.sql.streaming.forceDeleteTempCheckpointLocation"] = "true"

# Spark configuration
spark_host = f"spark://{config['spark_master_host_name']}:{config['spark_master_port']}"
spark_context = pyspark.SparkContext.getOrCreate(conf=(pyspark.SparkConf().setAppName("DEStreaming").setMaster(spark_host)))
spark_context.setLogLevel("WARN")
spark = pyspark.sql.SparkSession.builder.master(spark_host).appName("DEStreaming").getOrCreate()

# Define Kafka schema
kafka_schema = StructType().add("order_id", StringType()).add("customer_id", IntegerType()).add("furniture", StringType()).add("color", StringType()).add("price", IntegerType()).add("ts", IntegerType())

# Read Kafka stream
stream_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", f"{config['kafka_host']}:9092")
    .option("subscribe", config['kafka_topic'])
    .option("startingOffsets", "latest")
    .load()
)

# Parse Kafka stream
parsed_df = stream_df.select(from_json(col("value").cast("string"), kafka_schema).alias("parsed_value")).select("parsed_value.*")
parsed_df = parsed_df.withColumn("ts", from_unixtime("ts").cast(TimestampType()))

# Define result schema
result_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", IntegerType()),
    StructField("furniture", StringType()),
    StructField("color", StringType()),
    StructField("price", IntegerType()),
    StructField("ts", TimestampType()),
    StructField("window_start", TimestampType()),
    StructField("window_end", TimestampType()),
    StructField("total_amount_sold", LongType())
])

# Create an empty result DataFrame
result_df = spark.createDataFrame([], result_schema)

# Process each batch
def process_batch(df, epoch_id):
    global result_df
    
    print(f"Results for Batch {epoch_id}:")
    df.show(truncate=False)

    windowed_df = df.withWatermark("ts", "60 minutes").withColumn("window", window("ts", "1 day")) \
        .groupBy(window("ts", "1 day", "1 day").alias("window")).agg(sum("price").alias("total_amount_sold")) \
        .selectExpr("window.start AS window_start", "window.end AS window_end", "total_amount_sold")

    print("Windowed DataFrame:")
    windowed_df.show(truncate=False)

    windowed_df = windowed_df.withColumn("order_id", lit(None).cast(StringType())) \
        .withColumn("customer_id", lit(None).cast(IntegerType())) \
        .withColumn("furniture", lit(None).cast(StringType())) \
        .withColumn("color", lit(None).cast(StringType())) \
        .withColumn("price", lit(None).cast(IntegerType())) \
        .withColumn("ts", lit(None).cast(IntegerType())) \
        .withColumn("ts", from_unixtime("ts").cast(TimestampType()))

    result_df = result_df.union(windowed_df.select(
        "order_id", "customer_id", "furniture", "color", "price", "ts", 
        "window_start", "window_end", "total_amount_sold"
    ))

    updated_total_amount = result_df.groupBy("window_start", "window_end").agg(sum("total_amount_sold").alias("updated_total_amount"))

    print("Updated Total Amount Per Day:")
    updated_total_amount.show(truncate=False)

# Define the streaming query
foreach_query = parsed_df.writeStream.outputMode("update").trigger(processingTime="10 seconds").foreachBatch(process_batch).start()

# Await termination
foreach_query.awaitTermination()
