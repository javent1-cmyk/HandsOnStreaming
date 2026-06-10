import os

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, window, to_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Task3_WindowedAggregation") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("trip_id", StringType()) \
    .add("driver_id", StringType()) \
    .add("distance_km", DoubleType()) \
    .add("fare_amount", DoubleType()) \
    .add("timestamp", StringType())

raw_df = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

parsed_df = raw_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

timestamped_df = parsed_df.withColumn(
    "event_time",
    to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
)

watermarked_df = timestamped_df.withWatermark("event_time", "1 minute")

windowed_df = watermarked_df.groupBy(
    window(col("event_time"), "5 minutes", "1 minute")
).agg(
    sum("fare_amount").alias("total_fare_amount")
)

final_df = windowed_df.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("total_fare_amount")
)

def write_batch(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"outputs/task_3/batch_{batch_id}")

query = final_df.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "checkpoints/task3") \
    .start()

query.awaitTermination()