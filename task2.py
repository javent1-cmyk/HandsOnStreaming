import os

os.environ["JAVA_HOME"] = "/Library/Java/JavaVirtualMachines/jdk-17.jdk/Contents/Home"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, sum
from pyspark.sql.types import StructType, StringType, DoubleType

spark = SparkSession.builder \
    .appName("Task2_DriverAggregations") \
    .config(
        "spark.sql.streaming.checkpointFileManagerClass",
        "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager"
    ) \
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

agg_df = parsed_df.groupBy("driver_id").agg(
    sum("fare_amount").alias("total_fare_amount"),
    avg("distance_km").alias("average_distance")
)

def write_batch(batch_df, batch_id):
    batch_df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(f"outputs/task_2/batch_{batch_id}")

query = agg_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "checkpoints/task2") \
    .start()

query.awaitTermination()