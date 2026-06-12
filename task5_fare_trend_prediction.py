from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, hour, minute
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
import os
import shutil

spark = SparkSession.builder.appName("Task5FareTrendPrediction").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Train model from static CSV
training_df = spark.read.csv(
    "training-dataset.csv",
    header=True,
    inferSchema=True
)

training_df = training_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

windowed_training = training_df.groupBy(
    window(col("timestamp"), "5 minutes")
).agg(
    avg("fare_amount").alias("avg_fare")
)

features_training = windowed_training.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "avg_fare"
).withColumn(
    "hour_of_day", hour(col("window_start"))
).withColumn(
    "minute_of_hour", minute(col("window_start"))
)

assembler = VectorAssembler(
    inputCols=["hour_of_day", "minute_of_hour"],
    outputCol="features"
)

training_features = assembler.transform(features_training)

lr = LinearRegression(
    featuresCol="features",
    labelCol="avg_fare"
)

model = lr.fit(training_features)

model_path = "models/fare_trend_model_v2"

if os.path.exists(model_path):
    shutil.rmtree(model_path)

model.save(model_path)

# Read streaming socket data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", IntegerType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

stream_raw = spark.readStream \
    .format("socket") \
    .option("host", "localhost") \
    .option("port", 9999) \
    .load()

stream_df = stream_raw.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

stream_df = stream_df.withColumn("timestamp", col("timestamp").cast("timestamp"))

windowed_stream = stream_df.groupBy(
    window(col("timestamp"), "5 minutes")
).agg(
    avg("fare_amount").alias("avg_fare")
)

stream_features = windowed_stream.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "avg_fare"
).withColumn(
    "hour_of_day", hour(col("window_start"))
).withColumn(
    "minute_of_hour", minute(col("window_start"))
)

saved_model = LinearRegressionModel.load(model_path)

stream_features = assembler.transform(stream_features)

predictions = saved_model.transform(stream_features)

results = predictions.select(
    "window_start",
    "window_end",
    "avg_fare",
    col("prediction").alias("predicted_next_avg_fare")
)

query = results.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()