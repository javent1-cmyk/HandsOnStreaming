from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, abs
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
import os
import shutil

spark = SparkSession.builder.appName("Task4FarePrediction").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# -----------------------------
# 1. Train model from CSV
# -----------------------------
training_df = spark.read.csv(
    "training-dataset.csv",
    header=True,
    inferSchema=True
)

assembler = VectorAssembler(
    inputCols=["distance_km"],
    outputCol="features"
)

training_features = assembler.transform(training_df)

lr = LinearRegression(
    featuresCol="features",
    labelCol="fare_amount"
)

model = lr.fit(training_features)

model_path = "models/fare_model"

if os.path.exists(model_path):
    shutil.rmtree(model_path)

model.save(model_path)

# -----------------------------
# 2. Read streaming socket data
# -----------------------------
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

# -----------------------------
# 3. Load model and predict
# -----------------------------
saved_model = LinearRegressionModel.load(model_path)

stream_features = assembler.transform(stream_df)

predictions = saved_model.transform(stream_features)

results = predictions.withColumn(
    "deviation",
    abs(col("fare_amount") - col("prediction"))
).select(
    "trip_id",
    "driver_id",
    "distance_km",
    "fare_amount",
    col("prediction").alias("predicted_fare"),
    "deviation"
)

# -----------------------------
# 4. Print to console
# -----------------------------
query = results.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()