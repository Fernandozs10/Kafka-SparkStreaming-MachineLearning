from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

# 1. Crear sesión Spark con soporte Kafka y S3
spark = (
    SparkSession.builder
        .appName("KafkaRidePrediction")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 2. Ruta  del bucket
model_path = "s3://mybuckbig123/models/taxi_price_model_rf/"
print("Cargando modelo desde:", model_path)
model = PipelineModel.load(model_path)
print("Modelo cargado correctamente")

# 3. Definir el esquema del JSON
schema = StructType([
    StructField("cab_type", StringType()),
    StructField("short_summary", StringType()),
    StructField("name", StringType()),
    StructField("distance", DoubleType()),
    StructField("hour", IntegerType()),
    StructField("day", IntegerType()),
    StructField("month", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("precipIntensity", DoubleType()),
    StructField("surge_multiplier", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])

# 4. Kafka (stream)
kafka_bootstrap = (
    "b-1.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092,"
    "b-2.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092,"
    "b-3.democluster1.du8hev.c19.kafka.us-east-1.amazonaws.com:9092"
)

df = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", "rides_data")
        .option("startingOffsets", "latest")
        .load()
)

json_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

# 5. Aplicar modelo ML
pred_df = model.transform(json_df)

output_df = pred_df.select(
    to_json(
        struct(
            col("cab_type"),
            col("short_summary"),
            col("name"),
            col("distance"),
            col("hour"),
            col("day"),
            col("month"),
            col("temperature"),
            col("precipIntensity"),
            col("surge_multiplier"),
            col("latitude"),
            col("longitude"),
            col("prediction").alias("predicted_price")
        )
    ).alias("value")
)

# 6. Enviar resultados a Kafka
checkpoint_path = "s3://mybuckbig123/models/checkpoints/rides_predictions/"

query = (
    output_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("topic", "rides_predictions")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start()
)

print(" Streaming iniciado. Esperando datos...")
query.awaitTermination()
