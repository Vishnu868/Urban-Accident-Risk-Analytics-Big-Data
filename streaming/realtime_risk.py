"""Spark Structured Streaming: score live Kafka traffic events with the trained model and
emit per-zone risk hotspots every window."""
import argparse

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("event_time", StringType()), StructField("zone", StringType()),
    StructField("lat", DoubleType()), StructField("lng", DoubleType()),
    StructField("hour", IntegerType()), StructField("day_of_week", IntegerType()),
    StructField("avg_speed_kmph", DoubleType()), StructField("vehicle_count", IntegerType()),
    StructField("temperature_c", DoubleType()), StructField("humidity", DoubleType()),
    StructField("visibility_km", DoubleType()), StructField("precipitation_mm", DoubleType()),
    StructField("weather", StringType()), StructField("social_incident_reports", IntegerType()),
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="traffic_events")
    ap.add_argument("--model", default="models/risk_rf")
    ap.add_argument("--window", default="1 minute")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("UrbanAccidentRisk-Stream").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    model = PipelineModel.load(args.model)

    events = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", args.bootstrap)
              .option("subscribe", args.topic).load()
              .select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
              .withColumn("event_time", F.to_timestamp("event_time")))

    scored = (model.transform(events)
              .withColumn("model_risk", vector_to_array("probability")[1])
              # fuse live signals the historical model has not seen: congestion + social reports
              .withColumn("congestion", F.greatest(F.lit(0.0), 1 - F.col("avg_speed_kmph") / 50))
              .withColumn("risk_score", F.least(F.lit(1.0),
                          0.6 * F.col("model_risk") + 0.25 * F.col("congestion")
                          + 0.15 * F.least(F.col("social_incident_reports") / 3, F.lit(1.0)))))

    hotspots = (scored.withWatermark("event_time", "2 minutes")
                .groupBy(F.window("event_time", args.window), "zone")
                .agg(F.round(F.avg("risk_score"), 3).alias("avg_risk"),
                     F.count("*").alias("events"),
                     F.round(F.avg("avg_speed_kmph"), 1).alias("avg_speed"))
                .withColumn("alert", F.when(F.col("avg_risk") > 0.6, "HIGH")
                            .when(F.col("avg_risk") > 0.35, "MEDIUM").otherwise("LOW")))

    (hotspots.writeStream.outputMode("update").format("console")
     .option("truncate", False).start().awaitTermination())


if __name__ == "__main__":
    main()
