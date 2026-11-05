"""Spark batch job: clean and fuse historical accident + weather records, train an MLlib risk model.

Expected input columns (US-Accidents style, rename as needed):
  Start_Time, Start_Lat, Start_Lng, Severity, Temperature(F), Humidity(%), Visibility(mi),
  Precipitation(in), Weather_Condition
Severity >= 3 is treated as a high-risk accident.
"""
import argparse

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

NUMERIC = ["hour", "day_of_week", "temperature_c", "humidity", "visibility_km",
           "precipitation_mm", "lat", "lng"]


def load_and_fuse(spark, path):
    df = spark.read.csv(path, header=True, inferSchema=True)
    df = (df
          .withColumn("ts", F.to_timestamp("Start_Time"))
          .select(
              F.hour("ts").alias("hour"),
              F.dayofweek("ts").alias("day_of_week"),
              ((F.col("`Temperature(F)`") - 32) * 5 / 9).alias("temperature_c"),
              F.col("`Humidity(%)`").alias("humidity"),
              (F.col("`Visibility(mi)`") * 1.609).alias("visibility_km"),
              (F.coalesce(F.col("`Precipitation(in)`"), F.lit(0.0)) * 25.4).alias("precipitation_mm"),
              F.col("Start_Lat").alias("lat"), F.col("Start_Lng").alias("lng"),
              F.coalesce(F.col("Weather_Condition"), F.lit("Unknown")).alias("weather"),
              (F.col("Severity") >= 3).cast("int").alias("label"))
          .dropna(subset=NUMERIC + ["label"])
          .dropDuplicates())
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--model", default="models/risk_rf")
    ap.add_argument("--hive-table", default=None, help="optionally persist cleaned data to Hive")
    args = ap.parse_args()

    spark = (SparkSession.builder.appName("UrbanAccidentRisk-Train")
             .enableHiveSupport().getOrCreate())
    df = load_and_fuse(spark, args.data).cache()
    print(f"Clean records: {df.count():,}")
    if args.hive_table:
        df.write.mode("overwrite").saveAsTable(args.hive_table)

    # Hotspot summary (batch view)
    (df.withColumn("cell", F.concat_ws("_", F.round("lat", 2), F.round("lng", 2)))
       .groupBy("cell").agg(F.count("*").alias("accidents"), F.avg("label").alias("severe_rate"))
       .orderBy(F.desc("accidents")).show(10, truncate=False))

    pipe = Pipeline(stages=[
        StringIndexer(inputCol="weather", outputCol="weather_idx", handleInvalid="keep"),
        VectorAssembler(inputCols=NUMERIC + ["weather_idx"], outputCol="features"),
        RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=100,
                               maxDepth=10, maxBins=512, seed=42),
    ])
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    model = pipe.fit(train)
    pred = model.transform(test)
    auc = BinaryClassificationEvaluator(labelCol="label").evaluate(pred)
    f1 = MulticlassClassificationEvaluator(labelCol="label", metricName="f1").evaluate(pred)
    print(f"Test ROC-AUC: {auc:.4f} | F1: {f1:.4f}")
    model.write().overwrite().save(args.model)
    spark.stop()


if __name__ == "__main__":
    main()
