"""Shared Spark session & utility helpers."""
from __future__ import annotations
import os
from pyspark.sql import SparkSession
from delta import *

APP_NAME = os.getenv("APP_NAME", "YELP_RS Processing")
DELTA_VERSION = os.getenv("DELTA_VERSION", "io.delta:delta-core_2.12:3.1.0")

DEFAULT_CONFIGS = {
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.databricks.delta.optimizeWrite.enabled": "true",
    "spark.databricks.delta.autoCompact.enabled": "true",
    "spark.sql.shuffle.partitions": os.getenv("SPARK_SHUFFLE_PARTITIONS", "200")
}


def get_spark(extra_configs: dict | None = None) -> SparkSession:
    builder = (SparkSession.builder
               .appName(APP_NAME)
               .config("spark.jars.packages", DELTA_VERSION)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    # apply defaults
    for k, v in DEFAULT_CONFIGS.items():
        builder = builder.config(k, v)
    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)
    return configure_spark_with_delta_pip(builder).getOrCreate()


def optimize_table(spark: SparkSession, path: str):
    # Placeholder: In open-source Delta, manual OPTIMIZE not available; simulate compaction via repartition + overwrite
    df = spark.read.format("delta").load(path)
    df.coalesce(1).write.format("delta").mode("overwrite").option("mergeSchema", "true").save(path)
