"""Review Silver Layer Transformation."""
from __future__ import annotations
import os
import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import Imputer
from processing.common.spark_session import get_spark
import pathlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH = os.getenv("BRONZE_REVIEW_PATH", "./data/bronze/yelp_academic_dataset_review.json")
SILVER_PATH = os.getenv("SILVER_REVIEW_PATH", "./data/silver/review")
BATCH_ID = os.getenv("BATCH_ID", "manual_0001")
SOURCE_NAME = os.getenv("SOURCE_NAME", "yelp_raw")


def load_review_data(path: str, infer_schema: bool = False):
    from pyspark.sql import SparkSession
    spark = get_spark()
    if infer_schema:
        return spark.read.json(path)
    schema = StructType([
        StructField("review_id", StringType()),
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("stars", DoubleType()),
        StructField("useful", IntegerType()),
        StructField("funny", IntegerType()),
        StructField("cool", IntegerType()),
        StructField("text", StringType()),
        StructField("date", StringType())
    ])
    return spark.read.json(path, schema=schema)


def basic_clean(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("text", F.trim(F.col("text")))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_source", F.lit(SOURCE_NAME)))


def handle_missing(df: DataFrame) -> DataFrame:
    imputer = Imputer(inputCols=["stars"], outputCols=["stars_imputed"]).setStrategy("median")
    df = imputer.fit(df).transform(df)
    return df.na.fill({"text": "", "useful": 0, "funny": 0, "cool": 0})


def validate(df: DataFrame) -> DataFrame:
    return df.filter((F.length("review_id") > 0) & (F.col("stars_imputed").between(1,5)))


def add_derived(df: DataFrame) -> DataFrame:
    return df.withColumn("text_length", F.length(F.col("text")))


def write_delta(df: DataFrame, path: str):
    (df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy("business_id")
        .save(path))


def write_delta_incremental(df: DataFrame, path: str):
    spark = df.sparkSession
    from delta.tables import DeltaTable
    fs_path = pathlib.Path(path)
    if not fs_path.exists():
        (df.write.format("delta")
         .option("mergeSchema", "true")
         .mode("overwrite")
         .partitionBy("business_id")
         .save(path))
        return
    delta_tbl = DeltaTable.forPath(spark, path)
    (delta_tbl.alias('t').merge(df.alias('s'), 't.review_id = s.review_id')
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())


def run_pipeline():
    logging.info("Loading review bronze data")
    df = load_review_data(BRONZE_PATH)
    logging.info(f"Bronze review count: {df.count()}")
    df = basic_clean(df)
    df = handle_missing(df)
    df = add_derived(df)
    df = validate(df)
    logging.info(f"Final review rows: {df.count()}")
    write_delta_incremental(df, SILVER_PATH)
    logging.info("Review silver write complete")


if __name__ == "__main__":
    run_pipeline()
