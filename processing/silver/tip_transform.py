"""Tip Silver Layer Transformation."""
from __future__ import annotations
import os, logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from processing.common.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH = os.getenv("BRONZE_TIP_PATH", "./data/bronze/yelp_academic_dataset_tip.json")
SILVER_PATH = os.getenv("SILVER_TIP_PATH", "./data/silver/tip")
BATCH_ID = os.getenv("BATCH_ID", "manual_0001")
SOURCE_NAME = os.getenv("SOURCE_NAME", "yelp_raw")


def load_tip_data(path: str, infer_schema: bool = False):
    spark = get_spark()
    if infer_schema:
        return spark.read.json(path)
    schema = StructType([
        StructField("user_id", StringType()),
        StructField("business_id", StringType()),
        StructField("text", StringType()),
        StructField("date", StringType()),
        StructField("compliment_count", IntegerType())
    ])
    return spark.read.json(path, schema=schema)


def clean(df):
    return (df
            .withColumn("text", F.trim(F.col("text")))
            .withColumn("tip_ts", F.to_timestamp(F.col("date")))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_source", F.lit(SOURCE_NAME)))


def write_delta(df, path: str):
    (df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy("business_id")
        .save(path))


def write_delta_incremental(df, path: str):
    spark = df.sparkSession
    from delta.tables import DeltaTable
    if not os.path.exists(path):
        (df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .partitionBy("business_id")
            .save(path))
        return
    tbl = DeltaTable.forPath(spark, path)
    (tbl.alias('t').merge(df.alias('s'), 't.user_id = s.user_id AND t.business_id = s.business_id AND t.tip_ts = s.tip_ts')
        .whenNotMatchedInsertAll()
        .execute())


def run_pipeline():
    logging.info("Loading tip bronze data")
    df = load_tip_data(BRONZE_PATH)
    logging.info(f"Tip bronze count: {df.count()}")
    df = clean(df)
    logging.info(f"Tip final rows: {df.count()}")
    write_delta_incremental(df, SILVER_PATH)
    logging.info("Tip silver write complete")

if __name__ == "__main__":
    run_pipeline()
