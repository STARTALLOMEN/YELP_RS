"""Checkin Silver Layer Transformation."""
from __future__ import annotations
import os, logging
from pyspark.sql import functions as F
from pyspark.sql.types import *
from processing.common.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

BRONZE_PATH = os.getenv("BRONZE_CHECKIN_PATH", "./data/bronze/yelp_academic_dataset_checkin.json")
SILVER_PATH = os.getenv("SILVER_CHECKIN_PATH", "./data/silver/checkin")
BATCH_ID = os.getenv("BATCH_ID", "manual_0001")
SOURCE_NAME = os.getenv("SOURCE_NAME", "yelp_raw")


def load_checkin_data(path: str, infer_schema: bool = False):
    spark = get_spark()
    if infer_schema:
        return spark.read.json(path)
    schema = StructType([
        StructField("business_id", StringType()),
        StructField("date", StringType())
    ])
    return spark.read.json(path, schema=schema)


def explode_checkins(df):
    # date string contains comma-separated timestamps
    df = df.withColumn("checkin_ts", F.explode(F.split(F.col("date"), ",")))
    return df.withColumn("checkin_ts", F.to_timestamp(F.trim(F.col("checkin_ts"))))


def add_time_dimensions(df):
    return (df
            .withColumn("checkin_date", F.to_date(F.col("checkin_ts")))
            .withColumn("checkin_hour", F.hour(F.col("checkin_ts")))
            .withColumn("_ingest_ts", F.current_timestamp())
            .withColumn("_batch_id", F.lit(BATCH_ID))
            .withColumn("_source", F.lit(SOURCE_NAME)))


def write_delta(df, path: str):
    (df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy("checkin_date")
        .save(path))


def write_delta_incremental(df, path: str):
    spark = df.sparkSession
    from delta.tables import DeltaTable
    if not os.path.exists(path):
        (df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .partitionBy("checkin_date")
            .save(path))
        return
    tbl = DeltaTable.forPath(spark, path)
    (tbl.alias('t').merge(df.alias('s'), 't.business_id = s.business_id AND t.checkin_ts = s.checkin_ts')
        .whenNotMatchedInsertAll()
        .execute())


def run_pipeline():
    logging.info("Loading checkin bronze data")
    df = load_checkin_data(BRONZE_PATH)
    logging.info(f"Checkin bronze count: {df.count()}")
    df = explode_checkins(df)
    df = add_time_dimensions(df)
    logging.info(f"Checkin final rows: {df.count()}")
    write_delta_incremental(df, SILVER_PATH)
    logging.info("Checkin silver write complete")

if __name__ == "__main__":
    run_pipeline()
