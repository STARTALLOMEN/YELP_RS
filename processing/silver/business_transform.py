"""Business Silver Layer Transformation Module.
Extracted from exploratory notebook.
"""
from __future__ import annotations
import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import Imputer
from delta import *

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

APP_NAME = os.getenv("APP_NAME", "Business to Silver")
DELTA_VERSION = os.getenv("DELTA_VERSION", "io.delta:delta-core_2.12:3.1.0")
BRONZE_PATH = os.getenv("BRONZE_BUSINESS_PATH", "./data/bronze/yelp_academic_dataset_business.json")
SILVER_PATH = os.getenv("SILVER_BUSINESS_PATH", "./data/silver/business")
BATCH_ID = os.getenv("BATCH_ID", "manual_0001")
SOURCE_NAME = os.getenv("SOURCE_NAME", "yelp_raw")

ATTRIBUTE_LIST = [
    "AcceptsInsurance", "AgesAllowed", "Alcohol", "Ambience", "BYOB", 
    "BYOBCorkage", "BestNights", "BikeParking", "BusinessAcceptsBitcoin", 
    "BusinessAcceptsCreditCards", "BusinessParking", "ByAppointmentOnly", 
    "Caters", "CoatCheck", "Corkage", "DietaryRestrictions", "DogsAllowed", 
    "DriveThru", "GoodForDancing", "GoodForKids", "GoodForMeal", 
    "HairSpecializesIn", "HappyHour", "HasTV", "Music", "NoiseLevel", 
    "Open24Hours", "OutdoorSeating", "RestaurantsAttire", "RestaurantsCounterService", 
    "RestaurantsDelivery", "RestaurantsGoodForGroups", "RestaurantsPriceRange2", 
    "RestaurantsReservations", "RestaurantsTableService", "RestaurantsTakeOut", 
    "Smoking", "WheelchairAccessible", "WiFi"
]

HOURS_LIST = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

def get_spark() -> SparkSession:
    builder = (SparkSession.builder
               .appName(APP_NAME)
               .config("spark.jars.packages", DELTA_VERSION)
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return configure_spark_with_delta_pip(builder).getOrCreate()

def load_business_data(spark: SparkSession, path: str, infer_schema: bool = False) -> DataFrame:
    if not path:
        raise ValueError("Path required")
    if infer_schema:
        return spark.read.json(path)
    schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("stars", DoubleType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("is_open", IntegerType(), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
        StructField("categories", StringType(), True),
        StructField("hours", MapType(StringType(), StringType()), True)
    ])
    return spark.read.json(path, schema=schema)

def extract_attributes(df: DataFrame) -> DataFrame:
    if "attributes" not in df.columns:
        return df
    exprs = [col(c) for c in df.columns]
    for attr in ATTRIBUTE_LIST:
        exprs.append(col("attributes").getItem(attr).alias(f"attr_{attr}"))
    return df.select(*exprs)

def process_business_data(df: DataFrame) -> DataFrame:
    if "attributes" not in df.columns:
        return df
    dfp = df \
        .withColumn("business_parking_dict", from_json(col("attributes").getItem("BusinessParking"), MapType(StringType(), BooleanType()))) \
        .withColumn("ambience_dict", from_json(col("attributes").getItem("Ambience"), MapType(StringType(), BooleanType()))) \
        .withColumn("good_for_meal_dict", from_json(col("attributes").getItem("GoodForMeal"), MapType(StringType(), BooleanType())))
    for new_col, key in [("garage_parking","garage"),("street_parking","street"),("lot_parking","lot"),("valet_parking","valet")]:
        dfp = dfp.withColumn(new_col, col("business_parking_dict").getItem(key))
    for new_col, key in [("is_romantic","romantic"),("is_intimate","intimate"),("is_classy","classy"),("is_hipster","hipster")]:
        dfp = dfp.withColumn(new_col, col("ambience_dict").getItem(key))
    for new_col, key in [("good_for_dinner","dinner"),("good_for_lunch","lunch"),("good_for_breakfast","breakfast")]:
        dfp = dfp.withColumn(new_col, col("good_for_meal_dict").getItem(key))
    return dfp

def extract_hours(df: DataFrame) -> DataFrame:
    if "hours" not in df.columns:
        return df
    exprs = [col(c) for c in df.columns]
    for h in HOURS_LIST:
        exprs.append(col("hours").getItem(h).alias(f"hrs_{h}"))
    return df.select(*exprs)

def handle_missing_values(df: DataFrame) -> DataFrame:
    imputer = Imputer(inputCols=["stars", "review_count"], outputCols=["stars_imputed", "review_count_imputed"]).setStrategy("median")
    df = imputer.fit(df).transform(df)
    return df.na.fill({"is_open": 0, "name": "unknown", "city": "unknown", "state": "UN", "categories": "uncategorized"})

def remove_duplicates(df: DataFrame) -> DataFrame:
    return df.dropDuplicates(["business_id"])

def standardize_data(df: DataFrame) -> DataFrame:
    return (df
        .withColumn("name", trim(lower(col("name"))))
        .withColumn("city", trim(lower(col("city"))))
        .withColumn("state", regexp_replace(upper(col("state")), "[^A-Z]", ""))
        .withColumn("categories", trim(lower(col("categories"))))
        .withColumn("postal_code", regexp_replace(col("postal_code"), "[^0-9]", ""))
        .withColumn("_ingest_ts", current_timestamp())
        .withColumn("_batch_id", lit(BATCH_ID))
        .withColumn("_source", lit(SOURCE_NAME))
    )

def handle_outliers(df: DataFrame) -> DataFrame:
    if df.select("stars").where(col("stars").isNotNull()).count() == 0:
        return df
    stats = df.select(
        percentile_approx("stars", 0.25).alias("q1_stars"),
        percentile_approx("stars", 0.75).alias("q3_stars"),
        percentile_approx("review_count", 0.25).alias("q1_reviews"),
        percentile_approx("review_count", 0.75).alias("q3_reviews")
    ).collect()[0]
    iqr_stars = stats.q3_stars - stats.q1_stars
    iqr_reviews = stats.q3_reviews - stats.q1_reviews
    return df.filter(
        (col("stars").between(stats.q1_stars - 1.5 * iqr_stars, stats.q3_stars + 1.5 * iqr_stars)) &
        (col("review_count").between(stats.q1_reviews - 1.5 * iqr_reviews, stats.q3_reviews + 1.5 * iqr_reviews))
    )

def validate_data(df: DataFrame) -> DataFrame:
    df = df.filter((col("stars").between(1,5)) & (col("latitude").between(-90,90)) & (col("longitude").between(-180,180)) & (col("review_count") >= 0))
    df = df.filter((length(col("business_id")) > 0) & (length(col("postal_code")).between(3,10)))
    return df

def write_delta(df: DataFrame, path: str) -> None:
    (df.write.format("delta")
        .option("mergeSchema", "true")
        .mode("overwrite")
        .partitionBy("state")
        .save(path))

def write_delta_incremental(df: DataFrame, path: str) -> None:
    spark = df.sparkSession
    if not spark._jsparkSession.catalog().tableExists(path):
        (df.write.format("delta")
            .option("mergeSchema", "true")
            .mode("overwrite")
            .partitionBy("state")
            .save(path))
        return
    # MERGE incremental (assumes business_id natural key)
    from delta.tables import DeltaTable
    delta_tbl = DeltaTable.forPath(spark, path)
    (delta_tbl.alias('t').merge(df.alias('s'), 't.business_id = s.business_id')
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())

def run_pipeline():
    spark = get_spark()
    logging.info("Loading bronze data...")
    df = load_business_data(spark, BRONZE_PATH)
    logging.info(f"Bronze count: {df.count()}")
    df = extract_attributes(df)
    df = process_business_data(df)
    df = extract_hours(df)
    df = handle_missing_values(df)
    df = remove_duplicates(df)
    df = standardize_data(df)
    df = handle_outliers(df)
    df = validate_data(df)
    logging.info(f"Final row count: {df.count()} | Columns: {len(df.columns)}")
    write_delta_incremental(df, SILVER_PATH)
    logging.info("Write complete.")

if __name__ == "__main__":
    run_pipeline()
