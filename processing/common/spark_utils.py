"""
Common utilities for Spark and Data Processing.
Does not contain business logic.
"""
import os
import sys
import logging
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_spark_session(app_name: str = "Yelp_RS_App", memory: str = "4g") -> SparkSession:
    
    # Check enviroment (Hadoop Home) for Windows
    if sys.platform.startswith('win'):
        java_home = os.environ.get('JAVA_HOME', '')
        if ' ' in java_home and os.path.exists(java_home):
            try:
                import win32api
                short_path = win32api.GetShortPathName(java_home)
                logging.info(f"Converting JAVA_HOME from '{java_home}' to '{short_path}'")
                os.environ['JAVA_HOME'] = short_path
            except ImportError:
                pass
        
        if not os.environ.get('HADOOP_HOME'):
            os.environ['HADOOP_HOME'] = "C:\\hadoop"

    # Configure Spark - MANUAL MODE (No configure_spark_with_delta_pip wrapper)
    # This mimics the raw configuration usually done in notebooks when wrapper fails
    # NOTE: Using exactly the package version from your notebook: io.delta:delta-core_2.12:3.1.0
    # But wait, delta-core is old name. Since 3.0 it's delta-spark. 
    # Your notebook said: "io.delta:delta-core_2.12:3.1.0" <- This might be the key! 
    # Configure Spark - Standardized Config based on successful Notebook test
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", memory) \
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.ui.enabled", "false")

    # Use Delta's utility function to handle JARs automatically
    try:
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as e:
        logging.error("Failed to create Spark Session. Check JAVA_HOME/HADOOP_HOME.")
        raise e
    
    spark.sparkContext.setLogLevel("WARN")
    
    return spark
