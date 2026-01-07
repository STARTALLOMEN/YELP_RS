from pyspark.sql import SparkSession
import os
import sys

# Minimal Spark Config
# No Delta, No fancy stuff
os.environ['HADOOP_HOME'] = "C:\\hadoop"

def test_basic():
    print("Testing Basic Spark Session...")
    try:
        spark = SparkSession.builder \
            .appName("BasicTest") \
            .master("local[*]") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()
            
        print("Spark Session Created Successfully!")
        print(f"Version: {spark.version}")
        
        data = [("Alice", 1), ("Bob", 2)]
        df = spark.createDataFrame(data, ["Name", "Value"])
        df.show()
        
        spark.stop()
        print("Spark Stopped.")
    except Exception as e:
        print("FAILED.")
        print(e)

if __name__ == "__main__":
    test_basic()
