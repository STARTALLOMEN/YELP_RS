from processing.silver import checkin_transform as ct
from pyspark.sql import functions as F

def test_explode_and_time_dimensions(spark):
    data = [("b1","2024-01-01 10:00:00,2024-01-01 11:00:00")] 
    df = spark.createDataFrame(data, ["business_id","date"])
    df2 = ct.explode_checkins(df)
    assert df2.count() == 2
    df3 = ct.add_time_dimensions(df2)
    assert 'checkin_hour' in df3.columns
