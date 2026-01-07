import os
from processing.silver import business_transform as bt

def test_standardize_data(spark):
    data = [("b1", " Cafe ABC ", " New York ", "ny", "10001", 4.5, 10)]
    cols = ["business_id", "name", "city", "state", "postal_code", "stars", "review_count"]
    df = spark.createDataFrame(data, cols)
    result = bt.standardize_data(df)
    row = result.first()
    assert row.name == 'cafe abc'
    assert row.city == 'new york'
    assert row.state == 'NY'
    assert row.postal_code == '10001'

def test_handle_outliers(spark):
    data = [(f"b{i}", 4.0, i*10) for i in range(1, 21)] + [("b999", 5.0, 9999)]
    cols = ["business_id", "stars", "review_count"]
    df = spark.createDataFrame(data, cols)
    filtered = bt.handle_outliers(df)
    ids = [r.business_id for r in filtered.collect()]
    assert "b999" not in ids

def test_validate_data(spark):
    data = [("b1", 4.0, 10, 40.0, -73.0, 5), ("", 4.0, 10, 40.0, -73.0, 5)]
    cols = ["business_id", "stars", "review_count", "latitude", "longitude", "review_count"]
    df = spark.createDataFrame([(d[0], d[1], d[2], d[3], d[4]) for d in data], ["business_id", "stars", "review_count", "latitude", "longitude"])
    valid = bt.validate_data(df)
    assert valid.count() == 1
