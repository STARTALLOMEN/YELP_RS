from processing.silver import tip_transform as tt

def test_clean_tip(spark):
    data = [("u1","b1"," Nice place ","2024-02-01 12:00:00",3)]
    df = spark.createDataFrame(data, ["user_id","business_id","text","date","compliment_count"])
    df2 = tt.clean(df)
    row = df2.first()
    assert row.text == 'Nice place'
    assert 'tip_ts' in df2.columns
