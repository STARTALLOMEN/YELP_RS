from processing.silver import review_transform as rt

def test_basic_clean_and_derived(spark):
    data = [("r1","u1","b1",4.0,1,0,0," Great food ","2024-01-01")] 
    cols = ["review_id","user_id","business_id","stars","useful","funny","cool","text","date"]
    df = spark.createDataFrame(data, cols)
    df2 = rt.basic_clean(df)
    assert df2.first().text == 'Great food'
    df3 = rt.handle_missing(df2)
    df4 = rt.add_derived(df3)
    assert 'text_length' in df4.columns
    dfv = rt.validate(df4)
    assert dfv.count() == 1
