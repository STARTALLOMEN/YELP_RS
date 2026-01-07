from processing.silver import user_transform as ut

def test_basic_clean_user(spark):
    data = [("u1"," Alice ",10,"2020-01",1,0,0,"2020,2021",2,4.2,0,0,0,0,0,0,0,0,0,0,0)]
    cols = ["user_id","name","review_count","yelping_since","useful","funny","cool","elite","fans","average_stars","compliment_hot","compliment_more","compliment_profile","compliment_cute","compliment_list","compliment_note","compliment_plain","compliment_cool","compliment_funny","compliment_writer","compliment_photos"]
    df = spark.createDataFrame(data, cols)
    df2 = ut.basic_clean(df)
    row = df2.first()
    assert row.name == 'alice'
    assert row.elite_years == 2
    dfv = ut.validate(df2)
    assert dfv.count() == 1
