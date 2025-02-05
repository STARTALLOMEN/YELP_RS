from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from delta import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'yelp_recommendation_pipeline',
    default_args=default_args,
    description='Process Silver to Gold and train model weekly',
    schedule_interval='@weekly',
    catchup=False
)

# Task 1: Process Business Data to Gold
process_business_gold = SparkSubmitOperator(
    task_id='process_business_gold',
    application='scripts/business_to_gold.py',
    conn_id='spark_default',
    dag=dag
)

# Task 2: Process Reviews Data to Gold
process_reviews_gold = SparkSubmitOperator(
    task_id='process_reviews_gold',
    application='scripts/reviews_to_gold.py',
    conn_id='spark_default',
    dag=dag
)

# Task 3: Process Users Data to Gold
process_users_gold = SparkSubmitOperator(
    task_id='process_users_gold',
    application='scripts/users_to_gold.py',
    conn_id='spark_default',
    dag=dag
)

# Task 4: Create Training Dataset
def create_training_dataset():
    spark = SparkSession.builder \
        .appName("Create Training Dataset") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0") \
        .getOrCreate()
    
    # Load Gold data
    business_features = spark.read.format("delta").load("delta_lake/gold/business_features")
    user_features = spark.read.format("delta").load("delta_lake/gold/user_features")
    review_features = spark.read.format("delta").load("delta_lake/gold/review_features")
    
    # Join and prepare training data
    training_data = review_features \
        .join(business_features, "business_id") \
        .join(user_features, "user_id")
    
    # Save training dataset
    training_data.write.format("delta") \
        .mode("overwrite") \
        .save("delta_lake/gold/training_data")

create_training_task = PythonOperator(
    task_id='create_training_dataset',
    python_callable=create_training_dataset,
    dag=dag
)

# Task 5: Train Model
def train_model():
    spark = SparkSession.builder \
        .appName("Train Recommendation Model") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0") \
        .getOrCreate()
    
    # Load training data
    training_data = spark.read.format("delta").load("delta_lake/gold/training_data")
    
    # Initialize ALS model
    als = ALS(
        maxIter=5,
        regParam=0.01,
        userCol="user_id",
        itemCol="business_id",
        ratingCol="stars",
        coldStartStrategy="drop"
    )
    
    # Train model
    model = als.fit(training_data)
    
    # Save model
    model.write().overwrite().save("models/recommendation_model")

train_model_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    dag=dag
)

# Task 6: Evaluate Model
def evaluate_model():
    spark = SparkSession.builder \
        .appName("Evaluate Model") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:3.1.0") \
        .getOrCreate()
    
    # Load test data and model
    test_data = spark.read.format("delta").load("delta_lake/gold/test_data")
    model = ALS.load("models/recommendation_model")
    
    # Make predictions
    predictions = model.transform(test_data)
    
    # Calculate metrics
    from pyspark.ml.evaluation import RegressionEvaluator
    evaluator = RegressionEvaluator(
        metricName="rmse", 
        labelCol="stars",
        predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    
    # Log metrics
    with open('metrics/model_metrics.txt', 'w') as f:
        f.write(f"RMSE: {rmse}")

evaluate_model_task = PythonOperator(
    task_id='evaluate_model',
    python_callable=evaluate_model,
    dag=dag
)

# Define task dependencies
[process_business_gold, process_reviews_gold, process_users_gold] >> create_training_task >> train_model_task >> evaluate_model_task
