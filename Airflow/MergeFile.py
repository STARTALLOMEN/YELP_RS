# import os
# from datetime import timedelta
# from pyspark.sql import SparkSession
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.utils.dates import days_ago

# # Hàm merge CSV files sử dụng Spark
# def merge_csv_files_with_spark(input_directory, output_file='merged_output.csv', **kwargs):
#     # Khởi tạo SparkSession
#     spark = SparkSession.builder \
#         .appName("AmazonCSVFilesMerger") \
#         .getOrCreate()

#     # Đảm bảo thư mục đầu vào tồn tại
#     if not os.path.exists(input_directory):
#         print(f"Error: Directory {input_directory} does not exist.")
#         return 0

#     # Đọc tất cả các tệp CSV trong thư mục
#     csv_files = [os.path.join(input_directory, f) for f in os.listdir(input_directory) if f.endswith('.csv')]
    
#     if not csv_files:
#         print("No CSV files found in the directory.")
#         return 0

#     # Đọc và hợp nhất tất cả các tệp CSV
#     print(f"Found {len(csv_files)} CSV files. Merging...")
#     merged_df = spark.read.option("header", "true").csv(csv_files)

#     # Thêm cột theo dõi nguồn file (nếu cần)
#     merged_df = merged_df.withColumn("source_file", spark.functions.input_file_name())

#     # Tạo thư mục đầu ra nếu chưa tồn tại
#     output_directory = os.path.dirname(output_file) or '/opt/airflow/data/merged_output'
#     os.makedirs(output_directory, exist_ok=True)
#     full_output_path = os.path.join(output_directory, output_file)

#     # Ghi tệp hợp nhất ra CSV
#     merged_df.coalesce(1).write.option("header", "true").csv(full_output_path, mode="overwrite")

#     print(f"Merging complete!")
#     print(f"Output file: {full_output_path}")
#     print(f"Total rows in merged file: {merged_df.count()}")
    
#     # Dừng SparkSession
#     spark.stop()

#     return {
#         'merged_files_count': len(csv_files),
#         'output_file': full_output_path
#     }

# # Tham số mặc định cho DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# # Tạo DAG
# with DAG(
#     'amazon_csv_merger_with_spark',
#     default_args=default_args,
#     description='Merge Amazon product CSV files using Spark',
#     schedule_interval=timedelta(days=7),  # Chạy hàng tuần
#     catchup=False
# ) as dag:
    
#     merge_csv_task = PythonOperator(
#         task_id='merge_amazon_csv_files_with_spark',
#         python_callable=merge_csv_files_with_spark,
#         op_kwargs={
#             'input_directory': 'D:/app/Bigdata-IS405.P11/Crawl/Airflow/amazon_scraper_output',
#             'output_file': 'amazon_products_merged'
#         },
#         dag=dag,
#     )

#     merge_csv_task
