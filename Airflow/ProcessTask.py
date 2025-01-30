import os
import time
import random
from datetime import datetime, timedelta

import requests
from bs4 import BeautifulSoup

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import udf, col
from pyspark.sql.types import MapType, StringType

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def get_random_delay():
    return random.uniform(5, 7)

def generate_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept-Language': 'en-US, en;q=0.5',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://www.amazon.com/'
    }

def get_title(soup):
    try:
        title_element = soup.find("a", class_="a-link-normal s-line-clamp-4 s-link-style a-text-normal")
        if title_element:
            h2_element = title_element.find("h2", class_="a-size-base-plus a-spacing-none a-color-base a-text-normal")
            if h2_element:
                span_element = h2_element.find("span")
                return span_element.get_text(strip=True) if span_element else ""
        return ""
    except AttributeError:
        return ""

def get_price(soup):
    try:
        return soup.find('span', class_='a-price').find('span', class_='a-offscreen').text.strip()
    except AttributeError:
        return ""

def get_old_price(soup):
    try:
        return soup.find("div", class_="a-section aok-inline-block").find("span", class_="a-offscreen").text.strip()
    except AttributeError:
        return ""

def get_product_url(soup):
    try:
        return "https://www.amazon.com" + soup.find("a", class_="a-link-normal s-line-clamp-4 s-link-style a-text-normal")['href']
    except (TypeError, AttributeError):
        return ""

def get_rating(soup):
    try:
        return soup.find('i', class_='a-icon a-icon-star-small a-star-small-4-5').find('span', class_='a-icon-alt').text.strip()
    except (AttributeError, IndexError):
        return ""

def get_review_count(soup):
    try:
        return soup.find('span', class_='a-size-base s-underline-text').text.strip()
    except AttributeError:
        return ""

def get_purchase_count(soup):
    try:
        return soup.find("span", class_="a-size-base a-color-secondary").get_text(strip=True)
    except AttributeError:
        return ""

def flatten_product_details(details):
    return {
        'detail_' + key.lower().replace(' ', '_').replace('/', '_'): value 
        for key, value in details.items()
    }

def get_product_details(product_url):
    try:
        headers = generate_headers()
        
        with requests.Session() as session:
            page = session.get(product_url, headers=headers, timeout=10)
        
        soup = BeautifulSoup(page.content, "html.parser")

        details = {}
        info_sections = [
            soup.find("div", class_="a-section a-spacing-small a-spacing-top-small"),
            soup.find("div", id="productDetails_techSpec_section_1")
        ]
        
        style_section = soup.find("div", id="variation_style_name")
        if style_section:
            try:
                style_label = style_section.find("label", class_="a-form-label")
                style_value = style_section.find("span", class_="selection")
                if style_label and style_value:
                    details['Style'] = style_value.get_text(strip=True)
            except AttributeError:
                pass
        
        for info_section in info_sections:
            if info_section:
                table = info_section.find("table")
                if table:
                    for row in table.find_all("tr"):
                        try:
                            key = row.find("td", class_="a-span3").get_text(strip=True)
                            value = row.find("td", class_="a-span9").get_text(strip=True)
                            details[key] = value
                        except AttributeError:
                            continue
        
        try:
            description = soup.find("div", id="productDescription").get_text(strip=True)
            details['Description'] = description
        except AttributeError:
            pass
        
        try:
            brand = soup.find("span", class_="a-size-large a-color-base").get_text(strip=True)
            details['Brand'] = brand
        except AttributeError:
            pass

    except requests.RequestException as e:
        print(f"Error getting product details: {e}")
        details = {}

    return details

def scrape_amazon_products_task(**kwargs):
    """
    Airflow task to scrape Amazon products using PySpark
    """
    base_url = kwargs.get('base_url', "https://www.amazon.com/s?i=computers-intl-ship&bbn=16225007011&rh=n%3A16225007011%2Cn%3A1292110011")
    max_pages = kwargs.get('max_pages', 20)
    start_page = kwargs.get('start_page', 1)
    fetch_details = kwargs.get('fetch_details', True)

    # Create Spark Session
    spark = SparkSession.builder \
        .appName("AmazonProductScraper") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

    # Create output directory
    output_dir = "/opt/airflow/data/amazon_scraper_output"
    os.makedirs(output_dir, exist_ok=True)

    # Create schema for product data
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("price", StringType(), True),
        StructField("old_price", StringType(), True),
        StructField("product_url", StringType(), True),
        StructField("rating", StringType(), True),
        StructField("reviews", StringType(), True),
        StructField("purchases", StringType(), True)
    ])

    # Initialize list to store products
    all_products = []

    # Create session
    session = requests.Session()

    # Loop through pages
    for page in range(start_page, start_page + max_pages):
        # Dynamically build URL
        if 'page=' in base_url:
            url = base_url.replace(f'page={start_page-1}', f'page={page}')
        else:
            url = f"{base_url}&page={page}"
        
        print(f"Crawling page {page} - URL: {url}")
        
        try:
            # Use dynamic headers
            headers = generate_headers()
            response = session.get(url, headers=headers, timeout=10)
            
            # Check status code
            if response.status_code != 200:
                print(f"Cannot load page {page}. Status code: {response.status_code}")
                break
            
            # Parse HTML
            soup = BeautifulSoup(response.content, "html.parser")
            product_elements = soup.find_all("div", attrs={"data-asin": True})
            
            # Check if no products found
            if not product_elements:
                print(f"No products found on page {page}. Stopping scraping.")
                break

            # Scrape each product
            for product in product_elements:
                try:
                    title = get_title(product)
                    price = get_price(product)
                    old_price = get_old_price(product)
                    product_url = get_product_url(product)
                    rating = get_rating(product)
                    reviews = get_review_count(product)
                    purchases = get_purchase_count(product)
                    
                    # Get product details if requested
                    additional_details = {}
                    if fetch_details and product_url:
                        try:
                            details = get_product_details(product_url)
                            additional_details = flatten_product_details(details)
                        except Exception as e:
                            print(f"Error getting details for product {title}: {e}")
                    
                    # Combine product info
                    product_info = {
                        "title": title,
                        "price": price,
                        "old_price": old_price,
                        "product_url": product_url,
                        "rating": rating,
                        "reviews": reviews,
                        "purchases": purchases,
                        **additional_details
                    }
                    
                    # Keep only non-empty title entries
                    if title:
                        all_products.append(product_info)
                
                except Exception as e:
                    print(f"Error processing product: {e}")

            # Random delay between requests
            delay = get_random_delay()
            print(f"Waiting {delay:.2f} seconds before crawling next page...")
            time.sleep(delay)

        except requests.RequestException as e:
            print(f"Error crawling page {page}: {e}")
            break

    # Convert products to Spark DataFrame
    df = spark.createDataFrame(all_products)
    
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Output paths
    output_file_csv = os.path.join(output_dir, f"amazon_products_{timestamp}.csv")
    output_file_json = os.path.join(output_dir, f"amazon_products_{timestamp}.json")
    
    # Save CSV
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_file_csv)
    
    # Save JSON
    df.coalesce(1).write.mode("overwrite").json(output_file_json)
    
    print(f"Scraping completed. Total products: {df.count()}")
    print(f"CSV data saved to: {output_file_csv}")
    print(f"JSON data saved to: {output_file_json}")
    
    return output_file_csv  # Return file path for use in subsequent tasks

# Default parameters for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
    'amazon_product_scraper_pyspark',
    default_args=default_args,
    description='Scrape Amazon product listings with PySpark',
    schedule_interval=timedelta(days=7),  # Run weekly
    catchup=False
) as dag:
    
    scrape_products = PythonOperator(
        task_id='scrape_amazon_products',
        python_callable=scrape_amazon_products_task,
        op_kwargs={
            'base_url': "https://www.amazon.com/s?i=computers-intl-ship&bbn=16225007011&rh=n%3A16225007011%2Cn%3A1292110011",
            'max_pages': 20,
            'start_page': 1,
            'fetch_details': True
        },
        dag=dag,
    )

    scrape_products