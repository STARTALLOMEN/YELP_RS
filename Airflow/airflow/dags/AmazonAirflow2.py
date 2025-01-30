import os
import time
import random
from datetime import datetime, timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def get_random_delay():
    return random.uniform(5, 7)

def generate_headers():
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:108.0) Gecko/20100101 Firefox/118.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
    ]
    return {
        'User-Agent': random.choice(user_agents),
        'Accept-Language': 'en-US, en;q=0.5',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Referer': 'https://www.amazon.com/'
    }

def flatten_product_details(details):
    flattened = {}
    for key, value in details.items():
        clean_key = 'detail_' + key.lower().replace(' ', '_').replace('/', '_')
        flattened[clean_key] = value
    return flattened

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

def scrape_amazon_products_task(**kwargs):
    """
    Airflow task to scrape Amazon products
    """
    base_url = kwargs.get('base_url', "https://www.amazon.com/s?i=computers-intl-ship&bbn=16225007011&rh=n%3A16225007011%2Cn%3A1292110011")
    max_pages = kwargs.get('max_pages', 50)
    start_page = kwargs.get('start_page', 70)
    fetch_details = kwargs.get('fetch_details', True)

    # Create session
    session = requests.Session()

    # Initialize list to store data
    products_data = []

    # Create output directory
    output_dir = "/opt/airflow/data/amazon_scraper_output"
    os.makedirs(output_dir, exist_ok=True)

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
                product_info = {
                    'title': get_title(product),
                    'price': get_price(product),
                    'old_price': get_old_price(product),
                    'product_url': get_product_url(product),
                    'rating': get_rating(product),
                    'reviews': get_review_count(product),
                    'purchases': get_purchase_count(product)
                }
                
                # Get product details if requested
                if fetch_details and product_info['product_url']:
                    try:
                        details = get_product_details(product_info['product_url'])
                        flattened_details = flatten_product_details(details)
                        product_info.update(flattened_details)
                    except Exception as e:
                        print(f"Error getting details for product {product_info['title']}: {e}")
                
                # Add product to list
                products_data.append(product_info)

            # Random delay between requests
            delay = get_random_delay()
            print(f"Waiting {delay:.2f} seconds before crawling next page...")
            time.sleep(delay)

        except requests.RequestException as e:
            print(f"Error crawling page {page}: {e}")
            break

    # Convert to DataFrame
    amazon_df = pd.DataFrame(products_data)
    
    # Remove rows without titles
    amazon_df = amazon_df[amazon_df['title'].notna()]
    
    # Create timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save CSV
    output_file_csv = os.path.join(output_dir, f"amazon_products_{timestamp}.csv")
    amazon_df.to_csv(output_file_csv, index=False, encoding='utf-8')
    
    # Save JSON
    output_file_json = os.path.join(output_dir, f"amazon_products_{timestamp}.json")
    amazon_df.to_json(output_file_json, orient='records', indent=2, force_ascii=False)
    
    print(f"Scraping completed. Total products: {len(amazon_df)}")
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
    'amazon_product_scraper2',
    default_args=default_args,
    description='Scrape Amazon product listings',
    schedule_interval=timedelta(days=7),  # Run weekly
    catchup=False
) as dag:
    
    scrape_products = PythonOperator(
        task_id='scrape_amazon_products2',
        python_callable=scrape_amazon_products_task,
        op_kwargs={
            'base_url': "https://www.amazon.com/s?i=computers-intl-ship&bbn=16225007011&rh=n%3A16225007011%2Cn%3A1292110011",
            'max_pages': 20,
            'start_page': 21,
            'fetch_details': True
        },
        dag=dag,
    )

    scrape_products