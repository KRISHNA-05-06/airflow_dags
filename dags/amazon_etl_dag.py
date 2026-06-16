from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import random
import os
import time
import requests
from bs4 import BeautifulSoup

default_args = {
    "owner": "Sri Krishna Sai Kota",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

@dag(
    dag_id = "amazon_books_etl_pipeline",
    description = "Automated ETL pipeline to fetch and load Amazon Data Engineering book data into MySQL.",
    schedule = "@daily",
    start_date = datetime(2026, 6, 16),
    catchup = False,
    default_args = default_args,
    tags = ["amazon", "etl", "airflow"],
)
def amazon_books_etl():
    
    @task
    def get_amazon_data_books(num_books=50, max_pages=10, ti=None):
        """
        Extracts Amazon Data Engineering book details such as 
        Title, Author, Price, and Rating. Saves the raw extracted
        data locally and pushes it to XCom for downstream tasks."""
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Cache-Control": "max-age=0",
        }

        base_url = "https://www.amazon.com/s?k=data+engineering+books"
        books, seen_titles = [], set()
        page = 1

        while page <= max_pages and len(books) < num_books:
            url = f"{base_url}&page={page}"

            try:
                response = requests.get(url, headers=headers, timeout=15)
            except requests.RequestException as e:
                print(f"Request failed: {e}")
                break

            if response.status_code != 200:
                print(f"Failed to retrieve page {page} (status {response.status_code})")
                break

            soup = BeautifulSoup(response.text, "html.parser")
            book_containers = soup.find_all("div", {"data-component-type": "s-impression-counter"})

            for book in book_containers:
                title_tag = book.select_one("h2 span")
                author_tag = book.select_one("a.a-size-base.a-link-normal")
                price_tag = book.select_one("span.a-price > span.a-offscreen")
                rating_tag = book.select_one("span.a-icon-alt")

                if title_tag and price_tag:
                    title = title_tag.text.strip()
                    if title not in seen_titles:
                        seen_titles.add(title)
                        books.append({
                            "Title": title,
                            "Author": author_tag.text.strip() if author_tag else "N/A",
                            "Price": price_tag.text.strip(),
                            "Rating": rating_tag.text.strip() if rating_tag else "N/A"
                        })

            if len(books) >= num_books:
                break

            page += 1
            time.sleep(random.uniform(1.5, 3.0))

        # Convert to DataFrame
        df = pd.DataFrame(books)
        df.drop_duplicates(subset="Title", inplace=True)

        # Save raw data
        os.makedirs("/opt/airflow/tmp", exist_ok=True)
        raw_path = "/opt/airflow/tmp/amazon_books_raw.csv"
        df.to_csv(raw_path, index= False)
        print(f"[EXTRACT] Amazon book data successfully saved at {raw_path}")

        # Push Summary to XCom
        import json
        summary = {
            "rows": len(df),
            "columns": list(df.columns),
            "sample": df.head(3).to_dict("records"),
        }
        formatted_summary = json.dumps(summary, indent=2, ensure_ascii=False).replace("\xa0", " ")

        if ti:
            ti.xcom_push(key="df_summary", value=formatted_summary)
            print("[XCOM] Pushed JSON summary to XCom.")
        
        print("\nPreview of Extracted Data:")
        print(df.head(5).to_string(index=False))

        return raw_path

    # task dependencies
    raw_file = get_amazon_data_books()

        
        
dag = amazon_books_etl()