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
    def get_amazon_data_books(ti=None):
        """
        Extracts Data Engineering book details from Open Library API.
        Returns Title, Author, Publish Year, and Rating.
        """
        import requests
        import pandas as pd
        import os
        import json

        url = "https://openlibrary.org/search.json?q=data+engineering&limit=50"
        
        try:
            response = requests.get(url, timeout=15)
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None

        if response.status_code != 200:
            print(f"Failed to retrieve data (status {response.status_code})")
            return None

        data = response.json()
        books = []

        for doc in data.get("docs", []):
            books.append({
                "Title": doc.get("title", "N/A"),
                "Author": ", ".join(doc.get("author_name", ["N/A"])),
                "First_Published": doc.get("first_publish_year", "N/A"),
                "Rating": round(doc.get("ratings_average", 0), 2),
                "Editions": doc.get("edition_count", 0),
            })

        df = pd.DataFrame(books)
        df.drop_duplicates(subset="Title", inplace=True)

        os.makedirs("/opt/airflow/tmp", exist_ok=True)
        raw_path = "/opt/airflow/tmp/amazon_books_raw.csv"
        df.to_csv(raw_path, index=False)
        print(f"[EXTRACT] {len(df)} books extracted and saved at {raw_path}")

        summary = {
            "rows": len(df),
            "columns": list(df.columns),
            "sample": df.head(3).to_dict("records"),
        }
        formatted_summary = json.dumps(summary, indent=2)

        if ti:
            ti.xcom_push(key="df_summary", value=formatted_summary)
            print("[XCOM] Pushed JSON summary to XCom.")

        print("\nPreview of Extracted Data:")
        print(df.head(5).to_string(index=False))

        return raw_path

    # task dependencies
    raw_file = get_amazon_data_books()

        
        
dag = amazon_books_etl()