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
    "email": ["srikrishnasaikota@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
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
    
    @task
    def transform_amazon_books(raw_file: str):
        """
        Cleans and standardizes the extracted Open Library book dataset.
        - Fills missing values
        - Converts data types
        - Filters out books with no rating
        - Adds extracted_at timestamp
        """
        import pandas as pd
        from datetime import datetime

        if not os.path.exists(raw_file):
            raise FileNotFoundError(f"Raw file not found: {raw_file}")

        df = pd.read_csv(raw_file)
        print(f"[TRANSFORM] Loaded {len(df)} records from raw dataset.")

        # Fill missing values
        df["Author"].fillna("N/A", inplace=True)
        df["First_Published"].fillna(0, inplace=True)
        df["Editions"].fillna(0, inplace=True)
        df["Rating"].fillna(0, inplace=True)

        # Convert types
        df["First_Published"] = pd.to_numeric(df["First_Published"], errors="coerce").fillna(0).astype(int)
        df["Editions"] = pd.to_numeric(df["Editions"], errors="coerce").fillna(0).astype(int)
        df["Rating"] = pd.to_numeric(df["Rating"], errors="coerce").fillna(0).round(2)

        # Drop rows where both Rating and First_Published are missing
        df.dropna(subset=["Rating", "First_Published"], how="all", inplace=True)

        # Add timestamp
        df["extracted_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Save transformed file
        transformed_path = raw_file.replace("raw", "transformed")
        df.to_csv(transformed_path, index=False)

        print(f"[TRANSFORM] Cleaned data saved at {transformed_path}")
        print(f"[TRANSFORM] {len(df)} valid records after standardization.")
        print(f"[TRANSFORM] Sample:\n{df.head(5).to_string(index=False)}")

        return transformed_path
    
    @task
    def load_to_mysql(transformed_file: str):
        """
        Loads the transfomred Open Library book dataset into MySQL.
        Uses trucate-and-load pattern for idempotency.
        """
        import mysql.connector
        import numpy as np

        db_config = {
            "host": "host.docker.internal",
            "user": "airflow",
            "password": "airflow",
            "database": "airflow_db",
            "port": 3306
        }

        df = pd.read_csv(transformed_file)
        table_name = "amazon_books_data"

        # Replace NaN with None for MySQL compatibility
        df = df.replace({np.nan: None})

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create table if not exists
        cursor.execute(f"""
                       CREATE TABLE IF NOT EXISTS {table_name} (
                       TITLE VARCHAR(512),
                       AUTHOR VARCHAR(255),
                       First_Published INT,
                       Rating DECIMAL(4,2),
                       Editions INT,
                       extracted_at VARCHAR(50)
                    );
                """)
        
        # Truncate for idempotency
        cursor.execute(f"TRUNCATE TABLE {table_name};")

        # Insert rows
        insert_query = f"""
        INSERT INTO {table_name}
        (Title, Author, First_Published, Rating, Editions, extracted_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row["Title"],
                    row["Author"],
                    row["First_Published"],
                    row["Rating"],
                    row["Editions"],
                    row["extracted_at"]
                ))
            except Exception as e:
                print(f"[LOAD] Skipped row due to error: {e}")
        
        conn.commit()
        conn.close()
        print(f"[LOAD] Table '{table_name}' loaded with {len(df)} records.")

    # task dependencies
    raw_file = get_amazon_data_books()
    transformed_file = transform_amazon_books(raw_file)
    load_to_mysql(transformed_file)

        
        
dag = amazon_books_etl()