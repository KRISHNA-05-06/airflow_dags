# ✈️ Airflow ETL Pipelines

Production-grade Apache Airflow DAGs built with Docker, MySQL, git-sync, and GitHub Actions CI/CD.

---

## 📦 Pipelines

### 1. Daily Market Data ETL
Simulates a multi-region stock market pipeline using dynamic task mapping.

- **Extract:** Generates market data for 4 regions (US, Europe, Asia, Africa) in parallel
- **Transform:** Sorts top gainers and losers by daily change percentage
- **Load:** Stores results into MySQL with dynamically named tables per region

**Tech:** Airflow 3.0 · Docker · MySQL · Dynamic Task Mapping · XCom

---

### 2. Data Engineering Books ETL
Fetches real book data from Open Library API and loads it into MySQL daily.

- **Extract:** Pulls 50 data engineering books via Open Library API
- **Transform:** Cleans data types, fills missing values, adds timestamps
- **Load:** Truncate-and-insert into MySQL for idempotent daily refreshes
- **Alerts:** Email notifications on task failure

**Tech:** Airflow 3.0 · Open Library API · MySQL · Email Alerts

---

## 🏗️ Architecture

```
GitHub (airflow_dags repo)
        ↓ every 30s (git-sync)
   Airflow reads DAGs automatically
        ↓
   Extract → Transform → Load to MySQL
        ↓
   GitHub Actions validates every push
```

---

## ⚙️ Tech Stack

| Tool | Purpose |
|---|---|
| Apache Airflow 3.0 | Workflow orchestration |
| Docker | Containerized deployment |
| MySQL | Data storage |
| git-sync | Automatic DAG deployment |
| GitHub Actions | CI/CD DAG validation |
| Open Library API | Real book data source |
| Python | ETL logic |

---

## 🚀 Setup

### Prerequisites
- Docker Desktop
- MySQL installed locally
- Git

### Run Locally

```bash
# Clone the repo
git clone https://github.com/KRISHNA-05-06/airflow_dags.git
cd airflow_dags

# Start Airflow
docker-compose up -d

# Open Airflow UI
# http://localhost:8080
# Login: airflow / airflow
```

### MySQL Setup

```sql
CREATE DATABASE airflow_db;
CREATE USER 'airflow'@'%' IDENTIFIED BY 'airflow';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'%';
FLUSH PRIVILEGES;
```

---

## 📊 DAG Structure

```
Daily Market ETL:
extract_market_data (x4 parallel)
        ↓
transform_market_data (x4 parallel)
        ↓
load_to_mysql (x4 parallel)

Books ETL:
get_amazon_data_books
        ↓
transform_amazon_books
        ↓
load_to_mysql
```

---

## 🔄 CI/CD

Every push to `dags/` triggers GitHub Actions to automatically validate DAG syntax using `py_compile`. Invalid DAGs are rejected before reaching Airflow, protecting the production environment.

---

## 📁 Repository Structure

```
airflow_dags/
├── dags/
│   ├── daily_etl_pipeline_airflow3.py   # Market data ETL
│   └── amazon_etl_dag.py                # Books ETL
├── .github/
│   └── workflows/
│       └── validate-dags.yml            # GitHub Actions CI
└── .gitignore
```

---

## 👤 Author

**Sri Krishna Sai Kota**
MS Computer Science · University of South Florida

- 🔗 LinkedIn: [linkedin.com/in/srikrishnasai](https://linkedin.com/in/srikrishnasai/)
- 💻 GitHub: [github.com/KRISHNA-05-06](https://github.com/KRISHNA-05-06)
- 📧 srikrishnasaikota1@gmail.com