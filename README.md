# 📊 YouTube Data Engineering Pipeline (Batch Processing)

## 🚀 Overview
This project implements a **modern data engineering pipeline** for analyzing YouTube trending video data.  
It demonstrates the **Medallion Architecture (Bronze → Silver → Gold)** using:

- **Apache Airflow (3.x)** → Orchestration & scheduling  
- **Apache Spark** → Scalable ETL transformations  
- **Local filesystem** → Data lake layers (Bronze/Silver/Gold)  
- **Postgres** → Serving layer for analytics  
- **Streamlit + Altair (via SQLAlchemy)** → Interactive BI dashboard  

The pipeline ingests raw JSON/CSV datasets, cleans and enriches them, computes derived metrics, and publishes analytics‑ready tables for visualization.

---

## 🏗️ Architecture
<img width="1137" height="607" alt="Architecture Design" src="https://github.com/user-attachments/assets/6614ebdf-a8e7-449c-a554-155d6224dcb3" />
---

## 📂 Project Structure
```text
YOUTUBE_DE_PROJECT/
│
├── bronze/                  # Raw input data (JSON/CSV)
├── silver/                  # Cleaned, normalized data
├── gold/                    # Aggregated, analytics-ready data
│
├── dags/
│   └── youtube_pipeline_dag.py   # Airflow DAG definition
│
├── scripts/
│   ├── json_to_silver.py         # Raw JSON → Silver layer
│   ├── csv_to_silver.py          # Raw CSV → Silver layer
│   ├── silver_to_gold.py         # Silver → Gold transformations
│   ├── gold_to_postgres.py       # Load Gold into Postgres
│   ├── dashboard.py              # Streamlit + Altair dashboard
│   └── clear_outputs.py          # Utility to clear old outputs
│
├── logs/                   # Airflow logs
├── airflow.cfg             # Airflow config
├── airflow.db              # Airflow metadata DB (SQLite for local)
├── postgresql-42.4.7.jar   # JDBC driver for Spark → Postgres
├── requirements.txt        # Python dependencies
└── venv_spark/             # Virtual environment
```

---

## ⚙️ Setup Instructions

### 1. Clone the repo
```bash
git clone https://github.com/Arjun-M-101/Youtube_DE_Project.git
cd Youtube_DE_Project
```

### 2. Create virtual environment
```bash
python3 -m venv venv_spark
source venv_spark/bin/activate
pip install -r requirements.txt
```

### 3. Initialize Airflow
```bash
airflow db migrate
airflow standalone
```

This starts:
- Scheduler
- Webserver (http://localhost:8080)
- Triggerer
- Workers
  
### 4. Place raw data
👉 Dataset link: YouTube Trending Video Dataset on Kaggle
https://www.kaggle.com/datasets/datasnaek/youtube-new
- Drop Kaggle YouTube trending .csv files into bronze/raw_statistics/
- Drop category .json files into bronze/raw_statistics_reference/

## 5. Postgres Setup
- Install Postgres locally (e.g., brew install postgresql on macOS or package manager on Linux).  
- Start Postgres service and create a database:
   ```bash
   createdb youtube_gold
   ```  
- (Optional) Create a user if not using the default postgres:
   ```bash
   createuser --interactive --pwprompt
   ```
- Ensure the JDBC driver (postgresql-42.4.7.jar) is present in the project root (already included).  
👉 Always check Postgres status before running the pipeline:
```bash
sudo systemctl status postgresql
```
If Postgres is inactive, start it with:
```
sudo systemctl start postgresql
```

## 6. Environment Variables
For security, set Postgres credentials as environment variables:
```bash
export PGUSER=postgres
export PGPASSWORD=your_password
```
These are automatically picked up by dashboard.py.

## 7. Fresh Setup (optional)
clear_outputs.py script can be used to reset the project state.
Before running the pipeline from scratch, clear old outputs (if already present):
```bash
python scripts/clear_outputs.py
```

### 8. Trigger the DAG
In the Airflow UI, enable and trigger youtube_pipeline.

### 9. Launch the dashboard
```bash
streamlit run scripts/dashboard.py
```

### 10. Sample Outputs (Airflow + Dashboard)
<img width="1280" height="531" alt="Airflow DAG Success Example" src="https://github.com/user-attachments/assets/73af76cc-a09e-470c-a31d-d28eab01fbd2" />
Airflow DAG Success Example

<img width="1280" height="538" alt="Streamlit Dashboard Example" src="https://github.com/user-attachments/assets/4a08d018-1c98-437e-9adb-2e7dc1f10ad8" />
Streamlit Dashboard Example

### 🔄 Data Flow

### **Bronze Layer (Raw Landing)**
- Stores raw `.csv` and `.json` files.  
- No transformations, just schema ingest and landing.  

### **Silver Layer (Cleaned & Normalized)**
- **`json_to_silver.py`**:  
  - Explodes `items` array in JSON.  
  - Extracts `id`, `category_name`.  
  - Adds `region`.  
- **`csv_to_silver.py`**:  
  - Casts numeric fields (`views`, `likes`, `comment_count`, etc.).  
  - Casts flags to boolean (`comments_disabled`, `ratings_disabled`, etc.).  
  - Normalizes `publish_time` → timestamp, `trending_date` → date.  
  - Adds `region`.  

### **Gold Layer (Analytics-Ready)**
- **`silver_to_gold.py`**:  
  - Joins videos with categories.  
  - Adds derived metric: `engagement_ratio = (likes + comment_count) / views`.  
  - Adds `region`.  
  - Unifies all regions into one dataset.  
  - Partitioned by `region`.  

### **Serving Layer**
- **`gold_to_postgres.py`**:  
  - Loads Gold dataset into Postgres table `videos_gold`.  
- **`dashboard.py`**:  
  - Streamlit + Altair visualizations:  
    - Top categories by views  
    - Views over time  
    - Likes vs Comments scatter  
    - Engagement ratio distribution  

## ✅ Key Takeaways
- Demonstrates Medallion Architecture (Bronze → Silver → Gold) with Spark.
- Shows Airflow 3.x orchestration with modern DAG syntax.
- Implements Spark → Postgres integration via JDBC.
- Provides an interactive BI dashboard with Streamlit + Altair.
- Fully reproducible locally, but designed with cloud mapping in mind (S3, EMR, MWAA, RDS/Redshift).

## ⚖️ Trade‑offs & Design Decisions
- Local filesystem vs. Cloud storage:
Used local directories for Bronze/Silver/Gold to keep setup simple. In production, these would map to S3 buckets for scalability and durability.
- Postgres vs. Data Warehouse:
Postgres is lightweight and easy to run locally. For enterprise scale, Redshift, Snowflake, or BigQuery would be more appropriate.
- Airflow standalone vs. Managed Airflow:
Standalone Airflow is quick to demo. In production, MWAA would handle scaling, logging, and monitoring.
- Spark local vs. Spark cluster:
Running Spark locally is enough for Kaggle‑sized datasets. For real YouTube‑scale data, Spark on EMR or Kubernetes would be required.
- Dashboarding with Streamlit:
Streamlit is fast for prototyping and recruiter‑friendly. For enterprise BI, Superset, Tableau, or Power BI would be more scalable.
