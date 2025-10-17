from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum
import os

home = os.path.expanduser("~")
project_root = os.path.join(home, "youtube_de_project")

default_args = {
    "owner": "arjun",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="youtube_pipeline",
    default_args=default_args,
    description="YouTube Bronze → Silver → Gold pipeline",
    schedule="@daily",            # ✅ new
    start_date=pendulum.now().subtract(days=1),
    catchup=False,
    tags=["youtube", "spark", "etl"],
) as dag:

    # Task 1: JSON → Silver
    json_to_silver = BashOperator(
        task_id="json_to_silver",
        bash_command=f"{os.path.join(project_root, 'venv_spark', 'bin', 'python')} {os.path.join(project_root, 'scripts', 'json_to_silver.py')}"
    )

    # Task 2: CSV → Silver
    csv_to_silver = BashOperator(
        task_id="csv_to_silver",
        bash_command=f"{os.path.join(project_root, 'venv_spark', 'bin', 'python')} {os.path.join(project_root, 'scripts', 'csv_to_silver.py')}"
    )

    # Task 3: Silver → Gold
    silver_to_gold = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"{os.path.join(project_root, 'venv_spark', 'bin', 'python')} {os.path.join(project_root, 'scripts', 'silver_to_gold.py')}"
    )

    # Task 4: Gold → Postgres
    gold_to_postgres = BashOperator(
        task_id="gold_to_postgres",
        bash_command=f"{os.path.join(project_root, 'venv_spark', 'bin', 'python')} {os.path.join(project_root, 'scripts', 'gold_to_postgres.py')}"
    )

    # Task 5: Dashboard
    dashboard = BashOperator(
        task_id="dashboard",
        bash_command=f"{os.path.join(project_root, 'venv_spark', 'bin', 'python')} {os.path.join(project_root, 'scripts', 'dashboard.py')}"
    )

    # Orchestration order
    [json_to_silver, csv_to_silver] >> silver_to_gold >> gold_to_postgres >> dashboard