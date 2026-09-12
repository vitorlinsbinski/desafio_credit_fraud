from airflow.sdk import dag, task
from datetime import datetime

from src.pipeline import CreditFraudPipeline

@dag(
    dag_id="dag_credit_fraud",
    start_date=datetime(2026, 9, 10),
    schedule="@daily",
    catchup=False,
    max_active_runs=1
)
def dag_credit_fraud():

    @task(
        task_id="extract_data"
    )
    def extract_data():
        csv_file_path = "./data/origin/df_fraud_credit.csv"
        pipeline = CreditFraudPipeline()
        
        pipeline.extract(csv_file_path=csv_file_path, csv_separator=",")

    extract_data()

dag_credit_fraud()