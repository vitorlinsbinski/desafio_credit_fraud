from airflow.sdk import dag, task, BaseHook, get_current_context
from airflow.providers.standard.operators.empty import EmptyOperator
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

from src.pipeline import CreditFraudPipeline
from src.etl import DatabaseHandler

@dag(
    dag_id="dag_credit_fraud",
    start_date=datetime(2026, 9, 10),
    schedule="@daily",
    catchup=False,
    max_active_runs=1
)
def dag_credit_fraud():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    @task(
        task_id="test_db_connection"
    )
    def test_db_connection():
        connection = BaseHook.get_connection(
            "postgres_dw"
        )
        hook = connection.get_hook()
        database = DatabaseHandler(engine=hook.get_sqlalchemy_engine())
        
        has_connection = database.check_connection()
        
        if not has_connection:
            raise ConnectionError("Falha ao conectar com o banco de dados.")
        else:
            logger.info("Conexão com o banco de dados estabelecida com sucesso!")

    @task(
        task_id="extract_data"
    )
    def extract_data():
        context = get_current_context()
        
        execution_id = context["run_id"]
        execution_timestamp = context["logical_date"]
        source_pipeline = context["dag"].dag_id
        
        connection = BaseHook.get_connection(
            "postgres_dw"
        )
        hook = connection.get_hook()
        database = DatabaseHandler(
            engine=hook.get_sqlalchemy_engine()
        )
        
        csv_file_path = "/opt/airflow/data/raw/df_fraud_credit.csv"
        
        pipeline = CreditFraudPipeline(
            database=database
        )
        
        pipeline.extract(
            execution_id=execution_id,
            execution_timestamp=execution_timestamp,
            source_pipeline=source_pipeline,
            csv_file_path=csv_file_path,
            csv_separator=","
        )

    start >> test_db_connection() >> extract_data() >> end

dag_credit_fraud()