from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "astro",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "02_nasa_ml_training",
    default_args=default_args,
    description="Treina modelo de IA para prever perigo de asteroides",
    schedule="@weekly", 
    start_date=datetime(2025, 12, 1),
    catchup=False,
) as dag:

    task_train_model = BashOperator(
        task_id="train_random_forest",
        bash_command="""
        export GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp/service_account.json && \
        python /usr/local/airflow/include/train_asteroid_model.py
        """
    )

    task_train_model