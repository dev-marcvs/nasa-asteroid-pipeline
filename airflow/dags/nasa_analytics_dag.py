from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "astro",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "03_nasa_analytics_dashboard",
    default_args=default_args,
    description="Gera graficos Seaborn e salva no GCS (Consome modelos de IA)",
    schedule="@daily",
    start_date=datetime(2025, 12, 1),
    catchup=False,
) as dag:

    task_generate_dash = BashOperator(
        task_id="generate_seaborn_plots",
        bash_command="""
        # Garante que as libs de visualizacao estao instaladas
        pip install seaborn matplotlib && \
        
        export GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp/service_account.json && \
        python /usr/local/airflow/include/generate_dashboard.py
        """
    )

    task_generate_dash