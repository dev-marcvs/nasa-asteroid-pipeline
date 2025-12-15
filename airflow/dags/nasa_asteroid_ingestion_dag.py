from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta

# --- CONFIGURAÇÕES ---
PROJECT_ID = "asteroid-data-project" 

DATASET_SILVER = "nasa_asteroid_silver"
DATASET_GOLD = "nasa_asteroid_gold"

default_args = {
    "owner": "astro",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "01_nasa_asteroind_ingestion_bronze",
    default_args=default_args,
    description="Pipeline: Ingestao Python -> Spark GCS/BQ -> SQL Gold",
    schedule="@daily",             
    start_date=datetime(2025, 6, 1), 
    catchup=True,                  
    max_active_runs=1,             
) as dag:

    # 1. TAREFA BRONZE: Ingestão
    task_ingest_bronze = BashOperator(
        task_id="ingest_nasa_asteroid_to_gcs",
        bash_command="""
        export GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp/service_account.json && \
        python /usr/local/airflow/include/ingest_nasa_asteroid.py {{ ds }}
        """
    )

    # 2. TAREFA SILVER: Processamento Spark
    task_transform_spark = BashOperator(
        task_id="transform_spark_silver",
        bash_command="""
        curl -L -o /tmp/gcs-connector-shaded.jar https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.8/gcs-connector-hadoop3-2.2.8-shaded.jar && \
        
        export GOOGLE_APPLICATION_CREDENTIALS=/usr/local/airflow/include/gcp/service_account.json && \
        
        spark-submit \
        --jars /tmp/gcs-connector-shaded.jar \
        --packages com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1 \
        /usr/local/airflow/include/process_nasa_asteroid.py {{ ds }}
        """
    )

    # 3. TAREFA GOLD: SQL
    sql_update_gold = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_GOLD}.fact_asteroid_approaches`
    (
        asteroid_id STRING,
        name STRING,
        close_approach_date DATE,
        ingestion_timestamp TIMESTAMP,
        absolute_magnitude FLOAT64,
        avg_diameter_km FLOAT64,
        velocity_kmph FLOAT64,
        miss_distance_km FLOAT64,
        is_hazardous BOOLEAN
    )
    PARTITION BY close_approach_date
    CLUSTER BY is_hazardous;

    DELETE FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_asteroid_approaches`
    WHERE close_approach_date = DATE('{{{{ ds }}}}');

    INSERT INTO `{PROJECT_ID}.{DATASET_GOLD}.fact_asteroid_approaches`
    SELECT
      asteroid_id,
      name,
      PARSE_DATE('%Y-%m-%d', close_approach_date),
      CURRENT_TIMESTAMP(),
      absolute_magnitude,
      ROUND((diameter_min_km + diameter_max_km) / 2, 4),
      CAST(velocity_kmph AS FLOAT64),
      CAST(miss_distance_km AS FLOAT64),
      is_hazardous
    FROM `{PROJECT_ID}.{DATASET_SILVER}.nasa_asteroids_cleaned`
    WHERE close_approach_date = '{{{{ ds }}}}';
    """

    task_update_gold = BigQueryInsertJobOperator(
        task_id="update_gold_layer",
        configuration={
            "query": {
                "query": sql_update_gold,
                "useLegacySql": False,
            }
        },
        location="us-central1",
        project_id=PROJECT_ID
    )

    task_ingest_bronze >> task_transform_spark >> task_update_gold