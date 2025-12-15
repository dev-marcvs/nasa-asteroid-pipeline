from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, array
import sys

# --- CONFIGURAÇÕES ---
if len(sys.argv) > 1:
    EXECUTION_DATE = sys.argv[1]
else:
    raise ValueError("Por favor, forneça a data de execução (YYYY-MM-DD).")

# SEU ID DE PROJETO (Minúsculo!)
PROJECT_ID = "asteroid-data-project" 

BUCKET_NAME = "nasa-asteroid-datalake-mvgf"
INPUT_PATH = f"gs://{BUCKET_NAME}/raw/neo_feed/data={EXECUTION_DATE}/*.json"
OUTPUT_TABLE = f"{PROJECT_ID}.nasa_asteroid_silver.nasa_asteroids_cleaned"

def create_spark_session():
    """
    Cria a sessão Spark.
    NOTA: O GCS Connector (Jar Shaded) é injetado via --jars na DAG, 
    então aqui configuramos apenas o BigQuery e as classes do Hadoop.
    """
    return (SparkSession.builder
            .appName("NASA Asteroid Processing")
            # Apenas BigQuery aqui (Versão 0.31.1 que funcionou)
            .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.31.1")
            
            # Configurações obrigatórias para o GCS Connector funcionar
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/usr/local/airflow/include/gcp/service_account.json")
            .getOrCreate())

def process_data():
    spark = create_spark_session()
    
    print(f"🔍 LENDO: {INPUT_PATH}")
    
    try:
        # Lê o JSON
        df_raw = spark.read.json(INPUT_PATH)
    except Exception as e:
        print(f"⚠️ Erro ao ler arquivo (pode estar vazio ou não existir): {e}")
        return

    # LÓGICA DE TRATAMENTO (Sua correção do erro Map vs Struct)
    # Pega todas as colunas dentro de near_earth_objects e joga num array
    df_arrays = df_raw.select(array(col("near_earth_objects.*")).alias("days_arrays"))
    df_day_list = df_arrays.select(explode(col("days_arrays")).alias("asteroid_list"))
    df_flat = df_day_list.select(explode(col("asteroid_list")).alias("asteroid"))
    
    # Seleciona colunas finais
    df_final = df_flat.select(
        col("asteroid.id").alias("asteroid_id"),
        col("asteroid.name").alias("name"),
        col("asteroid.absolute_magnitude_h").alias("absolute_magnitude"),
        col("asteroid.estimated_diameter.kilometers.estimated_diameter_min").alias("diameter_min_km"),
        col("asteroid.estimated_diameter.kilometers.estimated_diameter_max").alias("diameter_max_km"),
        col("asteroid.is_potentially_hazardous_asteroid").alias("is_hazardous"),
        col("asteroid.close_approach_data")[0].close_approach_date.alias("close_approach_date"),
        col("asteroid.close_approach_data")[0].relative_velocity.kilometers_per_hour.alias("velocity_kmph"),
        col("asteroid.close_approach_data")[0].miss_distance.kilometers.alias("miss_distance_km")
    )

    print(f"💾 Salvando no BigQuery: {OUTPUT_TABLE}")
    
    (df_final.write
     .format("bigquery")
     .option("temporaryGcsBucket", BUCKET_NAME)
     .mode("append")
     .save(OUTPUT_TABLE))
     
    print("✅ Sucesso Spark!")

if __name__ == "__main__":
    process_data()