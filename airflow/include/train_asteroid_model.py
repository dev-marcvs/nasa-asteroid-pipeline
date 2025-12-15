import os
import sys
import joblib
import pandas as pd
from google.cloud import bigquery
from google.cloud import storage
from sklearn.ensemble import RandomForestClassifier
from sklearn.cluster import KMeans
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES ---
PROJECT_ID = "asteroid-data-project"
DATASET_GOLD = "nasa_asteroid_gold"
BUCKET_NAME = "nasa-asteroid-datalake-mvgf"

MODEL_RF_FILENAME = "asteroid_hazard_rf.pkl"
MODEL_KM_FILENAME = "asteroid_cluster_km.pkl"

def train_models():
    logger.info("Iniciando Pipeline de Treinamento de Modelos (RF + KMeans)...")

    # 1. Carregar dados
    client_bq = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT
        absolute_magnitude,
        avg_diameter_km,
        velocity_kmph,
        miss_distance_km,
        is_hazardous
    FROM `{PROJECT_ID}.{DATASET_GOLD}.fact_asteroid_approaches`
    WHERE absolute_magnitude IS NOT NULL 
    """
    
    logger.info("Executando query no BigQuery para carregar dataset de treino...")
    df = client_bq.query(query).to_dataframe()
    
    if df.empty:
        logger.error("Dataset vazio. O treinamento foi abortado.")
        sys.exit(1)
        
    logger.info(f"Dataset carregado. Total de registros: {len(df)}")

    # 2. Preprocessamento
    X = df[['absolute_magnitude', 'avg_diameter_km', 'velocity_kmph', 'miss_distance_km']]
    y = df['is_hazardous'].astype(int)

    # ---------------------------------------------------------
    # PARTE A: SUPERVISIONADO (Random Forest)
    # ---------------------------------------------------------
    logger.info("Iniciando treinamento do modelo Random Forest (Classificacao)...")
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
    rf_model.fit(X_train, y_train)

    acc = accuracy_score(y_test, rf_model.predict(X_test))
    logger.info(f"Modelo Random Forest treinado. Acuracia no teste: {acc:.4f}")

    # ---------------------------------------------------------
    # PARTE B: NÃO-SUPERVISIONADO (K-Means)
    # ---------------------------------------------------------
    logger.info("Iniciando treinamento do modelo K-Means (Clusterizacao)...")
    
    kmeans_model = KMeans(n_clusters=4, random_state=42)
    kmeans_model.fit(X)
    
    logger.info("Modelo K-Means convergiu com sucesso (k=4).")

    # ---------------------------------------------------------
    # SALVAR MODELOS
    # ---------------------------------------------------------
    client_gcs = storage.Client()
    bucket = client_gcs.bucket(BUCKET_NAME)

    # Upload RF
    joblib.dump(rf_model, f"/tmp/{MODEL_RF_FILENAME}")
    blob_rf = bucket.blob(f"models/{MODEL_RF_FILENAME}")
    blob_rf.upload_from_filename(f"/tmp/{MODEL_RF_FILENAME}")
    logger.info(f"Modelo Random Forest salvo em: gs://{BUCKET_NAME}/models/{MODEL_RF_FILENAME}")

    # Upload KMeans
    joblib.dump(kmeans_model, f"/tmp/{MODEL_KM_FILENAME}")
    blob_km = bucket.blob(f"models/{MODEL_KM_FILENAME}")
    blob_km.upload_from_filename(f"/tmp/{MODEL_KM_FILENAME}")
    logger.info(f"Modelo K-Means salvo em: gs://{BUCKET_NAME}/models/{MODEL_KM_FILENAME}")

    logger.info("Pipeline de ML finalizado com sucesso.")

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        logger.warning("Credenciais GCP nao encontradas no ambiente. Tentando Application Default Credentials...")
    train_models()