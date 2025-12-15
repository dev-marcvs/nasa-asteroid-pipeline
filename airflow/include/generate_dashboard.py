import os
import sys
import joblib
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from google.cloud import bigquery
from google.cloud import storage
import logging

# Configuração visual do Seaborn
sns.set_theme(style="whitegrid")
plt.rcParams.update({'figure.max_open_warning': 0})

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

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Baixa arquivos do GCS para local."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    logger.info(f"Download concluido: {source_blob_name} -> {destination_file_name}")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Sobe arquivos locais para o GCS."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    logger.info(f"Upload concluido: {destination_blob_name}")

def generate_visualizations():
    logger.info("Iniciando geracao de Dashboard Analitico...")

    # 1. Carregar Dados
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
    LIMIT 2000 
    """
    # Limitamos a 2000 para o grafico nao ficar poluido demais
    
    df = client_bq.query(query).to_dataframe()
    
    if df.empty:
        logger.error("Sem dados para gerar graficos.")
        sys.exit(1)

    # 2. Carregar Modelos (Baixar do GCS para /tmp)
    try:
        download_blob(BUCKET_NAME, f"models/{MODEL_RF_FILENAME}", f"/tmp/{MODEL_RF_FILENAME}")
        download_blob(BUCKET_NAME, f"models/{MODEL_KM_FILENAME}", f"/tmp/{MODEL_KM_FILENAME}")
        
        rf_model = joblib.load(f"/tmp/{MODEL_RF_FILENAME}")
        kmeans_model = joblib.load(f"/tmp/{MODEL_KM_FILENAME}")
    except Exception as e:
        logger.error(f"Erro ao carregar modelos: {e}")
        sys.exit(1)

    # --- GRAFICO 1: FEATURE IMPORTANCE (O que causa perigo?) ---
    logger.info("Gerando Grafico 1: Importancia de Atributos...")
    
    features = ['absolute_magnitude', 'avg_diameter_km', 'velocity_kmph', 'miss_distance_km']
    importances = rf_model.feature_importances_
    
    df_importances = pd.DataFrame({'Feature': features, 'Importance': importances})
    df_importances = df_importances.sort_values(by='Importance', ascending=False)

    plt.figure(figsize=(10, 6))
    sns.barplot(x='Importance', y='Feature', data=df_importances, palette='viridis')
    plt.title('O que define um Asteroide Perigoso? (Random Forest)')
    plt.xlabel('Grau de Importancia')
    plt.tight_layout()
    plt.savefig("/tmp/dash_1_feature_importance.png")
    plt.close()

    # --- GRAFICO 2: CLUSTERING (Grupos Ocultos) ---
    logger.info("Gerando Grafico 2: Clusters K-Means...")
    
    # Prever os clusters usando o modelo carregado
    X = df[features]
    df['cluster'] = kmeans_model.predict(X)
    
    plt.figure(figsize=(10, 6))
    sns.scatterplot(
        data=df, 
        x='miss_distance_km', 
        y='velocity_kmph', 
        hue='cluster', 
        palette='deep',
        style='is_hazardous',
        s=100, 
        alpha=0.7
    )
    plt.title('Agrupamento de Asteroides (K-Means): Distancia vs Velocidade')
    plt.xlabel('Distancia (km)')
    plt.ylabel('Velocidade (km/h)')
    plt.legend(title='Cluster ID', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.savefig("/tmp/dash_2_clusters.png")
    plt.close()

    # --- GRAFICO 3: PAIRPLOT (Visao Geral Estatistica) ---
    logger.info("Gerando Grafico 3: Pairplot de Correlacoes...")
    
    # Selecionar apenas algumas colunas para o grafico nao ficar gigante
    subset = df[['avg_diameter_km', 'velocity_kmph', 'is_hazardous']]
    sns.pairplot(subset, hue='is_hazardous', palette='husl')
    plt.savefig("/tmp/dash_3_overview.png")
    plt.close()

    # 3. Upload para o GCS
    logger.info("Fazendo upload dos relatorios para o Storage...")
    upload_blob(BUCKET_NAME, "/tmp/dash_1_feature_importance.png", "dashboard/relatorio_importancia.png")
    upload_blob(BUCKET_NAME, "/tmp/dash_2_clusters.png", "dashboard/relatorio_clusters.png")
    upload_blob(BUCKET_NAME, "/tmp/dash_3_overview.png", "dashboard/relatorio_geral.png")

    logger.info("Dashboard gerado com sucesso. Verifique a pasta 'dashboard' no seu Bucket.")

if __name__ == "__main__":
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        logger.warning("Credenciais nao encontradas. Tentando Default...")
    generate_visualizations()