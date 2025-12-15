import sys
import json
import requests
from google.cloud import storage
import logging
import os

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÕES ---
if len(sys.argv) > 1:
    EXECUTION_DATE = sys.argv[1]
else:
    raise ValueError("Data de execução não fornecida.")

# SEU BUCKET
BUCKET_NAME = "nasa-asteroid-datalake-mvgf"
API_KEY = "SUA_CHAVE_AQUI" 
BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"

def ingest_data():
    logger.info(f"🚀 Iniciando ingestão para data: {EXECUTION_DATE}")
    
    # Verifica autenticação
    if "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ:
        logger.error("❌ Variável GOOGLE_APPLICATION_CREDENTIALS não encontrada!")
        sys.exit(1)
    
    url = f"{BASE_URL}?start_date={EXECUTION_DATE}&end_date={EXECUTION_DATE}&api_key={API_KEY}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        client = storage.Client()
        bucket = client.bucket(BUCKET_NAME)
        
        # Caminho no Storage
        destination_blob_name = f"raw/neo_feed/data={EXECUTION_DATE}/nasa_neo_{EXECUTION_DATE}.json"
        
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(json.dumps(data), content_type='application/json')
        
        logger.info(f"✅ Arquivo salvo com sucesso: gs://{BUCKET_NAME}/{destination_blob_name}")
        
    except Exception as e:
        logger.error(f"❌ Erro na ingestão: {e}")
        sys.exit(1)

if __name__ == "__main__":
    ingest_data()