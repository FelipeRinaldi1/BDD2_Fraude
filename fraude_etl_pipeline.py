# Nome do arquivo (salvar na pasta de DAGs do Airflow): fraude_etl_pipeline.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
from pymongo import MongoClient
import json
import os

# --- CONFIGURAÇÕES GERAIS E CAMINHOS ---
BASE_PATH = "/app" 

BRONZE_PATH = os.path.join(BASE_PATH, "data/bronze")
SILVER_PATH = os.path.join(BASE_PATH, "data/silver")
GOLD_DB_PATH = os.path.join(BASE_PATH, "data/fraude_gold.db")

# Configurações das Fontes (Sources)
MYSQL_STR = 'mysql+pymysql://root:root@db/projeto_bd2'
# SUBSTITUA PELA SUA STRING REAL DO ATLAS
MONGO_URI = "mongodb+srv://aluno:aluno123@cluster0.e1klxkq.mongodb.net/?appName=Cluster0"
MONGO_DB = "db_fraude_origem"
MONGO_COLL = "transacoes_parte2"

# Arquivos intermediários
BRONZE_MYSQL_FILE = os.path.join(BRONZE_PATH, "mysql_raw.txt")
BRONZE_MONGO_FILE = os.path.join(BRONZE_PATH, "mongo_raw.txt")
SILVER_DATAFRAME_FILE = os.path.join(SILVER_PATH, "transacoes_unificadas.pkl")

# --- FUNÇÕES ETL ---

def etl_extract_mysql_to_bronze():
    """Lê do MySQL e salva como TXT (string CSV) na Bronze"""
    print("Iniciando extração MySQL...")
    engine = create_engine(MYSQL_STR)
    conn = engine.raw_connection() # <--- A correção mágica
    df = pd.read_sql("SELECT * FROM transacoes_parte1", conn)
    conn.close()
    
    # Simula salvamento raw (bruto) em TXT
    csv_content = df.to_csv(index=False)
    with open(BRONZE_MYSQL_FILE, "w") as f:
        f.write(csv_content)
    print(f"Dados do MySQL salvos em {BRONZE_MYSQL_FILE}")

def etl_extract_mongo_to_bronze():
    """Lê do Mongo Atlas e salva como TXT (string JSON) na Bronze"""
    print("Iniciando extração MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    coll = db[MONGO_COLL]
    
    # Busca tudo e remove o '_id' padrão do Mongo que atrapalha depois
    cursor = coll.find({}, {'_id': 0})
    list_docs = list(cursor)
    
    # Simula salvamento raw (bruto) em TXT formatado como JSON string
    json_content = json.dumps(list_docs)
    with open(BRONZE_MONGO_FILE, "w") as f:
        f.write(json_content)
    print(f"Dados do MongoDB salvos em {BRONZE_MONGO_FILE}")

def etl_transform_to_silver():
    """Lê os TXTs da Bronze, unifica, limpa e salva como Dataframe (pickle) na Silver"""
    print("Iniciando transformação Silver...")
    
    # Ler Bronze MySQL (formato CSV string)
    df_mysql = pd.read_csv(BRONZE_MYSQL_FILE)
    
    # Ler Bronze Mongo (formato JSON string)
    with open(BRONZE_MONGO_FILE, 'r') as f:
        mongo_data = json.load(f)
    df_mongo = pd.DataFrame(mongo_data)
    
    print(f"MySQL shape: {df_mysql.shape}, Mongo shape: {df_mongo.shape}")
    
    # Unificar
    df_final = pd.concat([df_mysql, df_mongo], ignore_index=True)
    
    # --- Transformações básicas (Exemplos) ---
    # Garantir que 'Class' seja inteiro
    df_final['Class'] = df_final['Class'].astype(int)
    # Preencher nulos (se houvesse) com 0
    df_final.fillna(0, inplace=True)
    
    print(f"Shape final unificado: {df_final.shape}")
    
    # Salvar na Silver (Usando pickle para manter o objeto DataFrame fiel)
    df_final.to_pickle(SILVER_DATAFRAME_FILE)
    print(f"DataFrame salvo na Silver em {SILVER_DATAFRAME_FILE}")

def etl_load_to_gold():
    """Lê o dataframe da Silver e salva como tabela no SQLite (Gold)"""
    print("Iniciando carga Gold (SQLite)...")
    
    # Ler Dataframe da Silver
    df = pd.read_pickle(SILVER_DATAFRAME_FILE)
    
    # Conectar SQLite
    # Usando 3 barras /// para caminho absoluto no Linux
    sqlite_engine = create_engine(f'sqlite:///{GOLD_DB_PATH}')
    
    # Salvar tabela (substitui se existir)
    conn = sqlite_engine.raw_connection()
    df.to_sql('tb_transacoes_completa', conn, if_exists='replace', index=False)
    conn.close()

# --- DEFINIÇÃO DA DAG DO AIRFLOW ---

default_args = {
    'owner': 'grupo_bd2',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1), # Data no passado para permitir execução manual imediata
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'fraude_etl_medallion',
    default_args=default_args,
    description='Pipeline ETL de Fraude - Arquitetura Medalhao',
    schedule=timedelta(days=1), # Executa uma vez por dia
    catchup=False # Não tenta rodar execuções passadas
)

# Definindo as Tasks (Tarefas)
t1_mysql_bronze = PythonOperator(
    task_id='extract_mysql_to_bronze',
    python_callable=etl_extract_mysql_to_bronze,
    dag=dag,
)

t2_mongo_bronze = PythonOperator(
    task_id='extract_mongo_to_bronze',
    python_callable=etl_extract_mongo_to_bronze,
    dag=dag,
)

t3_transform_silver = PythonOperator(
    task_id='transform_bronze_to_silver',
    python_callable=etl_transform_to_silver,
    dag=dag,
)

t4_load_gold = PythonOperator(
    task_id='load_silver_to_gold_sqlite',
    python_callable=etl_load_to_gold,
    dag=dag,
)

# --- DEFINIÇÃO DAS DEPENDÊNCIAS (O FLUXO) ---
# T1 e T2 podem rodar em paralelo. T3 só roda depois que AMBAS acabarem. T4 roda depois de T3.
[t1_mysql_bronze, t2_mongo_bronze] >> t3_transform_silver >> t4_load_gold