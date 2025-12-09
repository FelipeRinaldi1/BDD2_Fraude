import pandas as pd
import json
from sqlalchemy import create_engine, text
from pymongo import MongoClient
import sys

# --- Configurações MySQL (Docker) ---
MYSQL_USER = 'root'
MYSQL_PASS = 'root' # A senha que você definiu ao subir o Docker
MYSQL_HOST = 'db'
MYSQL_PORT = '3306'
MYSQL_DB_NAME = 'projeto_bd2'

# --- Configurações MongoDB (Atlas Cloud) ---
# SUBSTITUA ABAIXO PELA SUA STRING DO ATLAS
MONGO_URI = "mongodb+srv://aluno:aluno123@cluster0.e1klxkq.mongodb.net/?appName=Cluster0"
MONGO_DB_NAME = "db_fraude_origem"
MONGO_COLL_NAME = "transacoes_parte2"

# --- Arquivos de Entrada ---
INPUT_MYSQL = 'credit-card1.csv'
INPUT_MONGO = 'credit-card2.json'


def load_mysql():
    print("\n--- Iniciando Carga MySQL ---")
    # String de conexão SQLAlchemy para MySQL+pymysql
    db_connection_str = f'mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}'
    db_connection = create_engine(db_connection_str)

    try:
        # Cria o banco de dados se não existir
        with db_connection.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB_NAME}"))
            print(f"Banco {MYSQL_DB_NAME} verificado/criado.")

        engine_db = create_engine(f'{db_connection_str}/{MYSQL_DB_NAME}')
    
        print(f"Lendo {INPUT_MYSQL}...")
        df_mysql = pd.read_csv(INPUT_MYSQL)
        

        print("Inserindo dados na tabela 'transacoes_parte1' (isso pode demorar um pouco)...")
        
        df_mysql.to_sql('transacoes_parte1', con=engine_db, if_exists='replace', index=False, chunksize=1000)

        print("Carga MySQL concluída com sucesso!")
        
    except Exception as e:
        print(f"Erro no MySQL: {e}")
        sys.exit(1)


def load_mongo():
    print("\n--- Iniciando Carga MongoDB ---")
    try:
        client = MongoClient(MONGO_URI)
        db = client[MONGO_DB_NAME]
        collection = db[MONGO_COLL_NAME]
        
        # Limpa coleção anterior se existir (para testes limpos)
        collection.delete_many({})
        print(f"Coleção {MONGO_COLL_NAME} limpa.")

        # Lê o arquivo JSON
        print(f"Lendo {INPUT_MONGO}...")
        with open(INPUT_MONGO, 'r') as f:
            data_json = json.load(f)
        
        print(f"Inserindo {len(data_json)} documentos no MongoDB Atlas...")
        collection.insert_many(data_json)
        print("Carga MongoDB concluída com sucesso!")

    except Exception as e:
        print(f"Erro no MongoDB. Verifique sua Connection String. Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    # Garanta que seu container MySQL Docker esteja rodando antes disso!
    load_mysql()
    load_mongo()