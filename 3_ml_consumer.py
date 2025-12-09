import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, confusion_matrix
import os

# --- Configurações ---
BASE_PATH = '/app'
# CORREÇÃO 1: Caminho apontando para a pasta 'gold' e sem o prefixo 'sqlite:///'
GOLD_DB_PATH = '/app/data/gold/fraude_gold.db'

def run_ml_model():
    print("--- Iniciando Consumo de Dados (ML) ---")
    
    # 1. Conectar na camada GOLD (SQLite)
    # O prefixo é adicionado aqui corretamente
    db_path = f'sqlite:///{GOLD_DB_PATH}'
    print(f"Conectando ao SQLite Gold: {db_path}")
    
    try:
        engine = create_engine(db_path)
        
        # CORREÇÃO 2: Usar raw_connection() para evitar erro do Pandas vs SQLAlchemy
        conn = engine.raw_connection()
        df = pd.read_sql("SELECT * FROM tb_transacoes_completa", conn)
        conn.close()
        
        # Verifica se temos as classes de fraude (0 e 1)
        print("Distribuição de classes:\n", df['Class'].value_counts())

    except Exception as e:
        print(f"Erro ao ler do SQLite. Verifique se o arquivo existe em {GOLD_DB_PATH}. Erro: {e}")
        return

    # 2. Preparação para ML
    print("\nPreparando dados para treinamento...")
    # X são as features (tudo menos a coluna Class)
    X = df.drop('Class', axis=1)
    # y é o alvo (a coluna Class)
    y = df['Class']
    
    # Divisão Treino/Teste
    print("Dividindo dados em Treino e Teste (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 3. Treinamento do Modelo
    print("Treinando RandomForestClassifier (pode levar alguns segundos)...")
    # n_jobs=-1 usa todos os núcleos da CPU para ser mais rápido
    model = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
    model.fit(X_train, y_train)
    
    # 4. Avaliação
    print("\nAvaliando o modelo...")
    y_pred = model.predict(X_test)
    
    print("\n--- Relatório de Classificação ---")
    print(classification_report(y_test, y_pred))
    
    print("\n--- Matriz de Confusão ---")
    print(confusion_matrix(y_test, y_pred))
    print("\nConsumo e Data Science finalizados com sucesso!")

if __name__ == "__main__":
    run_ml_model()