import pandas as pd
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
import os

# --- Configurações ---
BASE_PATH = '/app'
GOLD_DB_PATH = '/app/data/gold/fraude_gold.db'

def run_ml_model():
    print("--- Iniciando Consumo de Dados (ML) ---")
    
    # 1. Conectar na camada GOLD (SQLite)
    db_path = f'sqlite:///{GOLD_DB_PATH}'
    print(f"Conectando ao SQLite Gold: {db_path}")
    
    try:
        engine = create_engine(db_path)
        conn = engine.raw_connection()
        df = pd.read_sql("SELECT * FROM tb_transacoes_completa", conn)
        conn.close()
        
        print("Distribuição de classes:\n", df['Class'].value_counts())

    except Exception as e:
        print(f"Erro ao ler do SQLite. Verifique se o arquivo existe em {GOLD_DB_PATH}. Erro: {e}")
        return

    print("\n" + "="*50)
    print("   RELATÓRIO FINANCEIRO (Gold Analytics)")
    print("="*50)
    
    if 'Amount' in df.columns:
        total_transacionado = df['Amount'].sum()
        
        df_fraude = df[df['Class'] == 1]
        df_legitimo = df[df['Class'] == 0]
        
        total_prejuizo = df_fraude['Amount'].sum()
        ticket_medio_geral = df['Amount'].mean()
        ticket_medio_fraude = df_fraude['Amount'].mean()
        
        print(f"1. Volume Total Processado:   R$ {total_transacionado:,.2f}")
        print(f"2. Total Identificado Fraude: R$ {total_prejuizo:,.2f}")
        print(f"3. Ticket Médio (Geral):      R$ {ticket_medio_geral:,.2f}")
        print(f"4. Ticket Médio (Fraudes):    R$ {ticket_medio_fraude:,.2f}")
        print("-" * 50)
        print("Insight: Dados agregados prontos para Dashboards executivos.")
    else:
        print("AVISO: Coluna 'Amount' não encontrada. Pulei o relatório financeiro.")
    print("="*50 + "\n")


    # 2. Preparação para ML
    print("Preparando dados para treinamento...")
    X = df.drop('Class', axis=1)
    y = df['Class']
    
    # Divisão Treino/Teste
    print("Dividindo dados em Treino e Teste (80/20)...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
    
    # 3. Treinamento do Modelo RANDOM FOREST
    print("\n--- 2. Modelo Random Forest ---")
    print("Treinando RandomForestClassifier (pode levar alguns segundos)...")
    model_rf = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
    model_rf.fit(X_train, y_train)
    
    print("Avaliando Random Forest...")
    y_pred_rf = model_rf.predict(X_test)
    
    print("\n[Relatório Random Forest]")
    print(classification_report(y_test, y_pred_rf))
    print("Matriz de Confusão (RF):")
    print(confusion_matrix(y_test, y_pred_rf))

    # 4. Treinamento do Modelo RNA (Rede Neural Artificial)
    print("\n--- 3. Modelo RNA (Rede Neural) ---")
    print("Normalizando dados para a RNA (StandardScaler)...")

    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    print("Treinando MLPClassifier (RNA)...")
    # Camadas ocultas simples
    model_rna = MLPClassifier(hidden_layer_sizes=(30,), max_iter=200, random_state=42)
    model_rna.fit(X_train_scaled, y_train)

    # Avaliação RNA
    print("Avaliando RNA...")
    y_pred_rna = model_rna.predict(X_test_scaled)

    print("\n[Relatório RNA]")
    print(classification_report(y_test, y_pred_rna))
    print("Matriz de Confusão (RNA):")
    print(confusion_matrix(y_test, y_pred_rna))

    print("\n" + "="*50)
    print("   ANÁLISE FINAL: ARQUITETURA MEDALHÃO E DADOS GOLD")
    print("="*50)
    
    print("\n> Impressões sobre os dados Gold:")
    print("  1. Versatilidade: O relatório financeiro acima prova que o dado Gold serve tanto")
    print("     para Cientistas de Dados (ML) quanto para Analistas de Negócio (KPIs Financeiros).")
    print("  2. Performance: A leitura do SQLite local eliminou a latência de rede que teríamos")
    print("     se conectássemos diretamente no Mongo Cloud ou MySQL Docker a cada análise.")
    
    print("\n> Diferenças sobre as camadas do Medalhão:")
    print("  - BRONZE: Armazenamento cru (Raw), ideal para auditoria e reprocessamento histórico.")
    print("  - SILVER: Limpeza técnica (Deduplicação, Tipagem), mas ainda mantendo o grão original.")
    print("  - GOLD:   Regras de Negócio aplicadas. Aqui o dado está pronto para consumo final,")
    print("            seja por um modelo preditivo (como fizemos) ou um Dashboard PowerBI.")
    print("="*50)

    print("\nConsumo e Data Science finalizados com sucesso!")

if __name__ == "__main__":
    run_ml_model()
