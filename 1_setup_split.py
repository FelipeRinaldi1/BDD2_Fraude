import pandas as pd
import json
import os

# --- Configurações ---
SOURCE_FILE = 'creditcard.csv'
OUTPUT_MYSQL_CSV = 'credit-card1.csv'
OUTPUT_MONGO_JSON = 'credit-card2.json'

def split_data():
    print(f"Lendo {SOURCE_FILE}...")
    # Lê o dataset original
    try:
        df = pd.read_csv(SOURCE_FILE)
    except FileNotFoundError:
        print(f"ERRO: Coloque o arquivo {SOURCE_FILE} nesta pasta antes de rodar.")
        return

    print(f"Total de registros: {len(df)}")
    
    # Calcula o ponto da metade
    half_point = len(df) // 2
    
    # Divide o DataFrame
    df_part1 = df.iloc[:half_point]
    df_part2 = df.iloc[half_point:]
    
    print(f"Parte 1 (MySQL): {len(df_part1)} registros.")
    print(f"Parte 2 (Mongo): {len(df_part2)} registros.")
    
    # Salva Parte 1 como CSV para o MySQL
    df_part1.to_csv(OUTPUT_MYSQL_CSV, index=False)
    print(f"Gerado: {OUTPUT_MYSQL_CSV}")
    
    # Salva Parte 2 como JSON para o MongoDB
    # Convertendo para lista de dicionários para formato JSON correto
    records_list = df_part2.to_dict(orient='records')
    with open(OUTPUT_MONGO_JSON, 'w') as f:
        json.dump(records_list, f)
    print(f"Gerado: {OUTPUT_MONGO_JSON}")
    print("Divisão concluída!")

if __name__ == "__main__":
    split_data()
