# Execução do Pipeline de Dados

Siga a ordem exata dos comandos abaixo no terminal da sua VM.
```bash

#1
# Sobe os containers e recria se necessário
sudo docker-compose up -d --build


# 2
sudo docker-compose exec app pip install "pandas<2.0" "sqlalchemy<2.0" "numpy<2.0" pymysql cryptography pymongo apache-airflow scikit-learn matplotlib seaborn 


# 3
sudo docker-compose exec app python 1_setup_split.py

# 4 
sudo docker-compose exec app python 2_load_sources.py

#5
# Inicia o banco de metadados do Airflow
sudo docker-compose exec app airflow db migrate

# Cria a pasta de DAGs e copia o arquivo do pipeline
sudo docker-compose exec app mkdir -p /root/airflow/dags
sudo docker-compose exec app cp /app/fraude_etl_pipeline.py /root/airflow/dags/

# Executa a DAG (Aguarde a mensagem de "Success")
sudo docker-compose exec app airflow dags test fraude_etl_medallion 2025-01-01

#6
# Move o banco SQLite gerado para a pasta correta (Gold)
sudo docker-compose exec app mkdir -p /app/data/gold
sudo docker-compose exec app mv /app/data/fraude_gold.db /app/data/gold/fraude_gold.db

# Roda o consumidor de ML
sudo docker-compose exec app python 3_ml_consumer.py

Nota: Para desligar tudo ao final, use: sudo docker-compose down
