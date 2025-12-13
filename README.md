# Plataforma de Engenharia de Dados para Detecção de Fraudes em Cartão de Crédito

Este repositório apresenta a implementação de uma **plataforma completa de Engenharia de Dados**, com pipeline ETL automatizado, integração de múltiplas fontes de dados e aplicação de **Machine Learning** para detecção de fraudes em transações de cartão de crédito.

---

## Objetivos do Projeto

* Construir um pipeline de dados automatizado para detecção de fraudes;
* Ingerir dados de fontes heterogêneas (**MySQL** e **MongoDB**);
* Simular um **Data Lakehouse** utilizando a **Arquitetura Medalhão**;
* Automatizar o fluxo ETL com **Apache Airflow**;
* Preparar dados para análise e Ciência de Dados;
* Aplicar um modelo de **Machine Learning** para classificação de transações fraudulentas.

---

## Arquitetura da Solução

A arquitetura do projeto simula um ambiente real de Engenharia de Dados:

```
Fontes de Dados
   │
   ├── MySQL (Docker)
   ├── MongoDB Atlas
   │
   ▼
Pipeline ETL (Python)
   │
   ▼
Data Lakehouse (Arquitetura Medalhão)
   ├── Bronze  → Dados brutos (.txt)
   ├── Silver  → Dados tratados (DataFrames)
   └── Gold    → Dados analíticos (SQLite)
   │
   ▼
Machine Learning
   └── Classificação de Fraudes
```

---

## Tecnologias Utilizadas

* **Python 3**
* **Apache Airflow**
* **Docker**
* **MySQL**
* **MongoDB Atlas**
* **SQLite**
* **Pandas / NumPy**

---

## Fontes de Dados

O dataset original (`credit-card.csv`) foi particionado para simular fontes distintas:

* **credit-card1.csv** → Ingerido no **MySQL**;
* **credit-card2.json** → Ingerido no **MongoDB Atlas**.

---

## Pipeline ETL

### Extração

* Leitura dos dados a partir do MySQL (Docker);
* Leitura dos dados do MongoDB Atlas;
* Consolidação das fontes.

### Transformação

* Padronização de colunas;
* Limpeza de dados inconsistentes;
* Seleção de features relevantes;
* Preparação para Machine Learning.

### Carga

* **Bronze:** dados brutos;
* **Silver:** dados tratados;
* **Gold:** dados consolidados para análise.

---

## Conclusão

Este projeto demonstra, na prática, conceitos fundamentais de **Banco de Dados**, **Engenharia de Dados**, **ETL**, **Arquitetura de Dados**, **Automação com Airflow** e **Machine Learning**, simulando um cenário real de detecção de fraudes em cartão de crédito.

---

# Como Executar o Projeto

* Baixe os arquivos do repositório ou clone;
* E para a execução do Pipeline de Dados, siga a ordem exata dos comandos abaixo no terminal da sua VM:

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
