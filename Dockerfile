FROM python:3.9-slim
# Define a pasta de trabalho dentro do container
WORKDIR /app

# Instala as bibliotecas necessárias para o seu projeto
# (Pandas, MySQL Connector, PyMongo, etc)
RUN pip install pandas mysql-connector-python pymongo sqlalchemy

# Copia seus arquivos da pasta atual para dentro do container
COPY . .

# Mantém o container rodando para você poder executar comandos nele
CMD ["tail", "-f", "/dev/null"]
