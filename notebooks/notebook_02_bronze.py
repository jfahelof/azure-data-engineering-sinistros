# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze
# MAGIC
# MAGIC Ingestão dos dados brutos na camada Bronze via ADLS Gen2.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Gravação dos dados em formato Delta (Parquet)
# MAGIC
# MAGIC  Nesta etapa, os dados são gravados utilizando o formato **Delta Lake**, que é construído sobre arquivos **Parquet**.
# MAGIC
# MAGIC  O Parquet é um formato de armazenamento colunar, permitindo maior eficiência na compressão e melhor desempenho em consultas, pois apenas as colunas necessárias são lidas durante o processamento.
# MAGIC
# MAGIC  O Delta Lake adiciona funcionalidades importantes ao Parquet, como controle de versão, transações ACID e suporte a cargas incrementais. Isso garante maior confiabilidade no processamento de dados, evitando inconsistências e permitindo operações como inserção, atualização e merge de dados.
# MAGIC
# MAGIC  Além disso, será criada uma **tabela externa no Databricks SQL Warehouse**, apontando para os arquivos armazenados no Data Lake. Dessa forma, os dados podem ser consultados via SQL sem a necessidade de duplicação, facilitando a integração com ferramentas analíticas e de visualização.
# MAGIC
# MAGIC  Com essa abordagem, obtemos um pipeline mais eficiente, escalável e confiável para o processamento de grandes volumes de dados.

# COMMAND ----------

dbutils.widgets.text("env", "prod")
dbutils.widgets.text("execution_date", "")

env            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date")

print(f"Ambiente: {env}")
print(f"Data execução: {execution_date}")

# COMMAND ----------

import pyspark.sql.functions as F

storage_account = dbutils.secrets.get(scope='kv-scope', key='adls-storage-account-name')
storage_key     = dbutils.secrets.get(scope='kv-scope', key='adls-storage-key')

LANDING_PATH = f'abfss://landing@{storage_account}.dfs.core.windows.net/'
BRONZE_PATH  = f'abfss://bronze@{storage_account}.dfs.core.windows.net/sinistros/'

print('Configuração ADLS concluída.')

# COMMAND ----------

try:

    #  Leitura do CSV do landing 
    df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("sep", ";") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .load(LANDING_PATH + 'data.csv')

    total_registros = df.count()
    print(f'Registros carregados: {total_registros}')

    #  carga lake
    df = df.withColumn("data", F.to_date("data"))    
    df = df.withColumn("ano",  F.year("data")) \
           .withColumn("mes",  F.month("data")) \
           .withColumn("dia",  F.dayofmonth("data"))
    df = df.withColumn("data_carga_lake", F.current_date())

    # Gravação Delta no container bronze 
    df.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .save(BRONZE_PATH)

    # Registro no catálogo 
    spark.sql("CREATE DATABASE IF NOT EXISTS bronze;")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS bronze.tb_sinistros_transito_open_data
        USING DELTA
        LOCATION '{BRONZE_PATH}'
    """)

    print(f'Bronze concluído. Total de registros: {total_registros}')

    # Saída de sucesso 
    dbutils.notebook.exit(f"SUCCESS: Bronze processado com sucesso. Registros: {total_registros}")

except Exception as e:
    error_msg = str(e)
    
    # Ignora a exceção gerada pelo próprio notebook.exit
    if "SUCCESS" in error_msg:
        dbutils.notebook.exit(error_msg)
    
    print(f'ERRO no Bronze: {error_msg}')
    dbutils.notebook.exit(f"ERROR: Bronze falhou. Detalhes: {error_msg}")
