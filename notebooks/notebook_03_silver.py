# Databricks notebook source
# MAGIC %md
# MAGIC # Silver
# MAGIC
# MAGIC Este notebook corresponde à **camada Silver** do pipeline de dados, sendo responsável pela padronização inicial e preparação dos dados provenientes da camada Bronze, disponíveis na tabela **`workspace.bronze.tb_Sinistros_Transito_open_data`** no Databricks.
# MAGIC
# MAGIC Nesta etapa, os dados são carregados e passam por um processo de **inferência e ajuste de tipos**. A conversão é realizada com foco em transformar colunas originalmente interpretadas como texto em tipos mais adequados, como inteiros, valores numéricos e datas, garantindo maior consistência e melhor desempenho durante o processamento analítico dos dados.
# MAGIC
# MAGIC Após essa etapa, os dados são convertidos novamente para um DataFrame Spark e persistidos no Data Lake no formato **Delta Lake (baseado em Parquet)**, garantindo eficiência de leitura, compressão e escalabilidade.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  Remoção de duplicatas e dataframe silver
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC  Vamos remover as colunas vazias 

# COMMAND ----------

# MAGIC %md
# MAGIC  A coluna com a informação de protocolo também será removida, pois não traz nenhuma informação útil para futuras análises. 

# COMMAND ----------

# MAGIC %md
# MAGIC  Algumas colunas, faltam mais  de 50% das informações. Vamos removê-las. 

# COMMAND ----------

# MAGIC %md
# MAGIC  Algumas colunas deveriam ser numéricas, mas estão como string. Vamos fazer a transformação para inteiros: 1,0 -> 1, 2,0 -> 2, ... isto será responsável pela redução de memória ocupada pelos dados, diminuindo gastos com cluster. 

# COMMAND ----------

# MAGIC %md
# MAGIC  Vamos pradronizar valores nulos

# COMMAND ----------

# MAGIC %md
# MAGIC ### Persistência dos dados na camada Silver
# MAGIC
# MAGIC #### Gravação dos dados em formato Delta Lake (Parquet)
# MAGIC
# MAGIC Nesta etapa, os dados tratados e transformados na camada Silver são persistidos no Data Lake utilizando o formato **Delta Lake**, que é construído sobre arquivos **Parquet**.
# MAGIC
# MAGIC O Parquet é um formato de armazenamento colunar que proporciona **alta compressão** e **leitura eficiente**, permitindo que apenas as colunas necessárias sejam processadas. Isso resulta em melhor desempenho e redução de custo computacional, especialmente em grandes volumes de dados.
# MAGIC
# MAGIC O Delta Lake complementa o Parquet ao adicionar funcionalidades essenciais para pipelines de dados modernos, como:
# MAGIC - controle de versão (time travel)  
# MAGIC - transações ACID (maior confiabilidade)  
# MAGIC - suporte a evolução de schema  
# MAGIC - base para cargas incrementais futuras  
# MAGIC
# MAGIC Embora o cenário atual não envolva ingestão incremental, a utilização do Delta Lake garante que o pipeline esteja preparado para evoluções futuras, mantendo escalabilidade e robustez.
# MAGIC
# MAGIC Após a gravação dos dados, será criada uma **tabela no catálogo do Databricks (Unity Catalog)**, permitindo consultas via SQL diretamente sobre os dados armazenados no Data Lake, sem necessidade de duplicação.
# MAGIC
# MAGIC Essa abordagem assegura um fluxo de dados eficiente, confiável e alinhado com boas práticas de engenharia de dados.

# COMMAND ----------

# MAGIC %md
# MAGIC ##Widgets:

# COMMAND ----------


dbutils.widgets.text("env", "prod")
dbutils.widgets.text("execution_date", "")

env            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date")

print(f"Ambiente: {env}")
print(f"Data execução: {execution_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Imports e configuração:

# COMMAND ----------

from pyspark.sql import functions as F

storage_account = dbutils.secrets.get(scope='kv-scope', key='adls-storage-account-name')
storage_key     = dbutils.secrets.get(scope='kv-scope', key='adls-storage-key')

BRONZE_PATH = f'abfss://bronze@{storage_account}.dfs.core.windows.net/sinistros/'
SILVER_PATH = f'abfss://silver@{storage_account}.dfs.core.windows.net/sinistros/'

print('Configuração concluída.')

# COMMAND ----------

# MAGIC %md
# MAGIC ##Try/except com todo o processamento:

# COMMAND ----------

try:

    # Leitura da Bronze 
    df_bronze = spark.read.format("delta") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .load(BRONZE_PATH)

    total_bronze = df_bronze.count()
    print(f'Registros Bronze carregados: {total_bronze}')

    # Remoção de duplicatas 
    df_silver = df_bronze.dropDuplicates()

    # Remoção de colunas vazias
    colunas_para_remover = [
        'acidente_verificado', 'tempo_clima', 'situacao_semaforo', 'sinalizacao',
        'condicao_via', 'conservacao_via', 'ponto_controle', 'situacao_placa',
        'velocidade_max_via', 'mao_direcao', 'divisao_via1', 'divisao_via2',
        'num_semaforo', 'sentido_via', 'Protocolo', 'detalhe_endereco_acidente', 'numero'
    ]
    df_silver = df_silver.drop(*colunas_para_remover)

    # Conversão de colunas numéricas
    colunas_numericas = [
        "auto", "moto", "ciclom", "ciclista", "pedestre",
        "onibus", "caminhao", "viatura", "outros",
        "vitimas", "vitimasfatais"
    ]

    df_silver = df_silver.select(
        *[
            F.coalesce(
                F.regexp_replace(F.col(c), ",", ".").cast("double").cast("int"),
                F.lit(0)
            ).alias(c) if c in colunas_numericas else F.col(c)
            for c in df_silver.columns
        ]
    )

    #  Padronização de nulos
    colunas_texto = ["complemento", "bairro", "endereco", "bairro_cruzamento", "tipo"]
    for col_txt in colunas_texto:
        df_silver = df_silver.withColumn(
            col_txt,
            F.coalesce(F.col(col_txt), F.lit("NA"))
        )

    # Gravação Delta no container silver 
    df_silver.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .save(SILVER_PATH)

    # Registro no catálogo 
    spark.sql("CREATE DATABASE IF NOT EXISTS silver;")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS silver.tb_sinistros_transito_open_data
        USING DELTA
        LOCATION '{SILVER_PATH}'
    """)

    total_silver = df_silver.count()
    print(f'Silver concluído. Total de registros: {total_silver}')

    dbutils.notebook.exit(f"SUCCESS: Silver processado com sucesso. Registros: {total_silver}")

except Exception as e:
    error_msg = str(e)

    if "SUCCESS" in error_msg:
        dbutils.notebook.exit(error_msg)

    print(f'ERRO no Silver: {error_msg}')
    dbutils.notebook.exit(f"ERROR: Silver falhou. Detalhes: {error_msg}")
