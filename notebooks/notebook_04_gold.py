# Databricks notebook source
# MAGIC %md
# MAGIC # Gold
# MAGIC
# MAGIC Este notebook corresponde à **camada Gold** do pipeline de dados, sendo responsável pela padronização dos dados provenientes da camada Silver, disponíveis na tabela **`workspace.silver.tb_Sinistros_Transito_open_data`** no Databricks.
# MAGIC
# MAGIC A camada Gold da arquitetura medallion é a etapa da pipeline de dados responsável pela disponibilização de dados refinados, agregados e prontos para consumo analítico. Nessa camada, são aplicadas regras de negócio, métricas e transformações que permitem gerar insights estratégicos, dashboards e análises preditivas.
# MAGIC
# MAGIC Como o projeto utiliza dados de sinistros de trânsito da cidade do Recife, o foco da camada Gold foi a construção de tabelas analíticas capazes de responder questões relevantes para a mobilidade urbana e segurança no trânsito. Entre os exemplos de análises desenvolvidas estão: bairros com maior número de acidentes, horários de maior ocorrência, tipos de veículos mais envolvidos em sinistros e meios de transporte associados a acidentes fatais.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Informações por bairro
# MAGIC
# MAGIC ### Queremos saber informações apuradas para cada bairro. Perguntas como: "Quais são os bairros com maior número de acidentes?", "Qual bairro tem acidentes mais fatais?" e várias outras podem ser respondidas a partir dessa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa um bairro, contendo o total de acidentes registrados, o número total de vítimas, o número de vítimas fatais e a taxa de fatalidade (razão entre vítimas fatais e vítimas totais).
# MAGIC
# MAGIC ###Essa estrutura permite identificar regiões com maior concentração de ocorrências, avaliar a gravidade média dos acidentes e comparar o risco relativo entre diferentes bairros, servindo como base para análises mais profundas e tomada de decisão orientada por dados.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Informações por tipo de meio de transporte 
# MAGIC
# MAGIC ###Queremos entender como os diferentes meios de transporte estão associados aos acidentes. Perguntas como: "Qual tipo de veículo está mais envolvido em acidentes?", "Quais apresentam maior número de vítimas fatais?" e outras análises podem ser respondidas com essa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa um tipo de transporte (como carro, moto, pedestre, etc.), contendo o total de ocorrências em que esteve envolvido e o número total de vítimas fatais associadas.
# MAGIC
# MAGIC ###Essa estrutura permite identificar quais meios estão mais presentes nos acidentes e quais estão relacionados a ocorrências mais graves, servindo como base para análises comparativas e possíveis ações de prevenção.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Queremos entender como os acidentes se distribuem ao longo do dia. Perguntas como: "Em quais horários ocorrem mais acidentes?" e "Existe algum período mais crítico?" podem ser respondidas com essa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa uma hora do dia, contendo o total de acidentes registrados naquele horário.
# MAGIC
# MAGIC ###Essa estrutura permite identificar padrões temporais, como horários de pico ou períodos de maior risco, servindo como base para análises mais aprofundadas e possíveis ações preventivas.

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agora, queremos entender como os acidentes evoluem ao longo dos meses. Perguntas como: "Qual mês teve mais acidentes?", "Em quais períodos há mais mortes?" e "A taxa de fatalidade varia ao longo do ano?" podem ser respondidas com essa tabela.
# MAGIC
# MAGIC ###Nela, cada linha representa um mês, contendo o total de acidentes, o número de vítimas fatais e a taxa de fatalidade (razão entre vítimas fatais e vítimas totais).
# MAGIC
# MAGIC ###Essa estrutura permite identificar padrões sazonais, comparar períodos mais críticos e avaliar a gravidade dos acidentes ao longo do tempo.

# COMMAND ----------

# MAGIC %md
# MAGIC Widgets:

# COMMAND ----------


dbutils.widgets.text("env", "prod")
dbutils.widgets.text("execution_date", "")

env            = dbutils.widgets.get("env")
execution_date = dbutils.widgets.get("execution_date")

print(f"Ambiente: {env}")
print(f"Data execução: {execution_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC Imports e configuração:

# COMMAND ----------


from pyspark.sql import functions as F
from pyspark.sql.window import Window

storage_account = dbutils.secrets.get(scope='kv-scope', key='adls-storage-account-name')
storage_key     = dbutils.secrets.get(scope='kv-scope', key='adls-storage-key')

SILVER_PATH = f'abfss://silver@{storage_account}.dfs.core.windows.net/sinistros/'
GOLD_PATH   = f'abfss://gold@{storage_account}.dfs.core.windows.net/sinistros/'

print('Configuração concluída.')

# COMMAND ----------

# MAGIC %md
# MAGIC Try/except com todo o processamento:

# COMMAND ----------


try:

    # Leitura da Silver 
    df_silver = spark.read.format("delta") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .load(SILVER_PATH)

    print(f'Registros Silver carregados: {df_silver.count()}')

    spark.sql("CREATE DATABASE IF NOT EXISTS gold;")

    # Tabela Bairro
    df_bairro = (
        df_silver
        .groupBy("bairro")
        .agg(
            F.count("bairro").alias("total_acidentes"),
            F.sum("vitimas").alias("total_vitimas"),
            F.sum("vitimasfatais").alias("total_fatais")
        )
        .withColumn("localizacao", F.concat(F.col("bairro"), F.lit(", Recife, Pernambuco, Brasil")))
        .withColumn("taxa_fatalidade",
            F.when(F.col("total_vitimas") > 0,
                   F.col("total_fatais") / F.col("total_vitimas")
            ).otherwise(F.lit(0))
        )
        .withColumn("percentual_acidentes",
            (F.col("total_acidentes") / F.sum("total_acidentes").over(Window.partitionBy())) * 100
        )
        .orderBy(F.col("total_acidentes").desc())
    )

    df_bairro.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .save(GOLD_PATH + 'bairro/')

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.tb_sinistros_transito_bairro
        USING DELTA LOCATION '{GOLD_PATH}bairro/'
    """)
    print('Tabela bairro salva.')

    # Tabela Transporte
    cols_transportes = ["auto", "moto", "ciclom", "ciclista", "pedestre",
                        "onibus", "caminhao", "viatura", "outros"]

    dfs_meios = []
    for col in cols_transportes:
        df_temp = (
            df_silver
            .filter(F.col(col) > 0)
            .agg(
                F.count("*").alias("total_acidentes"),
                F.sum("vitimasfatais").alias("total_fatais")
            )
            .withColumn("meio_transporte", F.lit(col))
        )
        dfs_meios.append(df_temp)

    df_meio_de_transporte = dfs_meios[0]
    for df_proximo in dfs_meios[1:]:
        df_meio_de_transporte = df_meio_de_transporte.union(df_proximo)

    df_meio_de_transporte = (
        df_meio_de_transporte
        .withColumn("taxa_fatalidade",
            F.when(F.col("total_acidentes") > 0,
                   F.col("total_fatais") / F.col("total_acidentes")
            ).otherwise(F.lit(0))
        )
        .select("meio_transporte", "total_acidentes", "total_fatais", "taxa_fatalidade")
        .orderBy(F.col("total_acidentes").desc())
    )

    df_meio_de_transporte.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .save(GOLD_PATH + 'transporte/')

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.tb_sinistros_transito_transporte
        USING DELTA LOCATION '{GOLD_PATH}transporte/'
    """)
    print('Tabela transporte salva.')

    # Tabela Hora
    df_hora = (
        df_silver
        .withColumn("hora_dia", F.hour(F.to_timestamp(F.col("hora"), "HH:mm:ss")))
        .filter(F.col("hora_dia").isNotNull())
        .groupBy("hora_dia")
        .agg(F.count("hora_dia").alias("total_acidentes"))
        .orderBy("hora_dia")
    )

    df_hora.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .save(GOLD_PATH + 'hora/')

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.tb_sinistros_transito_hora
        USING DELTA LOCATION '{GOLD_PATH}hora/'
    """)
    print('Tabela hora salva.')

    # Tabela Mês
    df_mes = (
        df_silver
        .groupBy("mes")
        .agg(
            F.count("mes").alias("total_acidentes"),
            F.sum("vitimas").alias("total_vitimas"),
            F.sum("vitimasfatais").alias("total_fatais")
        )
        .withColumn("taxa_fatalidade",
            F.when(F.col("total_vitimas") > 0,
                   F.col("total_fatais") / F.col("total_vitimas")
            ).otherwise(F.lit(0))
        )
        .orderBy("mes")
    )

    df_mes.write.format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true") \
        .option(f'fs.azure.account.key.{storage_account}.dfs.core.windows.net', storage_key) \
        .save(GOLD_PATH + 'mes/')

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS gold.tb_sinistros_transito_mes
        USING DELTA LOCATION '{GOLD_PATH}mes/'
    """)
    print('Tabela mes salva.')

    dbutils.notebook.exit("SUCCESS: Gold processado com sucesso. 4 tabelas geradas: bairro, transporte, hora, mes")

except Exception as e:
    error_msg = str(e)

    if "SUCCESS" in error_msg:
        dbutils.notebook.exit(error_msg)

    print(f'ERRO no Gold: {error_msg}')
    dbutils.notebook.exit(f"ERROR: Gold falhou. Detalhes: {error_msg}")
