# Databricks notebook source
# MAGIC %md
# MAGIC # Arquitetura Medallion
# MAGIC
# MAGIC  Vamos criar os banco de dados (schemas) para cada camada da estrutura medallion: 
# MAGIC
# MAGIC  **Bronze** -> **Silver** -> **Gold** -> **Analytics**. 
# MAGIC
# MAGIC  Onde:
# MAGIC
# MAGIC  **Bronze**: ingestão de dados brutos.
# MAGIC  **Silver**: limpeza, tratamento e padronização.
# MAGIC  **Gold**: tabelas analíticas e métricas.

# COMMAND ----------


dbutils.widgets.text("env", "prod")
env = dbutils.widgets.get("env")
print(f"Ambiente: {env}")

# COMMAND ----------


try:

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS bronze
        LOCATION 'abfss://bronze@stsinistrosrecife.dfs.core.windows.net/'
    """)

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS silver
        LOCATION 'abfss://silver@stsinistrosrecife.dfs.core.windows.net/'
    """)

    spark.sql("""
        CREATE DATABASE IF NOT EXISTS gold
        LOCATION 'abfss://gold@stsinistrosrecife.dfs.core.windows.net/'
    """)

    print('Databases bronze, silver e gold criados com sucesso.')

    dbutils.notebook.exit("SUCCESS: Medallion databases criados com sucesso.")

except Exception as e:
    error_msg = str(e)

    if "SUCCESS" in error_msg:
        dbutils.notebook.exit(error_msg)

    print(f'ERRO no Medallion: {error_msg}')
    dbutils.notebook.exit(f"ERROR: Medallion falhou. Detalhes: {error_msg}")
