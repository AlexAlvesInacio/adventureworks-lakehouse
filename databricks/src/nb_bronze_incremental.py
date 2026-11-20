# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze incremental
# MAGIC Copia as tabelas do schema `stg` do Azure SQL (catálogo federado) para tabelas Delta.
# MAGIC Carga incremental por marca d'água (`ColunaMarco`) com upsert pelas chaves de `ChavesJson`.
# MAGIC Os destinos vêm dos parâmetros do job, definidos por ambiente no bundle.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("catalogo_origem", "azure_adventureworks")
dbutils.widgets.text("schema_bronze", "dev_adventureworks_bronze")

catalogo_destino = dbutils.widgets.get("catalogo")
catalogo_origem = dbutils.widgets.get("catalogo_origem")
schema_destino = dbutils.widgets.get("schema_bronze")

schema_origem = "stg"
tabela_controle = f"{catalogo_origem}.ctl.ConfigTabela"
tabela_log = f"{catalogo_destino}.{schema_destino}.log_cargas"

print(f"Origem: {catalogo_origem}.{schema_origem} | Destino: {catalogo_destino}.{schema_destino}")

# COMMAND ----------

import json
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo_destino}.{schema_destino}")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {tabela_log} (
        data_execucao TIMESTAMP,
        tabela STRING,
        tipo_execucao STRING,
        linhas_processadas BIGINT,
        marco_anterior TIMESTAMP,
        mensagem_erro STRING
    )
""")

inicio_execucao = datetime.now(timezone.utc)
registros_log = []

# COMMAND ----------

controle = (
    spark.table(tabela_controle)
    .filter(F.col("Ativo") == True)  # noqa: E712
    .orderBy("Id")
    .collect()
)
print(f"Tabelas ativas na configuração: {len(controle)}\n")

for item in controle:
    nome_stg = item["TabelaDestino"]
    nome_bronze = nome_stg.lower()
    tipo_carga = (item["TipoCarga"] or "").upper()
    coluna_marco = item["ColunaMarco"]
    chaves = json.loads(item["ChavesJson"]) if item["ChavesJson"] else []

    origem = f"{catalogo_origem}.{schema_origem}.`{nome_stg}`"
    destino = f"{catalogo_destino}.{schema_destino}.`{nome_bronze}`"
    destino_sem_crase = f"{catalogo_destino}.{schema_destino}.{nome_bronze}"

    marco_atual = None

    try:
        destino_existe = spark.catalog.tableExists(destino_sem_crase)

        # Carga completa: tabela FULL, sem marco, sem chave ou primeira execução
        if tipo_carga != "INCREMENTAL" or not coluna_marco or not chaves or not destino_existe:
            spark.sql(f"""
                CREATE OR REPLACE TABLE {destino}
                TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
                AS SELECT * FROM {origem}
            """)
            linhas = spark.table(destino).count()
            registros_log.append((inicio_execucao, nome_bronze, "COMPLETA", linhas, None, None))
            print(f"COMPLETA     {nome_bronze}: {linhas} linhas")
            continue

        # Marco próprio da bronze: maior data já presente
        marco_atual = spark.table(destino).agg(F.max(F.col(f"`{coluna_marco}`")).alias("marco")).collect()[0]["marco"]

        df_origem = spark.table(origem)
        if marco_atual is not None:
            df_origem = df_origem.filter(F.col(f"`{coluna_marco}`") >= F.lit(marco_atual))

        quantidade = df_origem.count()
        if quantidade == 0:
            registros_log.append((inicio_execucao, nome_bronze, "SEM_NOVIDADE", 0, marco_atual, None))
            print(f"SEM NOVIDADE {nome_bronze}")
            continue

        # Upsert pela chave primária
        condicao = " AND ".join([f"d.`{c}` = o.`{c}`" for c in chaves])
        (
            DeltaTable.forName(spark, destino_sem_crase)
            .alias("d")
            .merge(df_origem.alias("o"), condicao)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        registros_log.append((inicio_execucao, nome_bronze, "INCREMENTAL", quantidade, marco_atual, None))
        print(f"INCREMENTAL  {nome_bronze}: {quantidade} linhas desde {marco_atual}")

    except Exception as erro:
        registros_log.append((inicio_execucao, nome_bronze, "FALHA", None, marco_atual, str(erro)[:1000]))
        print(f"FALHA        {nome_bronze}")

# COMMAND ----------

df_log = spark.createDataFrame(
    registros_log,
    schema="data_execucao TIMESTAMP, tabela STRING, tipo_execucao STRING, "
    "linhas_processadas BIGINT, marco_anterior TIMESTAMP, mensagem_erro STRING",
)
df_log.write.mode("append").saveAsTable(tabela_log)

display(df_log.groupBy("tipo_execucao").count().orderBy("tipo_execucao"))

falhas = df_log.filter(F.col("tipo_execucao") == "FALHA")
if falhas.count() > 0:
    display(falhas.select("tabela", "mensagem_erro"))
    raise RuntimeError(f"{falhas.count()} tabela(s) falharam na bronze. Veja {tabela_log}.")
