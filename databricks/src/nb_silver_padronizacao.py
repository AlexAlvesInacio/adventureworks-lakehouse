# Databricks notebook source
# MAGIC %md
# MAGIC # Prata: padronização genérica
# MAGIC Lê a bronze, padroniza nomes (snake_case), remove colunas técnicas, trata textos,
# MAGIC garante uma linha por chave e grava de forma incremental com upsert.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("catalogo_origem", "azure_adventureworks")
dbutils.widgets.text("schema_bronze", "dev_adventureworks_bronze")
dbutils.widgets.text("schema_silver", "dev_adventureworks_silver")

catalogo = dbutils.widgets.get("catalogo")
catalogo_origem = dbutils.widgets.get("catalogo_origem")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")

tabela_controle = f"{catalogo_origem}.ctl.ConfigTabela"
tabela_log = f"{catalogo}.{schema_silver}.log_cargas"

colunas_removidas = {"rowguid"}
coluna_auditoria = "data_processamento_silver"

print(f"Origem: {catalogo}.{schema_bronze} | Destino: {catalogo}.{schema_silver}")

# COMMAND ----------

import json
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.nomes import para_snake_case

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema_silver}")

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

# COMMAND ----------


def padronizar(df):
    colunas = []
    for campo in df.schema.fields:
        nome_novo = para_snake_case(campo.name)
        if nome_novo in colunas_removidas:
            continue

        coluna = F.col(f"`{campo.name}`")
        if campo.dataType.simpleString() == "string":
            coluna = F.when(F.trim(coluna) == "", None).otherwise(F.trim(coluna))

        colunas.append(coluna.alias(nome_novo))

    return df.select(*colunas).withColumn(coluna_auditoria, F.current_timestamp())


def remover_duplicados(df, chaves, coluna_marco):
    if not chaves:
        return df.dropDuplicates()

    ordem = F.col(coluna_marco).desc() if coluna_marco else F.lit(1)
    janela = Window.partitionBy(*chaves).orderBy(ordem)
    return df.withColumn("_ordem", F.row_number().over(janela)).filter(F.col("_ordem") == 1).drop("_ordem")


# COMMAND ----------

inicio_execucao = datetime.now(timezone.utc)
registros_log = []

controle = (
    spark.table(tabela_controle)
    .filter(F.col("Ativo") == True)  # noqa: E712
    .orderBy("Id")
    .collect()
)
print(f"Tabelas ativas na configuração: {len(controle)}\n")

for item in controle:
    nome_original = item["TabelaDestino"]
    nome_bronze = nome_original.lower()
    nome_silver = para_snake_case(nome_original)

    tipo_carga = (item["TipoCarga"] or "").upper()
    marco_bronze = item["ColunaMarco"]
    marco_silver = para_snake_case(marco_bronze) if marco_bronze else None

    chaves_originais = json.loads(item["ChavesJson"]) if item["ChavesJson"] else []
    chaves_silver = [para_snake_case(c) for c in chaves_originais]

    origem = f"{catalogo}.{schema_bronze}.`{nome_bronze}`"
    destino = f"{catalogo}.{schema_silver}.{nome_silver}"

    marco_atual = None

    try:
        destino_existe = spark.catalog.tableExists(destino)

        # Carga completa
        if tipo_carga != "INCREMENTAL" or not marco_silver or not chaves_silver or not destino_existe:
            df = remover_duplicados(padronizar(spark.table(origem)), chaves_silver, marco_silver)
            df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(destino)

            linhas = spark.table(destino).count()
            registros_log.append((inicio_execucao, nome_silver, "COMPLETA", linhas, None, None))
            print(f"COMPLETA     {nome_silver}: {linhas} linhas")
            continue

        # Marco próprio da prata
        marco_atual = spark.table(destino).agg(F.max(F.col(marco_silver)).alias("marco")).collect()[0]["marco"]

        df_bronze = spark.table(origem)
        if marco_atual is not None:
            df_bronze = df_bronze.filter(F.col(f"`{marco_bronze}`") >= F.lit(marco_atual))

        df = remover_duplicados(padronizar(df_bronze), chaves_silver, marco_silver)

        quantidade = df.count()
        if quantidade == 0:
            registros_log.append((inicio_execucao, nome_silver, "SEM_NOVIDADE", 0, marco_atual, None))
            print(f"SEM NOVIDADE {nome_silver}")
            continue

        condicao = " AND ".join([f"d.{c} = o.{c}" for c in chaves_silver])
        (
            DeltaTable.forName(spark, destino)
            .alias("d")
            .merge(df.alias("o"), condicao)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        registros_log.append((inicio_execucao, nome_silver, "INCREMENTAL", quantidade, marco_atual, None))
        print(f"INCREMENTAL  {nome_silver}: {quantidade} linhas desde {marco_atual}")

    except Exception as erro:
        registros_log.append((inicio_execucao, nome_silver, "FALHA", None, marco_atual, str(erro)[:1000]))
        print(f"FALHA        {nome_silver}")

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
    raise RuntimeError(f"{falhas.count()} tabela(s) falharam na prata. Veja {tabela_log}.")
