# Databricks notebook source
# MAGIC %md
# MAGIC # Ouro: financeiro de vendas (receita, custo e margem)
# MAGIC Acrescenta à fato de vendas o custo vigente de cada produto na data do pedido, o lucro bruto e a margem.
# MAGIC Gera uma fato financeira no grão de item, um agregado diário para painéis e uma view mensal.
# MAGIC Termina com validação contra a fato_vendas.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema_silver", "dev_adventureworks_silver")
dbutils.widgets.text("schema_gold", "dev_adventureworks_gold")

catalogo = dbutils.widgets.get("catalogo")
s = f"{catalogo}.{dbutils.widgets.get('schema_silver')}"
g = f"{catalogo}.{dbutils.widgets.get('schema_gold')}"

print(f"Origem: {s} e {g}.fato_vendas | Destino: {g}")

# COMMAND ----------

# Fato financeira (grão: item de pedido).
# Custo unitário = custo padrão vigente na data do pedido, pelo histórico de custos do produto.
# Se não houver histórico anterior à data, usa o custo padrão do cadastro do produto.
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.fato_financeiro_vendas AS
WITH vendas AS (
    SELECT f.*, d.data AS data_pedido
    FROM {g}.fato_vendas AS f
    INNER JOIN {g}.dim_data AS d ON d.data_id = f.data_pedido_id
),
historico AS (
    SELECT
        v.pedido_id,
        v.item_pedido_id,
        h.standard_cost,
        ROW_NUMBER() OVER (
            PARTITION BY v.pedido_id, v.item_pedido_id
            ORDER BY h.start_date DESC
        ) AS ordem
    FROM vendas AS v
    INNER JOIN {s}.production_product_cost_history AS h
        ON h.product_id = v.produto_id
       AND CAST(h.start_date AS DATE) <= v.data_pedido
),
custo_vigente AS (
    SELECT pedido_id, item_pedido_id, standard_cost
    FROM historico
    WHERE ordem = 1
)
SELECT
    v.pedido_id,
    v.item_pedido_id,
    v.data_pedido_id,
    v.data_pedido,
    v.cliente_id,
    v.produto_id,
    v.territorio_id,
    v.vendedor_id,
    v.canal,
    v.quantidade,
    v.valor_bruto,
    v.valor_desconto,
    v.valor_liquido,
    v.frete_rateado,
    v.imposto_rateado,
    COALESCE(c.standard_cost, p.custo_padrao) AS custo_unitario,
    CASE WHEN c.standard_cost IS NULL THEN 'cadastro_produto' ELSE 'historico_custo' END AS origem_custo,
    v.quantidade * COALESCE(c.standard_cost, p.custo_padrao) AS custo_total,
    v.valor_liquido - v.quantidade * COALESCE(c.standard_cost, p.custo_padrao) AS lucro_bruto
FROM vendas AS v
LEFT JOIN custo_vigente AS c
    ON c.pedido_id = v.pedido_id AND c.item_pedido_id = v.item_pedido_id
LEFT JOIN {g}.dim_produto AS p
    ON p.produto_id = v.produto_id
""")

# COMMAND ----------

# Agregado diário para painéis (Grafana e Power BI).
# Atenção: "pedidos" é contagem distinta dentro de cada combinação; não some pedidos entre categorias.
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.agg_financeiro_diario AS
SELECT
    f.data_pedido AS data,
    CAST(f.data_pedido AS TIMESTAMP) AS tempo,
    f.canal,
    p.categoria,
    t.grupo AS grupo_territorio,
    t.nome_pais,
    COUNT(DISTINCT f.pedido_id) AS pedidos,
    SUM(f.quantidade) AS unidades,
    SUM(f.valor_bruto) AS receita_bruta,
    SUM(f.valor_desconto) AS descontos,
    SUM(f.valor_liquido) AS receita_liquida,
    SUM(f.custo_total) AS custo,
    SUM(f.lucro_bruto) AS lucro_bruto,
    SUM(f.frete_rateado) AS frete,
    SUM(f.imposto_rateado) AS impostos
FROM {g}.fato_financeiro_vendas AS f
INNER JOIN {g}.dim_produto AS p ON p.produto_id = f.produto_id
INNER JOIN {g}.dim_territorio AS t ON t.territorio_id = f.territorio_id
GROUP BY f.data_pedido, f.canal, p.categoria, t.grupo, t.nome_pais
""")

# View mensal: indicadores consolidados por mês e canal, com margem e ticket médio.
spark.sql(f"""
CREATE OR REPLACE VIEW {g}.vw_financeiro_mensal AS
SELECT
    CAST(date_trunc('MONTH', f.data_pedido) AS TIMESTAMP) AS mes,
    f.canal,
    COUNT(DISTINCT f.pedido_id) AS pedidos,
    SUM(f.valor_liquido) AS receita_liquida,
    SUM(f.custo_total) AS custo,
    SUM(f.lucro_bruto) AS lucro_bruto,
    SUM(f.lucro_bruto) / NULLIF(SUM(f.valor_liquido), 0) AS margem_bruta,
    SUM(f.valor_liquido) / NULLIF(COUNT(DISTINCT f.pedido_id), 0) AS ticket_medio,
    SUM(f.valor_desconto) AS descontos,
    SUM(f.frete_rateado) AS frete,
    SUM(f.imposto_rateado) AS impostos
FROM {g}.fato_financeiro_vendas AS f
GROUP BY date_trunc('MONTH', f.data_pedido), f.canal
""")

# COMMAND ----------

# Validação: a camada financeira precisa reproduzir a fato_vendas e ter custo para todos os itens
v = spark.sql(f"""
SELECT
    (SELECT COUNT(*) FROM {g}.fato_vendas) AS linhas_vendas,
    (SELECT COUNT(*) FROM {g}.fato_financeiro_vendas) AS linhas_financeiro,
    (SELECT ROUND(SUM(valor_liquido), 2) FROM {g}.fato_vendas) AS receita_vendas,
    (SELECT ROUND(SUM(valor_liquido), 2) FROM {g}.fato_financeiro_vendas) AS receita_financeiro,
    (SELECT ROUND(SUM(receita_liquida), 2) FROM {g}.agg_financeiro_diario) AS receita_agregado,
    (SELECT COUNT(*) FROM {g}.fato_financeiro_vendas WHERE custo_unitario IS NULL) AS itens_sem_custo,
    (SELECT ROUND(SUM(custo_total), 2) FROM {g}.fato_financeiro_vendas) AS custo_total,
    (SELECT ROUND(SUM(lucro_bruto), 2) FROM {g}.fato_financeiro_vendas) AS lucro_bruto,
    (SELECT ROUND(100 * AVG(CASE WHEN origem_custo = 'historico_custo' THEN 1 ELSE 0 END), 1)
       FROM {g}.fato_financeiro_vendas) AS pct_custo_historico
""").collect()[0]

display(spark.createDataFrame([v]))

erros = []
if v["linhas_financeiro"] != v["linhas_vendas"]:
    erros.append("quantidade de linhas difere da fato_vendas")
if abs((v["receita_financeiro"] or 0) - (v["receita_vendas"] or 0)) > 1:
    erros.append("receita da fato financeira difere da fato_vendas")
if abs((v["receita_agregado"] or 0) - (v["receita_vendas"] or 0)) > 1:
    erros.append("receita do agregado diário difere da fato_vendas")
if v["itens_sem_custo"] != 0:
    erros.append(f"{v['itens_sem_custo']} itens sem custo")
if not v["custo_total"] or v["custo_total"] <= 0:
    erros.append("custo total zerado")

if erros:
    raise RuntimeError("Validação do financeiro falhou: " + "; ".join(erros))

print("Validação do financeiro concluída sem divergências.")
