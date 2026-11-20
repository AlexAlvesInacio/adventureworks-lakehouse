# Databricks notebook source
# MAGIC %md
# MAGIC # Ouro: modelo estrela de vendas
# MAGIC Fato no grão de item de pedido e dimensões de data, cliente, produto, território e vendedor.
# MAGIC Recriado por completo a cada execução. Termina com validação contra a prata.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema_silver", "adventureworks_silver")
dbutils.widgets.text("schema_gold", "adventureworks_gold")

catalogo = dbutils.widgets.get("catalogo")
s = f"{catalogo}.{dbutils.widgets.get('schema_silver')}"
g = f"{catalogo}.{dbutils.widgets.get('schema_gold')}"

print(f"Origem: {s} | Destino: {g}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {g}")

# COMMAND ----------

# Dimensão data
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_data AS
WITH limites AS (
    SELECT MIN(CAST(order_date AS DATE)) AS inicio, MAX(CAST(order_date AS DATE)) AS fim
    FROM {s}.sales_sales_order_header
),
datas AS (
    SELECT explode(sequence(make_date(year(inicio), 1, 1), make_date(year(fim), 12, 31), INTERVAL 1 DAY)) AS data
    FROM limites
)
SELECT
    CAST(date_format(data, 'yyyyMMdd') AS INT) AS data_id,
    data,
    year(data) AS ano,
    quarter(data) AS trimestre,
    month(data) AS mes,
    element_at(array('Janeiro', 'Fevereiro', 'Março', 'Abril', 'Maio', 'Junho', 'Julho',
                     'Agosto', 'Setembro', 'Outubro', 'Novembro', 'Dezembro'), month(data)) AS nome_mes,
    concat(year(data), '-', lpad(month(data), 2, '0')) AS ano_mes,
    day(data) AS dia,
    dayofweek(data) AS dia_semana_numero,
    element_at(array('Domingo', 'Segunda-feira', 'Terça-feira', 'Quarta-feira',
                     'Quinta-feira', 'Sexta-feira', 'Sábado'), dayofweek(data)) AS nome_dia_semana
FROM datas
""")

# COMMAND ----------

# Dimensão cliente
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_cliente AS
SELECT
    c.customer_id AS cliente_id,
    CASE WHEN c.store_id IS NOT NULL THEN 'Loja' ELSE 'Pessoa física' END AS tipo_cliente,
    COALESCE(st.name, concat_ws(' ', p.first_name, p.last_name), 'Sem nome') AS nome_cliente,
    c.territory_id AS territorio_id
FROM {s}.sales_customer AS c
LEFT JOIN {s}.person_person AS p ON p.business_entity_id = c.person_id
LEFT JOIN {s}.sales_store AS st ON st.business_entity_id = c.store_id
""")

# Dimensão produto
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_produto AS
SELECT
    p.product_id AS produto_id,
    p.name AS nome_produto,
    p.product_number AS codigo_produto,
    COALESCE(p.color, 'Sem cor') AS cor,
    COALESCE(sc.name, 'Sem subcategoria') AS subcategoria,
    COALESCE(c.name, 'Sem categoria') AS categoria,
    p.standard_cost AS custo_padrao,
    p.list_price AS preco_lista,
    CAST(p.sell_start_date AS DATE) AS data_inicio_venda,
    CAST(p.sell_end_date AS DATE) AS data_fim_venda
FROM {s}.production_product AS p
LEFT JOIN {s}.production_product_subcategory AS sc ON sc.product_subcategory_id = p.product_subcategory_id
LEFT JOIN {s}.production_product_category AS c ON c.product_category_id = sc.product_category_id
""")

# Dimensão território
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_territorio AS
SELECT
    t.territory_id AS territorio_id,
    t.name AS nome_territorio,
    t.`group` AS grupo,
    t.country_region_code AS codigo_pais,
    cr.name AS nome_pais
FROM {s}.sales_sales_territory AS t
LEFT JOIN {s}.person_country_region AS cr ON cr.country_region_code = t.country_region_code
""")

# Dimensão vendedor, com membro desconhecido -1 para vendas sem vendedor
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.dim_vendedor AS
SELECT
    sp.business_entity_id AS vendedor_id,
    concat_ws(' ', p.first_name, p.last_name) AS nome_vendedor,
    sp.territory_id AS territorio_id
FROM {s}.sales_sales_person AS sp
LEFT JOIN {s}.person_person AS p ON p.business_entity_id = sp.business_entity_id
UNION ALL
SELECT -1, 'Sem vendedor', CAST(NULL AS INT)
""")

# COMMAND ----------

# Fato vendas (grão: item de pedido), com frete e imposto rateados pelo valor líquido
spark.sql(f"""
CREATE OR REPLACE TABLE {g}.fato_vendas AS
WITH itens AS (
    SELECT
        d.sales_order_id,
        d.sales_order_detail_id,
        d.product_id,
        d.order_qty,
        d.unit_price,
        d.unit_price_discount,
        d.order_qty * d.unit_price AS valor_bruto,
        d.order_qty * d.unit_price * d.unit_price_discount AS valor_desconto,
        d.order_qty * d.unit_price * (1 - d.unit_price_discount) AS valor_liquido
    FROM {s}.sales_sales_order_detail AS d
)
SELECT
    i.sales_order_id AS pedido_id,
    i.sales_order_detail_id AS item_pedido_id,
    CAST(date_format(h.order_date, 'yyyyMMdd') AS INT) AS data_pedido_id,
    h.customer_id AS cliente_id,
    i.product_id AS produto_id,
    h.territory_id AS territorio_id,
    COALESCE(h.sales_person_id, -1) AS vendedor_id,
    CASE WHEN CAST(h.online_order_flag AS BOOLEAN) THEN 'Online' ELSE 'Revenda' END AS canal,
    i.order_qty AS quantidade,
    i.unit_price AS preco_unitario,
    i.unit_price_discount AS percentual_desconto,
    i.valor_bruto,
    i.valor_desconto,
    i.valor_liquido,
    h.freight * i.valor_liquido
        / NULLIF(SUM(i.valor_liquido) OVER (PARTITION BY i.sales_order_id), 0) AS frete_rateado,
    h.tax_amt * i.valor_liquido
        / NULLIF(SUM(i.valor_liquido) OVER (PARTITION BY i.sales_order_id), 0) AS imposto_rateado
FROM itens AS i
INNER JOIN {s}.sales_sales_order_header AS h ON h.sales_order_id = i.sales_order_id
""")

# COMMAND ----------

# View larga para painéis e Power BI
spark.sql(f"""
CREATE OR REPLACE VIEW {g}.vw_vendas_detalhadas AS
SELECT
    f.pedido_id, f.item_pedido_id,
    d.data, d.ano, d.trimestre, d.mes, d.nome_mes, d.ano_mes,
    f.canal,
    c.tipo_cliente, c.nome_cliente,
    p.nome_produto, p.subcategoria, p.categoria,
    t.grupo AS grupo_territorio, t.nome_territorio, t.nome_pais,
    v.nome_vendedor,
    f.quantidade, f.preco_unitario, f.percentual_desconto,
    f.valor_bruto, f.valor_desconto, f.valor_liquido,
    f.frete_rateado, f.imposto_rateado
FROM {g}.fato_vendas AS f
INNER JOIN {g}.dim_data AS d ON d.data_id = f.data_pedido_id
INNER JOIN {g}.dim_cliente AS c ON c.cliente_id = f.cliente_id
INNER JOIN {g}.dim_produto AS p ON p.produto_id = f.produto_id
INNER JOIN {g}.dim_territorio AS t ON t.territorio_id = f.territorio_id
INNER JOIN {g}.dim_vendedor AS v ON v.vendedor_id = f.vendedor_id
""")

# COMMAND ----------

# Validação: a fato precisa reproduzir os totais da prata
v = spark.sql(f"""
SELECT
    (SELECT COUNT(*) FROM {g}.fato_vendas) AS linhas_fato,
    (SELECT COUNT(*) FROM {s}.sales_sales_order_detail) AS linhas_prata,
    (SELECT ROUND(SUM(valor_liquido), 2) FROM {g}.fato_vendas) AS liquido_fato,
    (SELECT ROUND(SUM(sub_total), 2) FROM {s}.sales_sales_order_header) AS subtotal_prata,
    (SELECT ROUND(SUM(frete_rateado), 2) FROM {g}.fato_vendas) AS frete_fato,
    (SELECT ROUND(SUM(freight), 2) FROM {s}.sales_sales_order_header) AS frete_prata,
    (SELECT COUNT(*) FROM {g}.vw_vendas_detalhadas) AS linhas_view
""").collect()[0]

display(spark.createDataFrame([v]))

erros = []
if v["linhas_fato"] != v["linhas_prata"]:
    erros.append("quantidade de linhas da fato difere dos itens da prata")
if v["linhas_view"] != v["linhas_fato"]:
    erros.append("a view perdeu linhas nos relacionamentos com as dimensões")
if abs((v["liquido_fato"] or 0) - (v["subtotal_prata"] or 0)) > 1:
    erros.append("valor líquido da fato difere do subtotal dos pedidos")
if abs((v["frete_fato"] or 0) - (v["frete_prata"] or 0)) > 1:
    erros.append("frete rateado difere do frete dos pedidos")

if erros:
    raise RuntimeError("Validação da ouro falhou: " + "; ".join(erros))

print("Validação da ouro concluída sem divergências.")
