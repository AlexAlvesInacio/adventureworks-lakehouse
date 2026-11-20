"""Funções de padronização de nomes usadas na camada prata."""

import re


def para_snake_case(nome: str) -> str:
    """Converte um nome de coluna ou tabela para snake_case.

    Exemplos:
        ProductCategoryID          -> product_category_id
        Database Version           -> database_version
        Sales_SalesOrderHeader     -> sales_sales_order_header
        AWBuildVersion             -> aw_build_version
    """
    texto = re.sub(r"[^0-9a-zA-Z]+", "_", nome.strip())
    texto = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", texto)
    texto = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", texto)
    texto = re.sub(r"_+", "_", texto).strip("_")
    return texto.lower()
