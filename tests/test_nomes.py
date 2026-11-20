import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "databricks" / "src"))

from utils.nomes import para_snake_case  # noqa: E402


@pytest.mark.parametrize(
    ("entrada", "esperado"),
    [
        ("ProductCategoryID", "product_category_id"),
        ("SalesOrderID", "sales_order_id"),
        ("ModifiedDate", "modified_date"),
        ("rowguid", "rowguid"),
        ("Database Version", "database_version"),
        ("AddressLine1", "address_line1"),
        ("Sales_SalesOrderHeader", "sales_sales_order_header"),
        ("dbo_AWBuildVersion", "dbo_aw_build_version"),
        ("  Order   Qty ", "order_qty"),
    ],
)
def test_para_snake_case(entrada, esperado):
    assert para_snake_case(entrada) == esperado


def test_resultado_e_idempotente():
    nome = para_snake_case("SalesOrderHeaderSalesReason")
    assert para_snake_case(nome) == nome
