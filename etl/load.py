"""Módulo de carga (Load) hacia la bodega de datos en PostgreSQL."""

from __future__ import annotations

from typing import Any

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def load_table(
    df: pd.DataFrame,
    dw_engine: Engine,
    table_name: str,
    chunk_size: int = 5000,
) -> None:
    """Inserta un DataFrame en una tabla destino de PostgreSQL en bloques."""
    if df.empty:
        print(f"[LOAD] {table_name}: 0 registros (sin datos para insertar).")
        return

    df_clean = df.astype(object).where(pd.notna(df), None)
    df_clean.to_sql(
        name=table_name,
        con=dw_engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=chunk_size,
    )
    print(f"[LOAD] {table_name}: {len(df):,} registros cargados.")


def build_geography_lookup(
    dw_engine: Engine,
) -> dict[tuple[Any, Any, Any], int]:
    """Construye el índice {(city, state_province_code, postal_code): geography_key}."""
    query = text(
        "SELECT city, state_province_code, postal_code, geography_key FROM dim_geography;"
    )
    with dw_engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return {(r[0], r[1], r[2]): r[3] for r in rows}


def resolve_employee_hierarchy(dw_engine: Engine) -> None:
    """Resuelve la relación jerárquica auto-referenciada parent_employee_key en dim_employee."""
    sql = text("""
        UPDATE dim_employee AS emp
        SET parent_employee_key = mgr.employee_key
        FROM dim_employee AS mgr
        WHERE emp.parent_employee_national_id_alternate_key = mgr.employee_national_id_alternate_key;
    """)
    with dw_engine.begin() as conn:
        conn.execute(sql)
    print("[LOAD] Jerarquía organizacional (parent_employee_key) actualizada en dim_employee.")


def verify_load_counts(dw_engine: Engine) -> dict[str, int]:
    """Consulta el conteo final de filas cargadas en cada tabla del esquema dimensional."""
    tables = [
        "dim_date",
        "dim_currency",
        "dim_sales_territory",
        "dim_geography",
        "dim_product_category",
        "dim_product_subcategory",
        "dim_product",
        "dim_promotion",
        "dim_customer",
        "dim_reseller",
        "dim_employee",
        "dim_sales_reason",
        "fact_internet_sales",
        "fact_internet_sales_reason",
        "fact_reseller_sales",
    ]
    counts: dict[str, int] = {}
    with dw_engine.connect() as conn:
        for table in tables:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table};")).scalar_one()
            counts[table] = int(count)
    return counts

