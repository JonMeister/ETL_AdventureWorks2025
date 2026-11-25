"""Orquestador principal del ETL: AdventureWorks2025 (OLTP) -> PostgreSQL (Datamarts)."""

from __future__ import annotations

import time
from pathlib import Path

from sqlalchemy.engine import Engine

from etl import extract, load, transform, utils_etl

BASE_DIR = Path(__file__).resolve().parent


def load_dimensions(oltp_engine: Engine, dw_engine: Engine, chunk_size: int) -> None:
    """Extrae, transforma y carga todas las dimensiones respetando su orden jerárquico."""
    print("\n--- FASE 1: DIMENSIONES INDEPENDIENTES Y CONFORMADAS ---")

    min_date, max_date = extract.extract_date_bounds(oltp_engine)
    df_date = transform.transform_date(min_date, max_date)
    load.load_table(df_date, dw_engine, "dim_date", chunk_size)

    df_currency = extract.extract_currency(oltp_engine)
    load.load_table(df_currency, dw_engine, "dim_currency", chunk_size)

    df_territory = extract.extract_sales_territory(oltp_engine)
    load.load_table(df_territory, dw_engine, "dim_sales_territory", chunk_size)

    df_cat = extract.extract_product_category(oltp_engine)
    load.load_table(df_cat, dw_engine, "dim_product_category", chunk_size)

    df_promo = transform.transform_promotion(extract.extract_promotion(oltp_engine))
    load.load_table(df_promo, dw_engine, "dim_promotion", chunk_size)

    df_reason = extract.extract_sales_reason(oltp_engine)
    load.load_table(df_reason, dw_engine, "dim_sales_reason", chunk_size)

    print("\n--- FASE 2: DIMENSIONES JERÁRQUICAS Y ESPECÍFICAS DE CANAL ---")

    territory_map = utils_etl.fetch_surrogate_map(
        dw_engine, "dim_sales_territory", "sales_territory_alternate_key", "sales_territory_key"
    )
    category_map = utils_etl.fetch_surrogate_map(
        dw_engine, "dim_product_category", "product_category_alternate_key", "product_category_key"
    )

    df_geo = transform.transform_geography(
        extract.extract_geography(oltp_engine), territory_map
    )
    load.load_table(df_geo, dw_engine, "dim_geography", chunk_size)

    df_subcat = transform.transform_product_subcategory(
        extract.extract_product_subcategory(oltp_engine), category_map
    )
    load.load_table(df_subcat, dw_engine, "dim_product_subcategory", chunk_size)

    subcat_map = utils_etl.fetch_surrogate_map(
        dw_engine, "dim_product_subcategory", "product_subcategory_alternate_key", "product_subcategory_key"
    )
    df_prod = transform.transform_product(extract.extract_product(oltp_engine), subcat_map)
    load.load_table(df_prod, dw_engine, "dim_product", chunk_size)

    geo_lookup = load.build_geography_lookup(dw_engine)

    df_customer = transform.transform_customer(
        extract.extract_customer(oltp_engine), geo_lookup
    )
    load.load_table(df_customer, dw_engine, "dim_customer", chunk_size)

    df_reseller = transform.transform_reseller(
        extract.extract_reseller(oltp_engine), geo_lookup
    )
    load.load_table(df_reseller, dw_engine, "dim_reseller", chunk_size)

    df_employee = transform.transform_employee(
        extract.extract_employee(oltp_engine), territory_map
    )
    load.load_table(df_employee, dw_engine, "dim_employee", chunk_size)
    load.resolve_employee_hierarchy(dw_engine)


def load_facts(oltp_engine: Engine, dw_engine: Engine, chunk_size: int) -> None:
    """Extrae, transforma y carga las tablas de hechos de Internet Sales y Reseller Sales."""
    print("\n--- FASE 3: TABLAS DE HECHOS Y PUENTE ---")

    key_maps = {
        "product": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_product", "product_id_oltp", "product_key"
        ),
        "promotion": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_promotion", "promotion_alternate_key", "promotion_key"
        ),
        "currency": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_currency", "currency_alternate_key", "currency_key"
        ),
        "territory": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_sales_territory", "sales_territory_alternate_key", "sales_territory_key"
        ),
        "customer": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_customer", "customer_id_oltp", "customer_key"
        ),
        "reseller": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_reseller", "store_id_oltp", "reseller_key"
        ),
        "employee": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_employee", "employee_id_oltp", "employee_key"
        ),
        "reason": utils_etl.fetch_surrogate_map(
            dw_engine, "dim_sales_reason", "sales_reason_alternate_key", "sales_reason_key"
        ),
    }

    # Datamart 1: Ventas por Internet (OnlineOrderFlag = 1)
    df_internet_raw = extract.extract_sales_lines(oltp_engine, online_flag=1)
    df_internet_fact = transform.transform_sales_facts(
        df_internet_raw, key_maps, fact_type="internet"
    )
    load.load_table(df_internet_fact, dw_engine, "fact_internet_sales", chunk_size)

    # Puente: Motivos de Venta por Internet
    df_reasons_raw = extract.extract_internet_sales_reasons(oltp_engine)
    df_reasons_fact = transform.transform_internet_sales_reason(
        df_reasons_raw, key_maps["reason"]
    )
    load.load_table(
        df_reasons_fact, dw_engine, "fact_internet_sales_reason", chunk_size
    )

    # Datamart 2: Ventas por Revendedores (OnlineOrderFlag = 0)
    df_reseller_raw = extract.extract_sales_lines(oltp_engine, online_flag=0)
    df_reseller_fact = transform.transform_sales_facts(
        df_reseller_raw, key_maps, fact_type="reseller"
    )
    load.load_table(df_reseller_fact, dw_engine, "fact_reseller_sales", chunk_size)


def run_etl() -> None:
    """Ejecuta el pipeline completo de extremo a extremo y valida los conteos finales."""
    start_ts = time.perf_counter()
    cfg = utils_etl.load_yaml(str(BASE_DIR / "config.yml"))
    etl_opts = cfg.get("ETL_OPTIONS", {})
    chunk_size = int(etl_opts.get("chunk_size", 5000))
    full_refresh = bool(etl_opts.get("full_refresh", True))

    print("============================================================")
    print("  ETL AdventureWorks2025 (OLTP) -> PostgreSQL (Datamarts)")
    print("============================================================")

    utils_etl.ensure_postgres_database(cfg["DW_TARGET"])
    oltp_engine = utils_etl.build_oltp_engine(cfg["OLTP_SOURCE"])
    dw_engine = utils_etl.build_dw_engine(cfg["DW_TARGET"])

    utils_etl.initialize_schema(
        dw_engine, str(BASE_DIR / "sqlscripts.yml"), full_refresh=full_refresh
    )

    load_dimensions(oltp_engine, dw_engine, chunk_size)
    load_facts(oltp_engine, dw_engine, chunk_size)

    print("\n--- RESUMEN FINAL DE REGISTROS EN POSTGRESQL ---")
    counts = load.verify_load_counts(dw_engine)
    for table, count in counts.items():
        print(f"  {table:<30} {count:>10,}")

    elapsed = time.perf_counter() - start_ts
    print(f"\n[OK] Proceso ETL completado exitosamente en {elapsed:.2f} segundos.")


if __name__ == "__main__":
    run_etl()

