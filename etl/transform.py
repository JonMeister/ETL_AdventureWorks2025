"""Módulo de transformación (Transform) hacia el modelo dimensional de AdventureWorks."""

from __future__ import annotations

from typing import Any

import pandas as pd

from etl.utils_etl import parse_xml_dict

_ENGLISH_DAYS = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}

_SPANISH_DAYS = {
    0: "Lunes",
    1: "Martes",
    2: "Miércoles",
    3: "Jueves",
    4: "Viernes",
    5: "Sábado",
    6: "Domingo",
}

_ENGLISH_MONTHS = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

_SPANISH_MONTHS = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}

_INCOME_RANGE_MIDPOINT = {
    "0-25000": 15000.0,
    "25001-50000": 37500.0,
    "50001-75000": 62500.0,
    "75001-100000": 87500.0,
    "greater than 100000": 120000.0,
}

_BUSINESS_TYPE_MAP = {
    "BM": "Value Added Reseller",
    "BS": "Specialty Bike Shop",
    "OS": "Warehouse",
}


def transform_date(min_date: pd.Timestamp, max_date: pd.Timestamp) -> pd.DataFrame:
    """Genera la dimensión de tiempo completa cubriendo todo el rango anual de órdenes."""
    start = pd.Timestamp(year=min_date.year, month=1, day=1)
    end = pd.Timestamp(year=max_date.year, month=12, day=31)
    dates = pd.date_range(start=start, end=end, freq="D")

    records = []
    for dt in dates:
        py_wd = dt.weekday()  # 0=Mon .. 6=Sun
        day_of_week = ((py_wd + 1) % 7) + 1  # 1=Sun .. 7=Sat (estándar SQL Server DW)
        month = dt.month
        cal_quarter = (month - 1) // 3 + 1
        cal_semester = 1 if month <= 6 else 2

        # Año fiscal AdventureWorks inicia el 1 de julio
        fiscal_year = dt.year if month >= 7 else dt.year - 1
        fiscal_month_offset = (month - 7) % 12
        fiscal_quarter = fiscal_month_offset // 3 + 1
        fiscal_semester = 1 if fiscal_quarter <= 2 else 2

        records.append(
            {
                "date_key": int(dt.strftime("%Y%m%d")),
                "full_date_alternate_key": dt.date(),
                "day_number_of_week": day_of_week,
                "english_day_name_of_week": _ENGLISH_DAYS[py_wd],
                "spanish_day_name_of_week": _SPANISH_DAYS[py_wd],
                "day_number_of_month": dt.day,
                "day_number_of_year": dt.dayofyear,
                "week_number_of_year": int(dt.isocalendar().week),
                "english_month_name": _ENGLISH_MONTHS[month],
                "spanish_month_name": _SPANISH_MONTHS[month],
                "month_number_of_year": month,
                "calendar_quarter": cal_quarter,
                "calendar_year": dt.year,
                "calendar_semester": cal_semester,
                "fiscal_quarter": fiscal_quarter,
                "fiscal_year": fiscal_year,
                "fiscal_semester": fiscal_semester,
            }
        )
    return pd.DataFrame(records)


def transform_geography(
    df_geo: pd.DataFrame,
    territory_map: dict[int, int],
) -> pd.DataFrame:
    """Normaliza la dimensión geográfica y asigna la llave subrogada de territorio."""
    df = df_geo.copy()
    df["sales_territory_key"] = df["territory_id_oltp"].map(territory_map)
    df = df.drop(columns=["territory_id_oltp"])
    df = df.drop_duplicates(
        subset=["city", "state_province_code", "postal_code", "country_region_code"]
    ).reset_index(drop=True)
    return df


def transform_product_subcategory(
    df_sub: pd.DataFrame,
    category_map: dict[int, int],
) -> pd.DataFrame:
    """Asigna product_category_key a cada subcategoría."""
    df = df_sub.copy()
    df["product_category_key"] = df["product_category_alternate_key"].map(category_map)
    return df.drop(columns=["product_category_alternate_key"])


def transform_product(
    df_prod: pd.DataFrame,
    subcategory_map: dict[int, int],
) -> pd.DataFrame:
    """Limpia atributos del producto, asigna subcategoría y deriva el estado."""
    df = df_prod.copy()
    df["product_subcategory_key"] = (
        df["product_subcategory_alternate_key"].map(subcategory_map).astype("Int64")
    )
    df["color"] = df["color"].fillna("NA")
    df["finished_goods_flag"] = df["finished_goods_flag"].astype(bool)
    df["status"] = df["end_date"].apply(
        lambda x: "Current" if pd.isna(x) else "Discontinued"
    )
    return df.drop(columns=["product_subcategory_alternate_key"])


def transform_promotion(df_promo: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos anulables en la dimensión de promociones."""
    df = df_promo.copy()
    df["min_qty"] = df["min_qty"].astype("Int64")
    df["max_qty"] = df["max_qty"].astype("Int64")
    return df


def _clean_iso_date(val: str | None) -> Any:
    """Convierte fechas XML tipo '2003-09-01Z' en objetos date o None."""
    if not val:
        return None
    cleaned = val.rstrip("Z").strip()
    parsed = pd.to_datetime(cleaned, errors="coerce")
    return None if pd.isna(parsed) else parsed.date()


def _to_nullable_int(val: str | None) -> int | None:
    if val is None or val == "":
        return None
    try:
        return int(float(val))
    except ValueError:
        return None


def _to_nullable_float(val: str | None) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except ValueError:
        return None


def _attach_geography_key(
    df: pd.DataFrame,
    geo_lookup: dict[tuple[Any, Any, Any], int],
) -> pd.DataFrame:
    """Asocia geography_key usando la tupla (city, state_province_code, postal_code)."""
    keys = [
        geo_lookup.get((row.city, row.state_province_code, row.postal_code))
        for row in df.itertuples(index=False)
    ]
    df["geography_key"] = pd.array(keys, dtype="Int64")
    return df.drop(columns=["city", "state_province_code", "postal_code"])


def transform_customer(
    df_cust: pd.DataFrame,
    geo_lookup: dict[tuple[Any, Any, Any], int],
) -> pd.DataFrame:
    """Parsea el XML demográfico de clientes individuales y resuelve su ubicación."""
    df = df_cust.copy()
    parsed_xml = [parse_xml_dict(x) for x in df["demographics_xml"]]

    df["birth_date"] = [_clean_iso_date(d.get("BirthDate")) for d in parsed_xml]
    df["date_first_purchase"] = [
        _clean_iso_date(d.get("DateFirstPurchase")) for d in parsed_xml
    ]
    df["marital_status"] = [d.get("MaritalStatus") for d in parsed_xml]
    df["gender"] = [d.get("Gender") for d in parsed_xml]
    df["yearly_income_range"] = [d.get("YearlyIncome") for d in parsed_xml]
    df["yearly_income"] = [
        _INCOME_RANGE_MIDPOINT.get(d.get("YearlyIncome")) for d in parsed_xml
    ]
    df["total_children"] = pd.array(
        [_to_nullable_int(d.get("TotalChildren")) for d in parsed_xml], dtype="Int64"
    )
    df["number_children_at_home"] = pd.array(
        [_to_nullable_int(d.get("NumberChildrenAtHome")) for d in parsed_xml],
        dtype="Int64",
    )
    df["english_education"] = [d.get("Education") for d in parsed_xml]
    df["english_occupation"] = [d.get("Occupation") for d in parsed_xml]
    df["house_owner_flag"] = [d.get("HomeOwnerFlag") for d in parsed_xml]
    df["number_cars_owned"] = pd.array(
        [_to_nullable_int(d.get("NumberCarsOwned")) for d in parsed_xml], dtype="Int64"
    )
    df["commute_distance"] = [d.get("CommuteDistance") for d in parsed_xml]
    df["name_style"] = df["name_style"].astype(bool)

    df = _attach_geography_key(df, geo_lookup)
    return df.drop(columns=["demographics_xml"])


def transform_reseller(
    df_reseller: pd.DataFrame,
    geo_lookup: dict[tuple[Any, Any, Any], int],
) -> pd.DataFrame:
    """Parsea la encuesta XML de tiendas revendedoras y homologa el tipo de negocio."""
    df = df_reseller.copy()
    parsed_xml = [parse_xml_dict(x) for x in df["demographics_xml"]]

    df["business_type"] = [
        _BUSINESS_TYPE_MAP.get(d.get("BusinessType", ""), "Specialty Bike Shop")
        for d in parsed_xml
    ]
    df["number_employees"] = pd.array(
        [_to_nullable_int(d.get("NumberEmployees")) for d in parsed_xml], dtype="Int64"
    )
    df["annual_sales"] = [_to_nullable_float(d.get("AnnualSales")) for d in parsed_xml]
    df["annual_revenue"] = [
        _to_nullable_float(d.get("AnnualRevenue")) for d in parsed_xml
    ]
    df["bank_name"] = [d.get("BankName") for d in parsed_xml]
    df["year_opened"] = pd.array(
        [_to_nullable_int(d.get("YearOpened")) for d in parsed_xml], dtype="Int64"
    )
    df["product_line"] = [d.get("Specialty") for d in parsed_xml]
    df["order_frequency"] = None
    df["order_month"] = pd.array([None] * len(df), dtype="Int64")
    df["first_order_year"] = df["first_order_year"].astype("Int64")
    df["last_order_year"] = df["last_order_year"].astype("Int64")
    df["phone"] = None

    df = _attach_geography_key(df, geo_lookup)
    return df.drop(columns=["demographics_xml"])


def transform_employee(
    df_emp: pd.DataFrame,
    territory_map: dict[int, int],
) -> pd.DataFrame:
    """Limpia atributos de empleados/vendedores y asigna territorio de ventas."""
    df = df_emp.copy()
    df["sales_territory_key"] = df["territory_id_oltp"].map(territory_map).astype("Int64")
    df["name_style"] = df["name_style"].astype(bool)
    df["salaried_flag"] = df["salaried_flag"].astype(bool)
    df["current_flag"] = df["current_flag"].astype(bool)
    df["sales_person_flag"] = df["sales_person_flag"].astype(bool)
    df["status"] = df["current_flag"].apply(lambda x: "Current" if x else "Inactive")
    return df.drop(columns=["territory_id_oltp"])


def _date_to_key(series: pd.Series) -> pd.Series:
    """Convierte una columna datetime/date al formato entero YYYYMMDD."""
    return pd.to_datetime(series).dt.strftime("%Y%m%d").astype(int)


def transform_sales_facts(
    df_lines: pd.DataFrame,
    key_maps: dict[str, dict[Any, int]],
    fact_type: str,
) -> pd.DataFrame:
    """Transforma líneas de orden en la tabla de hechos correspondiente ('internet' o 'reseller')."""
    df = df_lines.copy()

    # Llaves conformadas
    df["product_key"] = df["product_id_oltp"].map(key_maps["product"])
    df["order_date_key"] = _date_to_key(df["order_date"])
    df["due_date_key"] = _date_to_key(df["due_date"])
    df["ship_date_key"] = _date_to_key(df["ship_date"])
    df["promotion_key"] = df["promotion_alternate_key"].map(key_maps["promotion"])
    df["currency_key"] = df["currency_alternate_key"].map(key_maps["currency"])
    df["sales_territory_key"] = df["territory_id_oltp"].map(key_maps["territory"])

    # Llaves específicas de canal
    if fact_type == "internet":
        df["customer_key"] = df["customer_id_oltp"].map(key_maps["customer"])
        channel_cols = ["customer_key"]
    elif fact_type == "reseller":
        df["reseller_key"] = df["store_id_oltp"].map(key_maps["reseller"])
        df["employee_key"] = df["employee_id_oltp"].map(key_maps["employee"])
        channel_cols = ["reseller_key", "employee_key"]
    else:
        raise ValueError(f"Tipo de hecho no soportado: {fact_type}")

    # Cálculo de medidas e indicadores financieros
    qty = df["order_quantity"].astype(float)
    unit_price = df["unit_price"].astype(float)
    disc_pct = df["unit_price_discount_pct"].astype(float)
    std_cost = df["product_standard_cost"].astype(float)
    line_total = df["line_total"].astype(float)
    header_sub = df["header_subtotal"].astype(float).replace(0.0, 1.0)
    weight_ratio = line_total / header_sub

    df["extended_amount"] = (qty * unit_price).round(4)
    df["discount_amount"] = (df["extended_amount"] * disc_pct).round(4)
    df["total_product_cost"] = (qty * std_cost).round(4)
    df["sales_amount"] = (df["extended_amount"] - df["discount_amount"]).round(4)
    df["tax_amt"] = (weight_ratio * df["header_tax_amt"].astype(float)).round(4)
    df["freight"] = (weight_ratio * df["header_freight"].astype(float)).round(4)

    ordered_cols = [
        "product_key",
        "order_date_key",
        "due_date_key",
        "ship_date_key",
        *channel_cols,
        "promotion_key",
        "currency_key",
        "sales_territory_key",
        "sales_order_number",
        "sales_order_line_number",
        "revision_number",
        "order_quantity",
        "unit_price",
        "extended_amount",
        "unit_price_discount_pct",
        "discount_amount",
        "product_standard_cost",
        "total_product_cost",
        "sales_amount",
        "tax_amt",
        "freight",
        "carrier_tracking_number",
        "customer_po_number",
        "order_date",
        "due_date",
        "ship_date",
    ]
    return df[ordered_cols]


def transform_internet_sales_reason(
    df_reasons: pd.DataFrame,
    reason_map: dict[int, int],
) -> pd.DataFrame:
    """Mapea la tabla puente de motivos de compra por internet."""
    df = df_reasons.copy()
    df["sales_reason_key"] = df["sales_reason_alternate_key"].map(reason_map)
    return df[["sales_order_number", "sales_order_line_number", "sales_reason_key"]]

