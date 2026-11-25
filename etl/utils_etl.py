"""Utilidades de conexión, inicialización de esquema, parsing XML y mapeo de llaves."""

from __future__ import annotations

import os
import re
import urllib.parse
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


def _load_dotenv(env_path: Path) -> None:
    """Carga variables KEY=VALUE desde un archivo .env sin sobrescribir variables ya definidas."""
    if not env_path.is_file():
        return
    with open(env_path, "r", encoding="utf-8") as file:
        for raw_line in file:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def _expand_env_vars(obj: Any) -> Any:
    """Resuelve recursivamente placeholders ${VAR} y ${VAR:-default} en estructuras YAML."""
    if isinstance(obj, dict):
        return {k: _expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env_vars(item) for item in obj]
    if isinstance(obj, str):
        def _replace(match: re.Match[str]) -> str:
            var_name, default = match.group(1), match.group(2)
            if var_name in os.environ:
                return os.environ[var_name]
            if default is not None:
                return default
            raise ValueError(
                f"Variable de entorno requerida no definida: '{var_name}'. "
                "Defínela en tu entorno o en el archivo .env."
            )

        return _ENV_PATTERN.sub(_replace, obj)
    return obj


def load_yaml(path: str) -> dict[str, Any]:
    """Carga un archivo YAML y resuelve variables de entorno (incluyendo .env local)."""
    yaml_path = Path(path).resolve()
    _load_dotenv(yaml_path.parent / ".env")
    with open(yaml_path, "r", encoding="utf-8") as file:
        data = yaml.safe_load(file)
    return _expand_env_vars(data)


def build_oltp_engine(cfg: dict[str, Any]) -> Engine:
    """Construye el motor SQLAlchemy hacia la base operacional SQL Server."""
    odbc_str = (
        f"DRIVER={{{cfg['driver']}}};"
        f"SERVER={cfg['host']},{cfg['port']};"
        f"DATABASE={cfg['dbname']};"
        f"UID={cfg['user']};"
        f"PWD={cfg['password']};"
        f"TrustServerCertificate={cfg.get('trust_server_certificate', 'yes')};"
    )
    quoted = urllib.parse.quote_plus(odbc_str)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={quoted}", fast_executemany=True)


def build_dw_engine(cfg: dict[str, Any]) -> Engine:
    """Construye el motor SQLAlchemy hacia la bodega de datos en PostgreSQL."""
    pwd = urllib.parse.quote_plus(str(cfg.get("password") or ""))
    auth = f"{cfg['user']}:{pwd}" if pwd else cfg["user"]
    url = (
        f"{cfg['drivername']}://{auth}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )
    return create_engine(url)


def ensure_postgres_database(cfg: dict[str, Any]) -> None:
    """Crea la base de datos destino en PostgreSQL si aún no existe."""
    target_db = cfg["dbname"]
    conn = psycopg2.connect(
        dbname="postgres",
        user=cfg["user"],
        password=cfg.get("password") or "",
        host=cfg["host"],
        port=cfg["port"],
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (target_db,))
        if not cur.fetchone():
            cur.execute(f'CREATE DATABASE "{target_db}";')
            print(f"[INIT] Base de datos '{target_db}' creada en PostgreSQL.")
        else:
            print(f"[INIT] Base de datos '{target_db}' ya existe en PostgreSQL.")
    conn.close()


def initialize_schema(dw_engine: Engine, sqlscripts_path: str, full_refresh: bool = True) -> None:
    """Ejecuta los scripts DDL en orden para preparar las tablas del esquema dimensional."""
    scripts = load_yaml(sqlscripts_path)
    ordered_keys = [
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
        "create_indexes",
    ]
    with dw_engine.begin() as conn:
        if full_refresh and "drop_all" in scripts:
            for stmt in _split_statements(scripts["drop_all"]):
                conn.execute(text(stmt))
        for key in ordered_keys:
            for stmt in _split_statements(scripts[key]):
                conn.execute(text(stmt))
    print("[INIT] Esquema dimensional inicializado correctamente en PostgreSQL.")


def _split_statements(sql_block: str) -> list[str]:
    """Divide un bloque SQL en sentencias individuales no vacías."""
    return [s.strip() for s in sql_block.split(";") if s.strip()]


def parse_xml_dict(xml_str: str | None) -> dict[str, str]:
    """Extrae etiquetas hijas de un XML de encuestas ignorando su namespace."""
    if not xml_str:
        return {}
    try:
        root = ET.fromstring(xml_str)
    except ET.ParseError:
        return {}
    result: dict[str, str] = {}
    for child in root:
        tag = child.tag.split("}", 1)[-1] if "}" in child.tag else child.tag
        if child.text is not None:
            result[tag] = child.text.strip()
    return result


def fetch_surrogate_map(
    dw_engine: Engine,
    table_name: str,
    business_col: str,
    surrogate_col: str,
) -> dict[Any, int]:
    """Obtiene el diccionario {llave_negocio: llave_subrogada} desde PostgreSQL."""
    query = text(f"SELECT {business_col}, {surrogate_col} FROM {table_name};")
    with dw_engine.connect() as conn:
        rows = conn.execute(query).fetchall()
    return {(row[0].strip() if isinstance(row[0], str) else row[0]): row[1] for row in rows}
