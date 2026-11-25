# Entrega 4 — Herramienta ETL (AdventureWorks2025 → PostgreSQL)

**Universidad del Valle**  
**Escuela de Ingeniería de Sistemas y Computación**  
**Asignatura:** Introducción a Ciencia de Datos — 2026  

## Integrantes

* **Juan José Cortés Rodríguez** — `2325109`
* **Valentina Sanchez Castrillon** — `2324754`
* **Juan Francesco Garcia Vargas** — `2310174`
* **Jonathan Aristizabal Vargas** — `2322626`

---

## Descripción

Herramienta ETL modular en Python que extrae los datos de la base de datos operacional **`AdventureWorks2025`** (SQL Server), aplica las transformaciones dimensionales y carga en **PostgreSQL** (`adventureworks_dw`) los datamarts de:

* **Ventas por Internet (`fact_internet_sales` + `fact_internet_sales_reason`)**
* **Ventas por Revendedores (`fact_reseller_sales`)**

junto con todas sus dimensiones conformadas y específicas de canal (`dim_date`, `dim_product`, `dim_product_subcategory`, `dim_product_category`, `dim_promotion`, `dim_currency`, `dim_sales_territory`, `dim_geography`, `dim_customer`, `dim_reseller`, `dim_employee` y `dim_sales_reason`).

---

## Estructura del Proyecto

```text
3rd Deliverable/
├── config.yml              # Credenciales de conexión (SQL Server OLTP y PostgreSQL DW)
├── sqlscripts.yml          # Scripts DDL (creación de tablas, llaves foráneas e índices)
├── requirements.txt        # Dependencias de Python
├── main.py                 # Orquestador principal del pipeline ETL
├── README.md               # Guía de instalación y ejecución
└── etl/
    ├── __init__.py
    ├── utils_etl.py        # Conexiones SQLAlchemy, parser XML, DDL y mapeo de llaves
    ├── extract.py          # Extracción SQL desde AdventureWorks2025 (OLTP)
    ├── transform.py        # Transformaciones, dim_date, parsing XML y cálculo de medidas
    └── load.py             # Carga en bloques a PostgreSQL y validación de conteos
```

---

## Requisitos Previos

* **Python 3.10+** (probado en Python 3.12).
* **SQL Server** activo en el puerto `1433` con la base operacional `AdventureWorks2025` restaurada.
* **PostgreSQL** activo en el puerto `5432`.
* **ODBC Driver 18 for SQL Server** instalado en el sistema operativo.

---

## Instalación y Ejecución

### 1. Crear y activar el entorno virtual

```bash
cd "3rd Deliverable"
python3 -m venv .venv
source .venv/bin/activate
```

*(En Windows PowerShell: `.venv\Scripts\Activate.ps1`)*

### 2. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 3. Configurar variables de entorno (`.env`)

Las credenciales no se guardan en `config.yml`. Copia la plantilla `.env.example` como `.env` y coloca tus contraseñas locales:

```bash
cp .env.example .env
```

Edita `.env`:

```dotenv
OLTP_HOST=127.0.0.1
OLTP_PORT=1433
OLTP_DBNAME=AdventureWorks2025
OLTP_USER=sa
OLTP_PASSWORD=tu_password_sqlserver

DW_HOST=localhost
DW_PORT=5432
DW_DBNAME=adventureworks_dw
DW_USER=postgres
DW_PASSWORD=tu_password_postgres
```

*(También puedes exportar estas variables directamente en tu terminal; las variables del sistema tienen prioridad sobre `.env`.)*

### 4. Ejecutar el ETL

```bash
python main.py
```

Al ejecutarse, el script:
1. Crea automáticamente la base de datos `adventureworks_dw` en PostgreSQL (si no existe).
2. Inicializa el esquema dimensional definido en `sqlscripts.yml`.
3. Ejecuta las fases **Extract → Transform → Load** en orden de dependencias (dimensiones independientes → dimensiones jerárquicas → tablas de hechos y puente).
4. Imprime un resumen con el conteo final de registros cargados en cada una de las 15 tablas.
