"""Módulo de extracción (Extract) desde la base operacional AdventureWorks2025 (OLTP)."""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine


def extract_date_bounds(oltp_engine: Engine) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Obtiene el rango mínimo y máximo de fechas de órdenes para generar dim_date."""
    sql = """
        SELECT
            MIN(OrderDate) AS min_date,
            MAX(COALESCE(DueDate, ShipDate, OrderDate)) AS max_date
        FROM Sales.SalesOrderHeader;
    """
    df = pd.read_sql(sql, oltp_engine)
    return pd.to_datetime(df.loc[0, "min_date"]), pd.to_datetime(df.loc[0, "max_date"])


def extract_currency(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae el catálogo maestro de monedas desde Sales.Currency."""
    sql = """
        SELECT
            RTRIM(CurrencyCode) AS currency_alternate_key,
            Name AS currency_name
        FROM Sales.Currency
        ORDER BY CurrencyCode;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_sales_territory(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae territorios de venta enriquecidos con el nombre del país."""
    sql = """
        SELECT
            st.TerritoryID AS sales_territory_alternate_key,
            st.Name AS sales_territory_region,
            cr.Name AS sales_territory_country,
            st.[Group] AS sales_territory_group
        FROM Sales.SalesTerritory st
        JOIN Person.CountryRegion cr
            ON st.CountryRegionCode = cr.CountryRegionCode
        ORDER BY st.TerritoryID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_geography(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae ubicaciones geográficas únicas desde Person.Address y StateProvince."""
    sql = """
        SELECT DISTINCT
            a.City AS city,
            RTRIM(sp.StateProvinceCode) AS state_province_code,
            sp.Name AS state_province_name,
            RTRIM(cr.CountryRegionCode) AS country_region_code,
            cr.Name AS english_country_region_name,
            a.PostalCode AS postal_code,
            sp.TerritoryID AS territory_id_oltp
        FROM Person.Address a
        JOIN Person.StateProvince sp
            ON a.StateProvinceID = sp.StateProvinceID
        JOIN Person.CountryRegion cr
            ON sp.CountryRegionCode = cr.CountryRegionCode;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_product_category(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae categorías de productos desde Production.ProductCategory."""
    sql = """
        SELECT
            ProductCategoryID AS product_category_alternate_key,
            Name AS english_product_category_name
        FROM Production.ProductCategory
        ORDER BY ProductCategoryID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_product_subcategory(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae subcategorías de productos desde Production.ProductSubcategory."""
    sql = """
        SELECT
            ProductSubcategoryID AS product_subcategory_alternate_key,
            Name AS english_product_subcategory_name,
            ProductCategoryID AS product_category_alternate_key
        FROM Production.ProductSubcategory
        ORDER BY ProductSubcategoryID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_product(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae el catálogo de productos junto con su modelo y descripción en inglés."""
    sql = """
        SELECT
            p.ProductID AS product_id_oltp,
            p.ProductNumber AS product_alternate_key,
            p.ProductSubcategoryID AS product_subcategory_alternate_key,
            RTRIM(p.WeightUnitMeasureCode) AS weight_unit_measure_code,
            RTRIM(p.SizeUnitMeasureCode) AS size_unit_measure_code,
            p.Name AS english_product_name,
            p.StandardCost AS standard_cost,
            p.FinishedGoodsFlag AS finished_goods_flag,
            p.Color AS color,
            p.SafetyStockLevel AS safety_stock_level,
            p.ReorderPoint AS reorder_point,
            p.ListPrice AS list_price,
            p.Size AS size,
            p.Weight AS weight,
            p.DaysToManufacture AS days_to_manufacture,
            RTRIM(p.ProductLine) AS product_line,
            RTRIM(p.Class) AS class,
            RTRIM(p.Style) AS style,
            pm.Name AS model_name,
            pd.Description AS english_description,
            p.SellStartDate AS start_date,
            p.SellEndDate AS end_date
        FROM Production.Product p
        LEFT JOIN Production.ProductModel pm
            ON p.ProductModelID = pm.ProductModelID
        LEFT JOIN Production.ProductModelProductDescriptionCulture pmpdc
            ON pm.ProductModelID = pmpdc.ProductModelID
            AND pmpdc.CultureID = 'en'
        LEFT JOIN Production.ProductDescription pd
            ON pmpdc.ProductDescriptionID = pd.ProductDescriptionID
        ORDER BY p.ProductID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_promotion(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae las promociones y ofertas especiales desde Sales.SpecialOffer."""
    sql = """
        SELECT
            SpecialOfferID AS promotion_alternate_key,
            Description AS english_promotion_name,
            DiscountPct AS discount_pct,
            Type AS english_promotion_type,
            Category AS english_promotion_category,
            StartDate AS start_date,
            EndDate AS end_date,
            MinQty AS min_qty,
            MaxQty AS max_qty
        FROM Sales.SpecialOffer
        ORDER BY SpecialOfferID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_customer(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae clientes individuales (B2C) con su dirección principal y XML demográfico."""
    sql = """
        WITH RankedAddresses AS (
            SELECT
                bea.BusinessEntityID,
                a.AddressLine1,
                a.AddressLine2,
                a.City,
                RTRIM(sp.StateProvinceCode) AS state_province_code,
                a.PostalCode,
                ROW_NUMBER() OVER (
                    PARTITION BY bea.BusinessEntityID
                    ORDER BY CASE WHEN bea.AddressTypeID = 2 THEN 0 ELSE 1 END, a.AddressID
                ) AS rn
            FROM Person.BusinessEntityAddress bea
            JOIN Person.Address a ON bea.AddressID = a.AddressID
            JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        ),
        RankedPhones AS (
            SELECT
                BusinessEntityID,
                PhoneNumber,
                ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY PhoneNumberTypeID) AS rn
            FROM Person.PersonPhone
        ),
        RankedEmails AS (
            SELECT
                BusinessEntityID,
                EmailAddress,
                ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY EmailAddressID) AS rn
            FROM Person.EmailAddress
        )
        SELECT
            c.CustomerID AS customer_id_oltp,
            c.AccountNumber AS customer_alternate_key,
            p.Title AS title,
            p.FirstName AS first_name,
            p.MiddleName AS middle_name,
            p.LastName AS last_name,
            p.NameStyle AS name_style,
            p.Suffix AS suffix,
            CAST(p.Demographics AS NVARCHAR(MAX)) AS demographics_xml,
            em.EmailAddress AS email_address,
            ph.PhoneNumber AS phone,
            addr.AddressLine1 AS address_line1,
            addr.AddressLine2 AS address_line2,
            addr.City AS city,
            addr.state_province_code,
            addr.PostalCode AS postal_code
        FROM Sales.Customer c
        JOIN Person.Person p
            ON c.PersonID = p.BusinessEntityID
        LEFT JOIN RankedAddresses addr
            ON p.BusinessEntityID = addr.BusinessEntityID AND addr.rn = 1
        LEFT JOIN RankedPhones ph
            ON p.BusinessEntityID = ph.BusinessEntityID AND ph.rn = 1
        LEFT JOIN RankedEmails em
            ON p.BusinessEntityID = em.BusinessEntityID AND em.rn = 1
        WHERE c.PersonID IS NOT NULL
          AND c.StoreID IS NULL
        ORDER BY c.CustomerID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_reseller(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae tiendas revendedoras (B2B) con su dirección principal, historial y XML."""
    sql = """
        WITH RankedAddresses AS (
            SELECT
                bea.BusinessEntityID,
                a.AddressLine1,
                a.AddressLine2,
                a.City,
                RTRIM(sp.StateProvinceCode) AS state_province_code,
                a.PostalCode,
                ROW_NUMBER() OVER (
                    PARTITION BY bea.BusinessEntityID
                    ORDER BY CASE WHEN bea.AddressTypeID = 3 THEN 0 ELSE 1 END, a.AddressID
                ) AS rn
            FROM Person.BusinessEntityAddress bea
            JOIN Person.Address a ON bea.AddressID = a.AddressID
            JOIN Person.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
        ),
        StoreOrderYears AS (
            SELECT
                c.StoreID,
                MIN(YEAR(h.OrderDate)) AS first_order_year,
                MAX(YEAR(h.OrderDate)) AS last_order_year
            FROM Sales.SalesOrderHeader h
            JOIN Sales.Customer c ON h.CustomerID = c.CustomerID
            WHERE h.OnlineOrderFlag = 0 AND c.StoreID IS NOT NULL
            GROUP BY c.StoreID
        )
        SELECT
            s.BusinessEntityID AS store_id_oltp,
            c.AccountNumber AS reseller_alternate_key,
            s.Name AS reseller_name,
            CAST(s.Demographics AS NVARCHAR(MAX)) AS demographics_xml,
            addr.AddressLine1 AS address_line1,
            addr.AddressLine2 AS address_line2,
            addr.City AS city,
            addr.state_province_code,
            addr.PostalCode AS postal_code,
            soy.first_order_year,
            soy.last_order_year
        FROM Sales.Store s
        JOIN Sales.Customer c
            ON s.BusinessEntityID = c.StoreID AND c.PersonID IS NULL
        LEFT JOIN RankedAddresses addr
            ON s.BusinessEntityID = addr.BusinessEntityID AND addr.rn = 1
        LEFT JOIN StoreOrderYears soy
            ON s.BusinessEntityID = soy.StoreID
        ORDER BY s.BusinessEntityID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_employee(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae empleados, vendedores, departamento actual y jerarquía organizacional."""
    sql = """
        WITH LatestPay AS (
            SELECT
                BusinessEntityID,
                Rate,
                PayFrequency,
                ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY RateChangeDate DESC) AS rn
            FROM HumanResources.EmployeePayHistory
        ),
        RankedPhones AS (
            SELECT
                BusinessEntityID,
                PhoneNumber,
                ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY PhoneNumberTypeID) AS rn
            FROM Person.PersonPhone
        ),
        RankedEmails AS (
            SELECT
                BusinessEntityID,
                EmailAddress,
                ROW_NUMBER() OVER (PARTITION BY BusinessEntityID ORDER BY EmailAddressID) AS rn
            FROM Person.EmailAddress
        )
        SELECT
            e.BusinessEntityID AS employee_id_oltp,
            e.NationalIDNumber AS employee_national_id_alternate_key,
            mgr.NationalIDNumber AS parent_employee_national_id_alternate_key,
            sp.TerritoryID AS territory_id_oltp,
            p.FirstName AS first_name,
            p.LastName AS last_name,
            p.MiddleName AS middle_name,
            p.NameStyle AS name_style,
            e.JobTitle AS title,
            e.HireDate AS hire_date,
            e.BirthDate AS birth_date,
            e.LoginID AS login_id,
            em.EmailAddress AS email_address,
            ph.PhoneNumber AS phone,
            RTRIM(e.MaritalStatus) AS marital_status,
            e.SalariedFlag AS salaried_flag,
            RTRIM(e.Gender) AS gender,
            lp.PayFrequency AS pay_frequency,
            lp.Rate AS base_rate,
            e.VacationHours AS vacation_hours,
            e.SickLeaveHours AS sick_leave_hours,
            e.CurrentFlag AS current_flag,
            CASE WHEN sp.BusinessEntityID IS NOT NULL THEN 1 ELSE 0 END AS sales_person_flag,
            dept.Name AS department_name,
            edh.StartDate AS start_date,
            edh.EndDate AS end_date
        FROM HumanResources.Employee e
        JOIN Person.Person p
            ON e.BusinessEntityID = p.BusinessEntityID
        LEFT JOIN HumanResources.Employee mgr
            ON e.OrganizationNode.GetAncestor(1) = mgr.OrganizationNode
        LEFT JOIN HumanResources.EmployeeDepartmentHistory edh
            ON e.BusinessEntityID = edh.BusinessEntityID AND edh.EndDate IS NULL
        LEFT JOIN HumanResources.Department dept
            ON edh.DepartmentID = dept.DepartmentID
        LEFT JOIN Sales.SalesPerson sp
            ON e.BusinessEntityID = sp.BusinessEntityID
        LEFT JOIN LatestPay lp
            ON e.BusinessEntityID = lp.BusinessEntityID AND lp.rn = 1
        LEFT JOIN RankedPhones ph
            ON e.BusinessEntityID = ph.BusinessEntityID AND ph.rn = 1
        LEFT JOIN RankedEmails em
            ON e.BusinessEntityID = em.BusinessEntityID AND em.rn = 1
        ORDER BY e.BusinessEntityID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_sales_reason(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae motivos de venta desde Sales.SalesReason."""
    sql = """
        SELECT
            SalesReasonID AS sales_reason_alternate_key,
            Name AS sales_reason_name,
            ReasonType AS sales_reason_reason_type
        FROM Sales.SalesReason
        ORDER BY SalesReasonID;
    """
    return pd.read_sql(sql, oltp_engine)


def extract_sales_lines(oltp_engine: Engine, online_flag: int) -> pd.DataFrame:
    """Extrae líneas de órdenes de venta para Internet (1) o Revendedores (0)."""
    sql = text("""
        SELECT
            h.SalesOrderNumber AS sales_order_number,
            ROW_NUMBER() OVER (
                PARTITION BY h.SalesOrderID
                ORDER BY d.SalesOrderDetailID
            ) AS sales_order_line_number,
            h.RevisionNumber AS revision_number,
            h.OrderDate AS order_date,
            h.DueDate AS due_date,
            h.ShipDate AS ship_date,
            h.CustomerID AS customer_id_oltp,
            c.StoreID AS store_id_oltp,
            h.SalesPersonID AS employee_id_oltp,
            h.TerritoryID AS territory_id_oltp,
            COALESCE(RTRIM(cr.ToCurrencyCode), 'USD') AS currency_alternate_key,
            d.ProductID AS product_id_oltp,
            d.SpecialOfferID AS promotion_alternate_key,
            d.OrderQty AS order_quantity,
            d.UnitPrice AS unit_price,
            d.UnitPriceDiscount AS unit_price_discount_pct,
            d.LineTotal AS line_total,
            p.StandardCost AS product_standard_cost,
            h.SubTotal AS header_subtotal,
            h.TaxAmt AS header_tax_amt,
            h.Freight AS header_freight,
            d.CarrierTrackingNumber AS carrier_tracking_number,
            h.PurchaseOrderNumber AS customer_po_number
        FROM Sales.SalesOrderDetail d
        JOIN Sales.SalesOrderHeader h
            ON d.SalesOrderID = h.SalesOrderID
        JOIN Sales.Customer c
            ON h.CustomerID = c.CustomerID
        JOIN Production.Product p
            ON d.ProductID = p.ProductID
        LEFT JOIN Sales.CurrencyRate cr
            ON h.CurrencyRateID = cr.CurrencyRateID
        WHERE h.OnlineOrderFlag = :online_flag
        ORDER BY h.SalesOrderID, d.SalesOrderDetailID;
    """)
    return pd.read_sql(sql, oltp_engine, params={"online_flag": online_flag})


def extract_internet_sales_reasons(oltp_engine: Engine) -> pd.DataFrame:
    """Extrae la relación entre líneas de órdenes por internet y sus motivos de compra."""
    sql = """
        WITH NumberedLines AS (
            SELECT
                h.SalesOrderID,
                h.SalesOrderNumber AS sales_order_number,
                ROW_NUMBER() OVER (
                    PARTITION BY h.SalesOrderID
                    ORDER BY d.SalesOrderDetailID
                ) AS sales_order_line_number
            FROM Sales.SalesOrderDetail d
            JOIN Sales.SalesOrderHeader h
                ON d.SalesOrderID = h.SalesOrderID
            WHERE h.OnlineOrderFlag = 1
        )
        SELECT
            nl.sales_order_number,
            nl.sales_order_line_number,
            hsr.SalesReasonID AS sales_reason_alternate_key
        FROM NumberedLines nl
        JOIN Sales.SalesOrderHeaderSalesReason hsr
            ON nl.SalesOrderID = hsr.SalesOrderID
        ORDER BY nl.sales_order_number, nl.sales_order_line_number, hsr.SalesReasonID;
    """
    return pd.read_sql(sql, oltp_engine)

