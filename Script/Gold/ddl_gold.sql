/*
===============================================================================
DDL Script: Create Gold Views (Clean Version)
===============================================================================
Purpose:
  Build the business-ready Gold layer (star schema) as VIEWS on top of Silver.
  - gold.dim_date        : Q1 2025 calendar (date_key = yyyymmdd)
  - gold.dim_store       : canonical stores incl. synthetic 'ONLINE'
  - gold.fact_orders     : one row per session/order (Q1 2025 window)
Design notes:
  - Idempotent (drops views first), never drops tables.
  - Explicit numeric casts for stable types in BI.
  - Case-safe ONLINE derivation via LOWER(channel) = 'online'.
===============================================================================
*/

USE [case_q1_2025];
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* Ensure schema */
IF SCHEMA_ID(N'gold') IS NULL
  EXEC(N'CREATE SCHEMA gold AUTHORIZATION dbo;');
GO

/* Drop old views */
IF OBJECT_ID(N'gold.fact_orders','V') IS NOT NULL DROP VIEW gold.fact_orders;
IF OBJECT_ID(N'gold.dim_store','V')  IS NOT NULL DROP VIEW gold.dim_store;
IF OBJECT_ID(N'gold.dim_date','V')   IS NOT NULL DROP VIEW gold.dim_date;
GO

/* =============================================================================
   gold.dim_date — Q1'2025 calendar (date_key = yyyymmdd)
============================================================================= */
CREATE VIEW gold.dim_date
AS
WITH d AS (
  SELECT CAST('2025-01-01' AS DATE) AS dt
  UNION ALL
  SELECT DATEADD(DAY, 1, dt) FROM d WHERE dt < '2025-03-31'
)
SELECT
  (YEAR(dt)*10000) + (MONTH(dt)*100) + DAY(dt) AS date_key,
  dt                                           AS full_date,
  YEAR(dt)                                     AS [year],
  DATEPART(QUARTER, dt)                        AS [quarter],
  MONTH(dt)                                    AS [month],
  DATENAME(MONTH, dt)                          AS month_name,
  DAY(dt)                                      AS day_of_month
FROM d;
GO

/* =============================================================================
   gold.dim_store — distinct stores within Q1'2025
   - Adds synthetic 'ONLINE' when store id is blank and channel is online.
============================================================================= */
CREATE VIEW gold.dim_store
AS
WITH base AS (
  SELECT DISTINCT
    COALESCE(
      NULLIF(LTRIM(RTRIM(store_integration_id)), ''),
      CASE WHEN LOWER(LTRIM(RTRIM(channel))) = 'online' THEN N'ONLINE' END
    ) AS store_integration_id
  FROM silver.customer_sessions
  WHERE created_at >= '2025-01-01' AND created_at < '2025-04-01'
)
SELECT
  store_integration_id,
  CAST(CASE WHEN store_integration_id = N'ONLINE' THEN 1 ELSE 0 END AS bit) AS is_online
FROM base
WHERE store_integration_id IS NOT NULL;
GO

/* =============================================================================
   gold.fact_orders — session/order grain (Q1'2025)
   Columns:
     date_key, store_integration_id, channel, state, items,
     total_usd, discount_amount_usd, net_revenue, discount_depth
   - Explicit DECIMAL casts for currency/ratio stability.
============================================================================= */
CREATE VIEW gold.fact_orders
AS
SELECT
  (YEAR(s.created_at)*10000) + (MONTH(s.created_at)*100) + DAY(s.created_at)   AS date_key,
  COALESCE(
    NULLIF(LTRIM(RTRIM(s.store_integration_id)), ''),
    CASE WHEN LOWER(LTRIM(RTRIM(s.channel))) = 'online' THEN N'ONLINE' END
  )                                                                             AS store_integration_id,
  s.channel,
  s.state,
  s.number_of_cart_items                                                        AS items,
  CAST(s.total_usd AS DECIMAL(18,2))                                            AS total_usd,
  CAST(ISNULL(s.discount_amount_usd, 0) AS DECIMAL(18,2))                       AS discount_amount_usd,
  CAST(
    CASE WHEN s.state = N'closed'
         THEN s.total_usd - ISNULL(s.discount_amount_usd, 0)
    END AS DECIMAL(18,2)
  )                                                                             AS net_revenue,
  CAST(
    CASE WHEN s.state = N'closed' AND s.total_usd > 0
         THEN ISNULL(s.discount_amount_usd, 0) / s.total_usd
    END AS DECIMAL(18,6)
  )                                                                             AS discount_depth
FROM silver.sessions_with_discounts AS s
WHERE s.created_at >= '2025-01-01'
  AND s.created_at <  '2025-04-01';
GO

PRINT N'✅ Gold views created: gold.dim_date, gold.dim_store, gold.fact_orders.';
