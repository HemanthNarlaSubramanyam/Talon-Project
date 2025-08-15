/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
  Create business-ready Gold views (Star Schema) from Silver.
 
===============================================================================
*/
USE [case_q1_2025];
GO

IF SCHEMA_ID('gold') IS NULL EXEC(N'CREATE SCHEMA gold AUTHORIZATION dbo;');
GO

-- Drop old views (and same-named tables if any) to avoid conflicts
IF OBJECT_ID('gold.fact_orders','V') IS NOT NULL DROP VIEW gold.fact_orders;
IF OBJECT_ID('gold.dim_store','V')  IS NOT NULL DROP VIEW gold.dim_store;
IF OBJECT_ID('gold.dim_date','V')   IS NOT NULL DROP VIEW gold.dim_date;
IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL DROP VIEW gold.dim_customers;
IF OBJECT_ID('gold.fact_orders','U') IS NOT NULL DROP TABLE gold.fact_orders;
IF OBJECT_ID('gold.dim_store','U')  IS NOT NULL DROP TABLE gold.dim_store;
IF OBJECT_ID('gold.dim_date','U')   IS NOT NULL DROP TABLE gold.dim_date;
GO

/* =============================================================================
   gold.dim_date (Q1'2025; date_key = yyyymmdd)
   - Uses math instead of FORMAT() for performance.
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
   gold.dim_store
   - Distinct stores from Silver.
   - Adds synthetic 'ONLINE' member when store id is blank but channel = Online.
============================================================================= */
CREATE VIEW gold.dim_store
AS
WITH base AS (
  SELECT DISTINCT 
         COALESCE(NULLIF(LTRIM(RTRIM(store_integration_id)), ''),
                  CASE WHEN channel = 'Online' THEN N'ONLINE' END) AS store_integration_id
  FROM silver.customer_sessions
  WHERE created_at >= '2025-01-01' AND created_at < '2025-04-01'
)
SELECT
  store_integration_id,
  CAST(CASE WHEN store_integration_id = N'ONLINE' THEN 1 ELSE 0 END AS BIT) AS is_online
FROM base
WHERE store_integration_id IS NOT NULL;
GO

/* =============================================================================
   gold.fact_orders (grain = session/order in Q1'2025)
   Columns:
     date_key, store_integration_id, channel, state, items, total_usd,
     discount_amount_usd, net_revenue, discount_depth
============================================================================= */
CREATE VIEW gold.fact_orders
AS
SELECT
  (YEAR(s.created_at)*10000) + (MONTH(s.created_at)*100) + DAY(s.created_at)  AS date_key,
  COALESCE(NULLIF(LTRIM(RTRIM(s.store_integration_id)), ''),
           CASE WHEN s.channel = 'Online' THEN N'ONLINE' END)                 AS store_integration_id,
  s.channel,
  s.state,
  s.number_of_cart_items                                                       AS items,
  s.total_usd,
  ISNULL(s.discount_amount_usd, 0)                                             AS discount_amount_usd,
  CASE WHEN s.state = 'closed'
       THEN s.total_usd - ISNULL(s.discount_amount_usd, 0)
       END                                                                     AS net_revenue,
  CASE WHEN s.state = 'closed' AND s.total_usd > 0
       THEN ISNULL(s.discount_amount_usd, 0) / s.total_usd
       END                                                                     AS discount_depth
FROM silver.sessions_with_discounts s
WHERE s.created_at >= '2025-01-01'
  AND s.created_at <  '2025-04-01';
GO

/* =============================================================================
   (Optional) gold.dim_customers
   - Minimal example using Bronze to get customer_profile_fk.
============================================================================= */
CREATE VIEW gold.dim_customers
AS
SELECT DISTINCT
  s.customer_profile_fk AS customer_id
FROM bronze.customer_sessions_raw s
WHERE s.customer_profile_fk IS NOT NULL
  AND TRY_CONVERT(DATETIME2(3), REPLACE(s.created,' UTC','')) >= '2025-01-01'
  AND TRY_CONVERT(DATETIME2(3), REPLACE(s.created,' UTC','')) <  '2025-04-01';
GO

PRINT '✅ Gold views created: gold.dim_date, gold.dim_store, gold.fact_orders (optional gold.dim_customers).';

