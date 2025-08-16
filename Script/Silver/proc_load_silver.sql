/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
  ETL from BRONZE into SILVER (typed, standardized, enriched).
  - Truncates Silver targets.
  - De-dupes sessions by session_id (keeps latest _load_ts).
  - Parses discount effects and rolls them up per session.
  - Joins sessions + discounts; computed metrics live on the table.

Usage:
  USE [case_q1_2025];
  EXEC silver.load_silver;
===============================================================================
*/

USE [case_q1_2025];
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  DECLARE @t0 DATETIME2(3) = SYSDATETIME(), @t DATETIME2(3);

  BEGIN TRY
    BEGIN TRAN;

    /* ==============================================================
       0) Truncate targets (idempotent full reload of Silver)
       ============================================================== */
    TRUNCATE TABLE silver.sessions_with_discounts;
    TRUNCATE TABLE silver.effects_summary;
    TRUNCATE TABLE silver.customer_sessions;

    /* ==============================================================
       1) Customer Sessions — cast, normalize, de-dupe
       ============================================================== */
    SET @t = SYSDATETIME();

    ;WITH ranked AS (
      SELECT
        s.session_id,
        TRY_CONVERT(DATETIME2(3), REPLACE(s.created,' UTC',''))       AS created_at,
        LOWER(LTRIM(RTRIM(s.state)))                                  AS state,
        TRY_CONVERT(DECIMAL(18,2), s.total_usd)                       AS total_usd,
        TRY_CONVERT(INT, s.number_of_cart_items)                      AS number_of_cart_items,
        NULLIF(LTRIM(RTRIM(s.store_integration_id)),'')               AS store_integration_id,
        CASE
          WHEN LOWER(ISNULL(s.channel,'')) LIKE '%store%' THEN 'Offline'
          WHEN LOWER(ISNULL(s.channel,'')) LIKE '%ecom%'  THEN 'Online'
          WHEN NULLIF(LTRIM(RTRIM(s.store_integration_id)),'') IS NULL THEN 'Online'
          ELSE 'Offline'
        END                                                           AS channel,
        s._load_ts,
        ROW_NUMBER() OVER (PARTITION BY s.session_id ORDER BY s._load_ts DESC) AS rn
      FROM bronze.customer_sessions_raw s
    )
    INSERT INTO silver.customer_sessions
      (session_id, created_at, state, total_usd, number_of_cart_items,
       store_integration_id, channel, _src_load_ts)
    SELECT
      session_id, created_at, state, total_usd, number_of_cart_items,
      store_integration_id, channel, _load_ts
    FROM ranked
    WHERE rn = 1;

    PRINT '>> silver.customer_sessions loaded in '
      + CONVERT(varchar(12), DATEDIFF(SECOND, @t, SYSDATETIME())) + 's';

    /* ==============================================================
       2) Effects — parse & roll up discount amount per session
       ============================================================== */
    SET @t = SYSDATETIME();

    ;WITH latest AS (
      SELECT
        e.session_fk                                            AS session_id,
        LTRIM(RTRIM(e.effect_type))                             AS effect_type,
        TRY_CONVERT(DECIMAL(18,2), NULLIF(e.value,''))          AS value_num,
        e._load_ts,
        ROW_NUMBER() OVER (PARTITION BY e.session_fk, e.effect_type ORDER BY e._load_ts DESC) AS rn
      FROM bronze.effects_raw e
    ),
    rolled AS (
      SELECT
        session_id,
        SUM(CASE
              WHEN effect_type IN ('setDiscountPerItem','setDiscountPerAdditionalCost')
              THEN ISNULL(value_num,0) ELSE 0
            END)                                                AS discount_amount_usd,
        MAX(effect_type)                                        AS representative_effect_type,
        MAX(_load_ts)                                           AS _src_load_ts
      FROM latest
      WHERE rn = 1
      GROUP BY session_id
    )
    INSERT INTO silver.effects_summary
      (session_id, discount_amount_usd, representative_effect_type, _src_load_ts)
    SELECT session_id, discount_amount_usd, representative_effect_type, _src_load_ts
    FROM rolled;

    PRINT '>> silver.effects_summary loaded in '
      + CONVERT(varchar(12), DATEDIFF(SECOND, @t, SYSDATETIME())) + 's';

    /* ==============================================================
       3) Sessions WITH Discounts — join + pass-through metrics
       ============================================================== */
    SET @t = SYSDATETIME();

    INSERT INTO silver.sessions_with_discounts
      (session_id, created_at, state, number_of_cart_items, store_integration_id,
       channel, total_usd, discount_amount_usd, _src_load_ts)
    SELECT
      s.session_id, s.created_at, s.state, s.number_of_cart_items, s.store_integration_id,
      s.channel, s.total_usd,
      ISNULL(d.discount_amount_usd, 0)                            AS discount_amount_usd,
      COALESCE(d._src_load_ts, s._src_load_ts)                    AS _src_load_ts
    FROM silver.customer_sessions s
    LEFT JOIN silver.effects_summary d
      ON d.session_id = s.session_id;

    PRINT '>> silver.sessions_with_discounts loaded in '
      + CONVERT(varchar(12), DATEDIFF(SECOND, @t, SYSDATETIME())) + 's';

    COMMIT;

    /* ==============================================================
       Summary
       ============================================================== */
    PRINT '==========================================';
    PRINT 'Silver load complete in '
      + CONVERT(varchar(12), DATEDIFF(SECOND, @t0, SYSDATETIME())) + 's';
    PRINT '==========================================';

    SELECT 'silver.customer_sessions'        AS table_name, COUNT(*) AS rows FROM silver.customer_sessions
    UNION ALL
    SELECT 'silver.effects_summary',                COUNT(*)         FROM silver.effects_summary
    UNION ALL
    SELECT 'silver.sessions_with_discounts',       COUNT(*)         FROM silver.sessions_with_discounts;
  END TRY
  BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK;
    THROW;
  END CATCH
END
GO

-- Execute once to load
EXEC silver.load_silver;
