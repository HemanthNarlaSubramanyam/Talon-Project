/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
    - Truncates Silver tables (customer_sessions, effects_summary, sessions_with_discounts).
    - Inserts transformed and cleansed data from Bronze into Silver tables:
        * Casts/normalizes session fields and de-duplicates by session_id
          (keeps latest _load_ts).
        * Parses discount effects and aggregates to session level.
        * Joins sessions + discounts and derives reporting metrics (computed columns).

Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

USE [case_q1_2025];
GO
CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer (Bronze -> Silver)';
        PRINT '================================================';

        BEGIN TRAN;

        /* ------------------------------------------------
           0) Truncate Silver tables
        ------------------------------------------------ */
        PRINT '>> Truncating Silver targets...';
        TRUNCATE TABLE silver.sessions_with_discounts;
        TRUNCATE TABLE silver.effects_summary;
        TRUNCATE TABLE silver.customer_sessions;

        /* ------------------------------------------------
           1) silver.customer_sessions — typed, normalized, deduped
        ------------------------------------------------ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.customer_sessions';

        ;WITH ranked AS (
            SELECT
                s.session_id,
                TRY_CONVERT(DATETIME2(3), REPLACE(s.created,' UTC',''))          AS created_at,
                LOWER(LTRIM(RTRIM(s.state)))                                     AS state,
                TRY_CONVERT(DECIMAL(18,2), s.total_usd)                          AS total_usd,
                TRY_CONVERT(INT, s.number_of_cart_items)                         AS number_of_cart_items,
                NULLIF(LTRIM(RTRIM(s.store_integration_id)),'')                  AS store_integration_id,
                CASE
                    WHEN LOWER(ISNULL(s.channel,'')) LIKE '%store%' THEN 'Offline'
                    WHEN LOWER(ISNULL(s.channel,'')) LIKE '%ecom%'  THEN 'Online'
                    WHEN NULLIF(LTRIM(RTRIM(s.store_integration_id)),'') IS NULL THEN 'Online'
                    ELSE 'Offline'
                END                                                              AS channel,
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

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (silver.customer_sessions): ' 
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(32)) + ' seconds';
        PRINT '>> -------------';

        /* ------------------------------------------------
           2) silver.effects_summary — per-session discount $
        ------------------------------------------------ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.effects_summary';

        ;WITH latest AS (
            SELECT
                e.session_fk                                             AS session_id,
                e.effect_type,
                TRY_CONVERT(DECIMAL(18,2), e.value)                      AS value_num,
                e._load_ts,
                ROW_NUMBER() OVER (PARTITION BY e.session_fk, e.effect_type ORDER BY e._load_ts DESC) AS rn
            FROM bronze.effects_raw e
        ),
        rolled AS (
            SELECT
                session_id,
                SUM(CASE WHEN effect_type IN ('setDiscountPerItem','setDiscountPerAdditionalCost')
                         THEN ISNULL(value_num,0) ELSE 0 END)           AS discount_amount_usd,
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

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (silver.effects_summary): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(32)) + ' seconds';
        PRINT '>> -------------';

        /* ------------------------------------------------
           3) silver.sessions_with_discounts — join + computed metrics
        ------------------------------------------------ */
        SET @start_time = GETDATE();
        PRINT '>> Loading: silver.sessions_with_discounts';

        INSERT INTO silver.sessions_with_discounts
            (session_id, created_at, state, number_of_cart_items, store_integration_id,
             channel, total_usd, discount_amount_usd, _src_load_ts)
        SELECT
            s.session_id, s.created_at, s.state, s.number_of_cart_items, s.store_integration_id,
            s.channel, s.total_usd,
            ISNULL(d.discount_amount_usd, 0)                               AS discount_amount_usd,
            COALESCE(d._src_load_ts, s._src_load_ts)                        AS _src_load_ts
        FROM silver.customer_sessions s
        LEFT JOIN silver.effects_summary d
            ON d.session_id = s.session_id;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (silver.sessions_with_discounts): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR(32)) + ' seconds';
        PRINT '>> -------------';

        COMMIT;

        /* ------------------------------------------------
           4) Batch summary
        ------------------------------------------------ */
        SET @batch_end_time = GETDATE();
        PRINT '==========================================';
        PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR(32)) + ' seconds';
        PRINT '==========================================';

        -- Quick rowcounts
        SELECT 'silver.customer_sessions'        AS table_name, COUNT(*) AS rows FROM silver.customer_sessions
        UNION ALL
        SELECT 'silver.effects_summary',                COUNT(*)         FROM silver.effects_summary
        UNION ALL
        SELECT 'silver.sessions_with_discounts',       COUNT(*)         FROM silver.sessions_with_discounts;

    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK;
        PRINT '==========================================';
        PRINT 'ERROR OCCURRED DURING LOADING SILVER LAYER';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR(32));
        PRINT 'Error State  : ' + CAST(ERROR_STATE()  AS NVARCHAR(32));
        PRINT '==========================================';
        THROW; -- bubble up if orchestrated
    END CATCH
END
GO

-- Execute
EXEC silver.load_silver;
