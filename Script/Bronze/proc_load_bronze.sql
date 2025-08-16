/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
  Truncate Bronze tables and load from CSV files into the 'bronze' schema.
  - Uses CSV-aware BULK INSERT (handles quotes/commas).
  - Stages first, then lands into RAW with metadata (_file_name, _load_ts).
  - Linux/Docker friendly (no CODEPAGE; UTF-8 is default). 

Usage:
  USE [case_q1_2025];
  EXEC bronze.load_bronze;
===============================================================================
*/

USE [case_q1_2025];
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* Ensure staging tables exist (one-time) */
IF OBJECT_ID('bronze.customer_sessions_stage','U') IS NULL
BEGIN
  CREATE TABLE bronze.customer_sessions_stage (
    session_id NVARCHAR(64),
    created NVARCHAR(64),
    closedAt NVARCHAR(64),
    cancelledAt NVARCHAR(64),
    state NVARCHAR(40),
    total_usd NVARCHAR(50),
    customer_profile_fk NVARCHAR(64),
    store_integration_id NVARCHAR(64),
    number_of_cart_items NVARCHAR(50),
    channel NVARCHAR(64)
  );
END;

IF OBJECT_ID('bronze.effects_stage','U') IS NULL
BEGIN
  CREATE TABLE bronze.effects_stage (
    effect_id NVARCHAR(64),
    session_fk NVARCHAR(64),
    customer_profile_fk NVARCHAR(64),
    created_ts NVARCHAR(64),
    effect_type NVARCHAR(128),
    name NVARCHAR(MAX),
    value NVARCHAR(MAX)
  );
END;
GO

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
  SET NOCOUNT ON;
  SET XACT_ABORT ON;

  BEGIN TRY
    /* ---------------------------- A) customer_sessions.csv ---------------------------- */
    TRUNCATE TABLE bronze.customer_sessions_stage;

    BULK INSERT bronze.customer_sessions_stage
    FROM '/var/opt/mssql/customer_sessions.csv'
    WITH (
      FORMAT = 'CSV',
      FIELDQUOTE = '"',
      FIRSTROW = 2,
      ROWTERMINATOR = '0x0a',   -- if Windows CRLF file, change to '0x0d0a'
      TABLOCK,
      KEEPNULLS,
      MAXERRORS = 0
    );

    TRUNCATE TABLE bronze.customer_sessions_raw;

    INSERT INTO bronze.customer_sessions_raw
    (session_id, created, closedAt, cancelledAt, state, total_usd,
     customer_profile_fk, store_integration_id, number_of_cart_items, channel,
     _file_name, _load_ts)
    SELECT
      session_id, created, closedAt, cancelledAt, state, total_usd,
      customer_profile_fk, store_integration_id, number_of_cart_items, channel,
      N'customer_sessions.csv', SYSUTCDATETIME()
    FROM bronze.customer_sessions_stage;

    /* ------------------------------- B) effects.csv ---------------------------------- */
    TRUNCATE TABLE bronze.effects_stage;

    BULK INSERT bronze.effects_stage
    FROM '/var/opt/mssql/effects.csv'
    WITH (
      FORMAT = 'CSV',
      FIELDQUOTE = '"',
      FIRSTROW = 2,
      ROWTERMINATOR = '0x0a',   -- if Windows CRLF file, change to '0x0d0a'
      TABLOCK,
      KEEPNULLS,
      MAXERRORS = 0
    );

    TRUNCATE TABLE bronze.effects_raw;

    INSERT INTO bronze.effects_raw
    (effect_id, session_fk, customer_profile_fk, created_ts, effect_type, name, value,
     _file_name, _load_ts)
    SELECT
      effect_id, session_fk, customer_profile_fk, created_ts, effect_type, name, value,
      N'effects.csv', SYSUTCDATETIME()
    FROM bronze.effects_stage;

    /* ----------------------------- Summary rowcounts --------------------------------- */
    SELECT 'customer_sessions_raw' AS table_name, COUNT(*) AS rows FROM bronze.customer_sessions_raw
    UNION ALL
    SELECT 'effects_raw', COUNT(*) FROM bronze.effects_raw;
  END TRY
  BEGIN CATCH
    THROW; -- bubble the original error
  END CATCH
END;
GO

-- Run it
EXEC bronze.load_bronze;
