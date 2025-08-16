/*
===============================================================================
Quick Overview
===============================================================================
This script:
1) Creates the database [case_q1_2025] if it does not exist.
2) Ensures schema [bronze] exists in that database (without issuing USE).
3) Drops existing raw Bronze tables (customer_sessions_raw, effects_raw) if present.
4) Recreates the raw Bronze tables with NVARCHAR columns to preserve fidelity.
5) Adds a default UTC load timestamp (_load_ts) to track ingestion time.

Purpose:
Sets up the Bronze layer in the medallion architecture to store raw, untransformed data.
===============================================================================
*/

-- 1) Create DB if needed
IF DB_ID(N'case_q1_2025') IS NULL
BEGIN
  EXEC('CREATE DATABASE [case_q1_2025]');
END;
GO

-- 2) Create schema [bronze] inside that DB (no USE; CREATE SCHEMA must be first in its batch)
IF NOT EXISTS (SELECT 1 FROM [case_q1_2025].sys.schemas WHERE name = N'bronze')
BEGIN
  DECLARE @cmd nvarchar(max) = N'CREATE SCHEMA bronze AUTHORIZATION dbo;';
  EXEC [case_q1_2025].sys.sp_executesql @cmd;  -- runs in [case_q1_2025] context
END;
GO

-- 3) Drop existing tables if they exist
IF OBJECT_ID(N'[case_q1_2025].bronze.customer_sessions_raw','U') IS NOT NULL
  DROP TABLE [case_q1_2025].bronze.customer_sessions_raw;
IF OBJECT_ID(N'[case_q1_2025].bronze.effects_raw','U') IS NOT NULL
  DROP TABLE [case_q1_2025].bronze.effects_raw;
GO

-- 4) Recreate Bronze raw tables (all text + metadata)
CREATE TABLE [case_q1_2025].bronze.customer_sessions_raw
(
  session_id               NVARCHAR(64)   NULL,
  created                  NVARCHAR(64)   NULL,
  closedAt                 NVARCHAR(64)   NULL,
  cancelledAt              NVARCHAR(64)   NULL,
  state                    NVARCHAR(40)   NULL,
  total_usd                NVARCHAR(50)   NULL,
  customer_profile_fk      NVARCHAR(64)   NULL,
  store_integration_id     NVARCHAR(64)   NULL,
  number_of_cart_items     NVARCHAR(50)   NULL,
  channel                  NVARCHAR(64)   NULL,
  _file_name               NVARCHAR(260)  NULL,
  _load_ts                 DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE [case_q1_2025].bronze.effects_raw
(
  effect_id                NVARCHAR(64)    NULL,
  session_fk               NVARCHAR(64)    NULL,
  customer_profile_fk      NVARCHAR(64)    NULL,
  created_ts               NVARCHAR(64)    NULL,
  effect_type              NVARCHAR(128)   NULL,
  name                     NVARCHAR(256)   NULL,
  value                    NVARCHAR(128)   NULL,
  _file_name               NVARCHAR(260)   NULL,
  _load_ts                 DATETIME2(3)    NOT NULL DEFAULT SYSUTCDATETIME()
);
GO

PRINT '✅ Bronze schema + tables created in [case_q1_2025].bronze';
