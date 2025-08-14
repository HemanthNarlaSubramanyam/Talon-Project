
Quick Overview:
-------------
This script:
1. Creates the database [case_q1_2025] if it does not exist.
2. Creates the schema [bronze] in that database (if missing) without switching DB context.
3. Drops existing raw Bronze tables (customer_sessions_raw, effects_raw) if they exist.
4. Recreates the raw Bronze tables with NVARCHAR columns to preserve source data fidelity.
5. Adds a default UTC load timestamp (_load_ts) to track ingestion time.

Purpose:
--------
Sets up the Bronze layer in the medallion architecture to store raw, untransformed data.
*/

IF DB_ID(N'case_q1_2025') IS NULL
BEGIN
  CREATE DATABASE [case_q1_2025];
END;
GO

/* Create schema in that DB without using USE (CREATE SCHEMA must be first in its batch) */
IF NOT EXISTS (SELECT 1 FROM [case_q1_2025].sys.schemas WHERE name = 'bronze')
BEGIN
  DECLARE @cmd nvarchar(max) = N'CREATE SCHEMA bronze AUTHORIZATION dbo;';
  EXEC [case_q1_2025].sys.sp_executesql @cmd;   -- runs in the case_q1_2025 context
END;
GO

/* Drop & recreate Bronze tables */
IF OBJECT_ID('[case_q1_2025].bronze.customer_sessions_raw','U') IS NOT NULL
  DROP TABLE [case_q1_2025].bronze.customer_sessions_raw;
IF OBJECT_ID('[case_q1_2025].bronze.effects_raw','U') IS NOT NULL
  DROP TABLE [case_q1_2025].bronze.effects_raw;
GO

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
