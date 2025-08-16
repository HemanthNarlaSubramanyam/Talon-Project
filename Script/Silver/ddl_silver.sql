/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
  Creates the 'silver' schema objects (tables + indexes) in [case_q1_2025].
  - Drops same-named views/tables first (idempotent).
  - Defines typed, standardized tables ready for downstream Gold views.
===============================================================================
*/

USE [case_q1_2025];
GO
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

/* Ensure schema exists */
IF SCHEMA_ID(N'silver') IS NULL
  EXEC(N'CREATE SCHEMA silver AUTHORIZATION dbo;');
GO

/* Drop any same-named VIEWS first (safe re-run) */
IF OBJECT_ID(N'silver.sessions_with_discounts','V') IS NOT NULL DROP VIEW silver.sessions_with_discounts;
IF OBJECT_ID(N'silver.effects_summary','V')        IS NOT NULL DROP VIEW silver.effects_summary;
IF OBJECT_ID(N'silver.customer_sessions','V')      IS NOT NULL DROP VIEW silver.customer_sessions;
GO

/* Drop TABLES in dependency order */
IF OBJECT_ID(N'silver.sessions_with_discounts','U') IS NOT NULL DROP TABLE silver.sessions_with_discounts;
IF OBJECT_ID(N'silver.effects_summary','U')          IS NOT NULL DROP TABLE silver.effects_summary;
IF OBJECT_ID(N'silver.customer_sessions','U')        IS NOT NULL DROP TABLE silver.customer_sessions;
GO

/* 1) silver.customer_sessions — typed, normalized, de-duped grain */
CREATE TABLE silver.customer_sessions (
  session_id            NVARCHAR(64)   NOT NULL,
  created_at            DATETIME2(3)   NULL,
  state                 NVARCHAR(20)   NULL,        -- lowercase 'closed','cancelled','open', etc.
  total_usd             DECIMAL(18,2)  NULL,
  number_of_cart_items  INT            NULL,
  store_integration_id  NVARCHAR(64)   NULL,
  channel               NVARCHAR(10)   NULL,        -- 'Online' / 'Offline'
  _src_load_ts          DATETIME2(3)   NOT NULL,
  CONSTRAINT PK_silver_customer_sessions PRIMARY KEY CLUSTERED (session_id)
);
GO

/* Helpful indexes for common filters */
CREATE INDEX IX_silver_sessions_created
  ON silver.customer_sessions (created_at)
  INCLUDE (state, channel, total_usd);
GO
CREATE INDEX IX_silver_sessions_state_channel
  ON silver.customer_sessions (state, channel);
GO

/* 2) silver.effects_summary — per-session discount rollup */
CREATE TABLE silver.effects_summary (
  session_id                 NVARCHAR(64)   NOT NULL,
  discount_amount_usd        DECIMAL(18,2)  NOT NULL DEFAULT (0),
  representative_effect_type NVARCHAR(128)  NULL,
  _src_load_ts               DATETIME2(3)   NOT NULL,
  CONSTRAINT PK_silver_effects_summary PRIMARY KEY CLUSTERED (session_id)
);
GO

/* 3) silver.sessions_with_discounts — reporting-ready denormalized table */
CREATE TABLE silver.sessions_with_discounts (
  session_id            NVARCHAR(64)   NOT NULL,
  created_at            DATETIME2(3)   NULL,
  state                 NVARCHAR(20)   NULL,
  number_of_cart_items  INT            NULL,
  store_integration_id  NVARCHAR(64)   NULL,
  channel               NVARCHAR(10)   NULL,
  total_usd             DECIMAL(18,2)  NULL,
  discount_amount_usd   DECIMAL(18,2)  NULL,
  -- Computed metrics
  net_revenue AS (
    CASE WHEN state = N'closed'
         THEN total_usd - ISNULL(discount_amount_usd, 0)
    END
  ) PERSISTED,
  discount_depth AS (
    CASE WHEN state = N'closed' AND total_usd > 0
         THEN ISNULL(discount_amount_usd, 0) / total_usd
    END
  ) PERSISTED,
  _src_load_ts          DATETIME2(3)   NOT NULL,
  CONSTRAINT PK_silver_sessions_with_discounts PRIMARY KEY CLUSTERED (session_id)
);
GO

/* Targeted index for time-series reporting */
CREATE INDEX IX_silver_swd_created
  ON silver.sessions_with_discounts (created_at)
  INCLUDE (state, channel, total_usd, discount_amount_usd, net_revenue);
GO

PRINT N'✅ Silver DDL complete.';
