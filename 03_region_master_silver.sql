-- Bronze-to-Silver: Master Dynamic Tables – dedup via QUALIFY, normalise, and DQ checks
-- Co-authored with CoCo

-- ============================================================
-- ARCHITECTURE DECISION: Do we need a Stream on the bronze table?
-- ============================================================
-- NO. Dynamic Tables have BUILT-IN change tracking.  Snowflake
-- automatically enables CHANGE_TRACKING on the source table
-- when a DT is created.
--
--  ┌─────────────┐      DT internal        ┌─────────────┐
--  │   BRONZE    │  ──  change tracking ──▶ │   SILVER    │
--  │ REGION_MASTER│     (no Stream needed)   │ REGION_MASTER│
--  └─────────────┘                          └─────────────┘
--
-- Why NOT a Stream?
--  1. DTs are declarative – they query base tables directly,
--     NOT Streams.  Streams are consumed by Tasks (imperative).
--  2. A redundant Stream adds storage cost (change-log retention)
--     with zero benefit for a DT pipeline.
--  3. Fewer objects = less operational overhead in dev/qa/prod.
--
-- When WOULD you use a Stream?
--   Only if you need a Task-triggered side-effect alongside the
--   DT pipeline (e.g. sending notifications, writing to an API).
--
-- COST-SAVING CHOICES
--  • TARGET_LAG = DOWNSTREAM  → this DT refreshes only when a
--    downstream DT (e.g. Gold layer) triggers the refresh chain,
--    so zero unnecessary compute for low-change master data.
--  • REFRESH_MODE = AUTO      → Snowflake picks INCREMENTAL when
--    the query supports it, falls back to FULL otherwise.
--    ROW_NUMBER() dedup typically resolves to FULL, which is
--    perfectly fine for small master tables (< 1 000 rows).
--
-- DEDUP APPROACH: QUALIFY (not CTEs)
--  • QUALIFY ROW_NUMBER() OVER (...) = 1 filters directly on
--    the window function result — no intermediate CTEs needed.
--  • Cleaner, fewer object scans, and idiomatic Snowflake SQL.
-- ============================================================


-- ====================================================================
-- REGION MASTER  (Bronze → Silver)
-- ====================================================================

----------------------------------------------------------------------
-- STEP 1: Enable change tracking on the bronze source
----------------------------------------------------------------------
ALTER TABLE SALES_DEV.BRONZE.REGION_MASTER
    SET CHANGE_TRACKING = TRUE;

----------------------------------------------------------------------
-- STEP 2: Create Silver Region Master Dynamic Table
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.REGION_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked region master. Refreshes on downstream demand.'
AS
SELECT
    -- ── Business columns (trimmed / normalised) ──────────────────
    TRIM(REGION_CODE)              AS REGION_CODE,
    TRIM(REGION_NAME)              AS REGION_NAME,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality: record-level status ────────────────────────
    CASE
        WHEN LENGTH(TRIM(REGION_CODE)) = 0                            THEN 'FAIL'
        WHEN REGION_NAME IS NULL OR LENGTH(TRIM(REGION_NAME)) = 0     THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                THEN 'FAIL'
        WHEN EFFECTIVE_START_DATE IS NULL                              THEN 'FAIL'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE             THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    -- ── Data Quality: first failing reason (NULL = clean) ────────
    CASE
        WHEN LENGTH(TRIM(REGION_CODE)) = 0
            THEN 'REGION_CODE is blank after trim'
        WHEN REGION_NAME IS NULL OR LENGTH(TRIM(REGION_NAME)) = 0
            THEN 'REGION_NAME is null or blank'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid – expected Y/N, got: ' || COALESCE(IS_ACTIVE, 'NULL')
        WHEN EFFECTIVE_START_DATE IS NULL
            THEN 'EFFECTIVE_START_DATE is null'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE
            THEN 'EFFECTIVE_END_DATE (' || EFFECTIVE_END_DATE::VARCHAR
                 || ') precedes EFFECTIVE_START_DATE (' || EFFECTIVE_START_DATE::VARCHAR || ')'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.REGION_MASTER
WHERE REGION_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY REGION_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

----------------------------------------------------------------------
-- STEP 3: Verify the Region Master Dynamic Table
----------------------------------------------------------------------
SELECT * FROM SALES_DEV.SILVER.REGION_MASTER ORDER BY REGION_CODE;


-- ====================================================================
-- COUNTRY MASTER  (Bronze → Silver)
-- ====================================================================

----------------------------------------------------------------------
-- STEP 4: Enable change tracking on bronze country source
----------------------------------------------------------------------
ALTER TABLE SALES_DEV.BRONZE.COUNTRY_MASTER
    SET CHANGE_TRACKING = TRUE;

----------------------------------------------------------------------
-- STEP 5: Create Silver Country Master Dynamic Table
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.COUNTRY_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked country master. Refreshes on downstream demand.'
AS
SELECT
    -- ── Business columns (trimmed / normalised) ──────────────────
    TRIM(COUNTRY_CODE)                       AS COUNTRY_CODE,
    TRIM(COUNTRY_NAME)                       AS COUNTRY_NAME,
    TRIM(REGION_CODE)                        AS REGION_CODE,
    TRIM(CURRENCY_CODE)                      AS CURRENCY_CODE,
    TRIM(TAX_CODE)                           AS TAX_CODE,
    TRIM(PRIMARY_LANGUAGE)                   AS PRIMARY_LANGUAGE,
    TRIM(TIMEZONE)                           AS TIMEZONE,
    UPPER(TRIM(ECOMMERCE_SUPPORTED))         AS ECOMMERCE_SUPPORTED,
    UPPER(TRIM(RETAIL_STORE_SUPPORTED))      AS RETAIL_STORE_SUPPORTED,
    TRIM(MARKET_TIER)                        AS MARKET_TIER,

    -- ── Data Quality: record-level status ────────────────────────
    CASE
        WHEN LENGTH(TRIM(COUNTRY_CODE)) = 0                               THEN 'FAIL'
        WHEN COUNTRY_NAME IS NULL OR LENGTH(TRIM(COUNTRY_NAME)) = 0       THEN 'FAIL'
        WHEN REGION_CODE IS NULL OR LENGTH(TRIM(REGION_CODE)) = 0         THEN 'FAIL'
        WHEN CURRENCY_CODE IS NULL OR LENGTH(TRIM(CURRENCY_CODE)) = 0     THEN 'FAIL'
        WHEN TAX_CODE IS NULL OR LENGTH(TRIM(TAX_CODE)) = 0               THEN 'FAIL'
        WHEN UPPER(TRIM(ECOMMERCE_SUPPORTED)) NOT IN ('Y', 'N')          THEN 'FAIL'
        WHEN UPPER(TRIM(RETAIL_STORE_SUPPORTED)) NOT IN ('Y', 'N')       THEN 'FAIL'
        ELSE 'PASS'
    END                                      AS DQ_STATUS,

    -- ── Data Quality: first failing reason (NULL = clean) ────────
    CASE
        WHEN LENGTH(TRIM(COUNTRY_CODE)) = 0
            THEN 'COUNTRY_CODE is blank after trim'
        WHEN COUNTRY_NAME IS NULL OR LENGTH(TRIM(COUNTRY_NAME)) = 0
            THEN 'COUNTRY_NAME is null or blank'
        WHEN REGION_CODE IS NULL OR LENGTH(TRIM(REGION_CODE)) = 0
            THEN 'REGION_CODE (FK) is null or blank'
        WHEN CURRENCY_CODE IS NULL OR LENGTH(TRIM(CURRENCY_CODE)) = 0
            THEN 'CURRENCY_CODE (FK) is null or blank'
        WHEN TAX_CODE IS NULL OR LENGTH(TRIM(TAX_CODE)) = 0
            THEN 'TAX_CODE (FK) is null or blank'
        WHEN UPPER(TRIM(ECOMMERCE_SUPPORTED)) NOT IN ('Y', 'N')
            THEN 'ECOMMERCE_SUPPORTED invalid – expected Y/N, got: '
                 || COALESCE(ECOMMERCE_SUPPORTED, 'NULL')
        WHEN UPPER(TRIM(RETAIL_STORE_SUPPORTED)) NOT IN ('Y', 'N')
            THEN 'RETAIL_STORE_SUPPORTED invalid – expected Y/N, got: '
                 || COALESCE(RETAIL_STORE_SUPPORTED, 'NULL')
        ELSE NULL
    END                                      AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                              AS BRONZE_SOURCE_FILE,
    __LOAD_TS                                AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()                      AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.COUNTRY_MASTER
WHERE COUNTRY_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY COUNTRY_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

----------------------------------------------------------------------
-- STEP 6: Verify the Country Master Dynamic Table
----------------------------------------------------------------------
SELECT * FROM SALES_DEV.SILVER.COUNTRY_MASTER ORDER BY COUNTRY_CODE;


-- ====================================================================
-- CURRENCY MASTER  (Bronze → Silver)
-- ====================================================================

----------------------------------------------------------------------
-- STEP 7: Enable change tracking on bronze currency source
----------------------------------------------------------------------
ALTER TABLE SALES_DEV.BRONZE.CURRENCY_MASTER
    SET CHANGE_TRACKING = TRUE;

----------------------------------------------------------------------
-- STEP 8: Create Silver Currency Master Dynamic Table
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.CURRENCY_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked currency master. Refreshes on downstream demand.'
AS
SELECT
    -- ── Business columns (trimmed / normalised) ──────────────────
    TRIM(CURRENCY_CODE)            AS CURRENCY_CODE,
    TRIM(CURRENCY_NAME)            AS CURRENCY_NAME,
    TRIM(CURRENCY_SYMBOL)          AS CURRENCY_SYMBOL,
    MINOR_UNIT,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality: record-level status ────────────────────────
    CASE
        WHEN LENGTH(TRIM(CURRENCY_CODE)) = 0                              THEN 'FAIL'
        WHEN CURRENCY_NAME IS NULL OR LENGTH(TRIM(CURRENCY_NAME)) = 0     THEN 'FAIL'
        WHEN CURRENCY_SYMBOL IS NULL OR LENGTH(TRIM(CURRENCY_SYMBOL)) = 0 THEN 'FAIL'
        WHEN MINOR_UNIT IS NULL OR MINOR_UNIT NOT BETWEEN 0 AND 4         THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                     THEN 'FAIL'
        WHEN EFFECTIVE_START_DATE IS NULL                                   THEN 'FAIL'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE                  THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    -- ── Data Quality: first failing reason (NULL = clean) ────────
    CASE
        WHEN LENGTH(TRIM(CURRENCY_CODE)) = 0
            THEN 'CURRENCY_CODE is blank after trim'
        WHEN CURRENCY_NAME IS NULL OR LENGTH(TRIM(CURRENCY_NAME)) = 0
            THEN 'CURRENCY_NAME is null or blank'
        WHEN CURRENCY_SYMBOL IS NULL OR LENGTH(TRIM(CURRENCY_SYMBOL)) = 0
            THEN 'CURRENCY_SYMBOL is null or blank'
        WHEN MINOR_UNIT IS NULL
            THEN 'MINOR_UNIT is null'
        WHEN MINOR_UNIT NOT BETWEEN 0 AND 4
            THEN 'MINOR_UNIT out of range (0–4): ' || MINOR_UNIT::VARCHAR
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid – expected Y/N, got: ' || COALESCE(IS_ACTIVE, 'NULL')
        WHEN EFFECTIVE_START_DATE IS NULL
            THEN 'EFFECTIVE_START_DATE is null'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE
            THEN 'EFFECTIVE_END_DATE (' || EFFECTIVE_END_DATE::VARCHAR
                 || ') precedes EFFECTIVE_START_DATE (' || EFFECTIVE_START_DATE::VARCHAR || ')'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.CURRENCY_MASTER
WHERE CURRENCY_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CURRENCY_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

----------------------------------------------------------------------
-- STEP 9: Verify the Currency Master Dynamic Table
----------------------------------------------------------------------
SELECT * FROM SALES_DEV.SILVER.CURRENCY_MASTER ORDER BY CURRENCY_CODE;


-- ====================================================================
-- TAX MASTER  (Bronze → Silver)
-- ====================================================================

----------------------------------------------------------------------
-- STEP 10: Enable change tracking on bronze tax source
----------------------------------------------------------------------
ALTER TABLE SALES_DEV.BRONZE.TAX_MASTER
    SET CHANGE_TRACKING = TRUE;

----------------------------------------------------------------------
-- STEP 11: Create Silver Tax Master Dynamic Table
----------------------------------------------------------------------
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.TAX_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked tax master. Refreshes on downstream demand.'
AS
SELECT
    -- ── Business columns (trimmed / normalised) ──────────────────
    TRIM(TAX_CODE)                 AS TAX_CODE,
    UPPER(TRIM(TAX_TYPE))          AS TAX_TYPE,
    TAX_RATE,
    UPPER(TRIM(TAX_INCLUSIVE_FLAG)) AS TAX_INCLUSIVE_FLAG,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality: record-level status ────────────────────────
    CASE
        WHEN LENGTH(TRIM(TAX_CODE)) = 0                               THEN 'FAIL'
        WHEN TAX_TYPE IS NULL OR LENGTH(TRIM(TAX_TYPE)) = 0           THEN 'FAIL'
        WHEN TAX_RATE IS NULL OR TAX_RATE < 0 OR TAX_RATE > 1        THEN 'FAIL'
        WHEN UPPER(TRIM(TAX_INCLUSIVE_FLAG)) NOT IN ('Y', 'N')        THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                THEN 'FAIL'
        WHEN EFFECTIVE_START_DATE IS NULL                              THEN 'FAIL'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE             THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    -- ── Data Quality: first failing reason (NULL = clean) ────────
    CASE
        WHEN LENGTH(TRIM(TAX_CODE)) = 0
            THEN 'TAX_CODE is blank after trim'
        WHEN TAX_TYPE IS NULL OR LENGTH(TRIM(TAX_TYPE)) = 0
            THEN 'TAX_TYPE is null or blank'
        WHEN TAX_RATE IS NULL
            THEN 'TAX_RATE is null'
        WHEN TAX_RATE < 0 OR TAX_RATE > 1
            THEN 'TAX_RATE out of range (0–1): ' || TAX_RATE::VARCHAR
        WHEN UPPER(TRIM(TAX_INCLUSIVE_FLAG)) NOT IN ('Y', 'N')
            THEN 'TAX_INCLUSIVE_FLAG invalid – expected Y/N, got: '
                 || COALESCE(TAX_INCLUSIVE_FLAG, 'NULL')
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid – expected Y/N, got: ' || COALESCE(IS_ACTIVE, 'NULL')
        WHEN EFFECTIVE_START_DATE IS NULL
            THEN 'EFFECTIVE_START_DATE is null'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE
            THEN 'EFFECTIVE_END_DATE (' || EFFECTIVE_END_DATE::VARCHAR
                 || ') precedes EFFECTIVE_START_DATE (' || EFFECTIVE_START_DATE::VARCHAR || ')'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.TAX_MASTER
WHERE TAX_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TAX_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

----------------------------------------------------------------------
-- STEP 12: Verify the Tax Master Dynamic Table
----------------------------------------------------------------------
SELECT * FROM SALES_DEV.SILVER.TAX_MASTER ORDER BY TAX_CODE;
