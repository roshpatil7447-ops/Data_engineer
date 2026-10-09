-- Bronze-to-Silver: Product Master Dynamic Tables – dedup via QUALIFY, normalise, and DQ checks
-- Co-authored with CoCo
--
-- Product hierarchy:  CATEGORY → FAMILY → MODEL → SKU → COUNTRY_AVAILABILITY
-- Same patterns as country-master silver DTs:
--   • QUALIFY ROW_NUMBER() for dedup (no CTEs)
--   • TARGET_LAG = DOWNSTREAM (refresh only when Gold needs it)
--   • REFRESH_MODE = AUTO
--   • DQ_STATUS / DQ_FAIL_REASON columns on every record


-- ====================================================================
-- 1. PRODUCT CATEGORY MASTER
-- ====================================================================
-- Natural key: CATEGORY_CODE   |   9 rows in bronze

ALTER TABLE SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_CATEGORY_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked product category master.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(CATEGORY_CODE)            AS CATEGORY_CODE,
    TRIM(CATEGORY_NAME)            AS CATEGORY_NAME,
    UPPER(TRIM(REPORTING_SEGMENT)) AS REPORTING_SEGMENT,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(CATEGORY_CODE)) = 0                                     THEN 'FAIL'
        WHEN CATEGORY_NAME IS NULL OR LENGTH(TRIM(CATEGORY_NAME)) = 0            THEN 'FAIL'
        WHEN REPORTING_SEGMENT IS NULL OR LENGTH(TRIM(REPORTING_SEGMENT)) = 0    THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                           THEN 'FAIL'
        WHEN EFFECTIVE_START_DATE IS NULL                                         THEN 'FAIL'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE                        THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(CATEGORY_CODE)) = 0
            THEN 'CATEGORY_CODE is blank'
        WHEN CATEGORY_NAME IS NULL OR LENGTH(TRIM(CATEGORY_NAME)) = 0
            THEN 'CATEGORY_NAME is null or blank'
        WHEN REPORTING_SEGMENT IS NULL OR LENGTH(TRIM(REPORTING_SEGMENT)) = 0
            THEN 'REPORTING_SEGMENT is null or blank'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid: ' || COALESCE(IS_ACTIVE, 'NULL')
        WHEN EFFECTIVE_START_DATE IS NULL
            THEN 'EFFECTIVE_START_DATE is null'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE
            THEN 'EFFECTIVE_END_DATE < EFFECTIVE_START_DATE'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER
WHERE CATEGORY_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CATEGORY_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT * FROM SALES_DEV.SILVER.PRODUCT_CATEGORY_MASTER ORDER BY CATEGORY_CODE;


-- ====================================================================
-- 2. PRODUCT FAMILY MASTER
-- ====================================================================
-- Natural key: FAMILY_CODE   |   FK: CATEGORY_CODE   |   9 rows

ALTER TABLE SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_FAMILY_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked product family master.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(FAMILY_CODE)              AS FAMILY_CODE,
    TRIM(FAMILY_NAME)              AS FAMILY_NAME,
    TRIM(CATEGORY_CODE)            AS CATEGORY_CODE,
    LAUNCH_YEAR,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    UPPER(TRIM(LIFECYCLE_STATUS))  AS LIFECYCLE_STATUS,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(FAMILY_CODE)) = 0                                       THEN 'FAIL'
        WHEN FAMILY_NAME IS NULL OR LENGTH(TRIM(FAMILY_NAME)) = 0                THEN 'FAIL'
        WHEN CATEGORY_CODE IS NULL OR LENGTH(TRIM(CATEGORY_CODE)) = 0            THEN 'FAIL'
        WHEN LAUNCH_YEAR IS NULL
             OR LAUNCH_YEAR < 1900
             OR LAUNCH_YEAR > YEAR(CURRENT_DATE()) + 1                           THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                           THEN 'FAIL'
        WHEN LIFECYCLE_STATUS IS NULL OR LENGTH(TRIM(LIFECYCLE_STATUS)) = 0      THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(FAMILY_CODE)) = 0
            THEN 'FAMILY_CODE is blank'
        WHEN FAMILY_NAME IS NULL OR LENGTH(TRIM(FAMILY_NAME)) = 0
            THEN 'FAMILY_NAME is null or blank'
        WHEN CATEGORY_CODE IS NULL OR LENGTH(TRIM(CATEGORY_CODE)) = 0
            THEN 'CATEGORY_CODE (FK) is null or blank'
        WHEN LAUNCH_YEAR IS NULL
            THEN 'LAUNCH_YEAR is null'
        WHEN LAUNCH_YEAR < 1900 OR LAUNCH_YEAR > YEAR(CURRENT_DATE()) + 1
            THEN 'LAUNCH_YEAR out of range: ' || LAUNCH_YEAR::VARCHAR
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid: ' || COALESCE(IS_ACTIVE, 'NULL')
        WHEN LIFECYCLE_STATUS IS NULL OR LENGTH(TRIM(LIFECYCLE_STATUS)) = 0
            THEN 'LIFECYCLE_STATUS is null or blank'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER
WHERE FAMILY_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY FAMILY_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT * FROM SALES_DEV.SILVER.PRODUCT_FAMILY_MASTER ORDER BY FAMILY_CODE;


-- ====================================================================
-- 3. PRODUCT MODEL MASTER
-- ====================================================================
-- Natural key: MODEL_CODE   |   FK: FAMILY_CODE   |   9 rows

ALTER TABLE SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_MODEL_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked product model master.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(MODEL_CODE)               AS MODEL_CODE,
    TRIM(MODEL_NAME)               AS MODEL_NAME,
    TRIM(FAMILY_CODE)              AS FAMILY_CODE,
    LAUNCH_DATE,
    DISCONTINUE_DATE,
    UPPER(TRIM(LIFECYCLE_STATUS))  AS LIFECYCLE_STATUS,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(MODEL_CODE)) = 0                                        THEN 'FAIL'
        WHEN MODEL_NAME IS NULL OR LENGTH(TRIM(MODEL_NAME)) = 0                  THEN 'FAIL'
        WHEN FAMILY_CODE IS NULL OR LENGTH(TRIM(FAMILY_CODE)) = 0                THEN 'FAIL'
        WHEN LAUNCH_DATE IS NULL                                                  THEN 'FAIL'
        WHEN DISCONTINUE_DATE IS NOT NULL AND DISCONTINUE_DATE < LAUNCH_DATE     THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                           THEN 'FAIL'
        WHEN LIFECYCLE_STATUS IS NULL OR LENGTH(TRIM(LIFECYCLE_STATUS)) = 0      THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(MODEL_CODE)) = 0
            THEN 'MODEL_CODE is blank'
        WHEN MODEL_NAME IS NULL OR LENGTH(TRIM(MODEL_NAME)) = 0
            THEN 'MODEL_NAME is null or blank'
        WHEN FAMILY_CODE IS NULL OR LENGTH(TRIM(FAMILY_CODE)) = 0
            THEN 'FAMILY_CODE (FK) is null or blank'
        WHEN LAUNCH_DATE IS NULL
            THEN 'LAUNCH_DATE is null'
        WHEN DISCONTINUE_DATE IS NOT NULL AND DISCONTINUE_DATE < LAUNCH_DATE
            THEN 'DISCONTINUE_DATE (' || DISCONTINUE_DATE::VARCHAR
                 || ') < LAUNCH_DATE (' || LAUNCH_DATE::VARCHAR || ')'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid: ' || COALESCE(IS_ACTIVE, 'NULL')
        WHEN LIFECYCLE_STATUS IS NULL OR LENGTH(TRIM(LIFECYCLE_STATUS)) = 0
            THEN 'LIFECYCLE_STATUS is null or blank'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER
WHERE MODEL_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY MODEL_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT * FROM SALES_DEV.SILVER.PRODUCT_MODEL_MASTER ORDER BY MODEL_CODE;


-- ====================================================================
-- 4. PRODUCT SKU MASTER
-- ====================================================================
-- Natural key: SKU_CODE   |   FK: MODEL_CODE   |   53 rows

ALTER TABLE SALES_DEV.BRONZE.PRODUCT_SKU_MASTER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_SKU_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked product SKU master.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(SKU_CODE)                 AS SKU_CODE,
    TRIM(MODEL_CODE)               AS MODEL_CODE,
    TRIM(VARIANT)                  AS VARIANT,
    UPPER(TRIM(PRICE_TIER))        AS PRICE_TIER,
    GLOBAL_LAUNCH_DATE,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(SKU_CODE)) = 0                                          THEN 'FAIL'
        WHEN MODEL_CODE IS NULL OR LENGTH(TRIM(MODEL_CODE)) = 0                  THEN 'FAIL'
        WHEN VARIANT IS NULL OR LENGTH(TRIM(VARIANT)) = 0                        THEN 'FAIL'
        WHEN PRICE_TIER IS NULL OR LENGTH(TRIM(PRICE_TIER)) = 0                  THEN 'FAIL'
        WHEN GLOBAL_LAUNCH_DATE IS NULL                                           THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                           THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(SKU_CODE)) = 0
            THEN 'SKU_CODE is blank'
        WHEN MODEL_CODE IS NULL OR LENGTH(TRIM(MODEL_CODE)) = 0
            THEN 'MODEL_CODE (FK) is null or blank'
        WHEN VARIANT IS NULL OR LENGTH(TRIM(VARIANT)) = 0
            THEN 'VARIANT is null or blank'
        WHEN PRICE_TIER IS NULL OR LENGTH(TRIM(PRICE_TIER)) = 0
            THEN 'PRICE_TIER is null or blank'
        WHEN GLOBAL_LAUNCH_DATE IS NULL
            THEN 'GLOBAL_LAUNCH_DATE is null'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')
            THEN 'IS_ACTIVE invalid: ' || COALESCE(IS_ACTIVE, 'NULL')
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_SKU_MASTER
WHERE SKU_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SKU_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT * FROM SALES_DEV.SILVER.PRODUCT_SKU_MASTER ORDER BY SKU_CODE;


-- ====================================================================
-- 5. PRODUCT COUNTRY AVAILABILITY
-- ====================================================================
-- Composite key: SKU_CODE + COUNTRY_CODE   |   371 rows

ALTER TABLE SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.PRODUCT_COUNTRY_AVAILABILITY
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked product-country availability.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(SKU_CODE)                 AS SKU_CODE,
    TRIM(COUNTRY_CODE)             AS COUNTRY_CODE,
    LOCAL_LAUNCH_DATE,
    LOCAL_DISCONTINUE_DATE,
    UPPER(TRIM(IS_AVAILABLE))      AS IS_AVAILABLE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(SKU_CODE)) = 0                                          THEN 'FAIL'
        WHEN COUNTRY_CODE IS NULL OR LENGTH(TRIM(COUNTRY_CODE)) = 0              THEN 'FAIL'
        WHEN LOCAL_LAUNCH_DATE IS NULL                                            THEN 'FAIL'
        WHEN LOCAL_DISCONTINUE_DATE IS NOT NULL
             AND LOCAL_DISCONTINUE_DATE < LOCAL_LAUNCH_DATE                       THEN 'FAIL'
        WHEN UPPER(TRIM(IS_AVAILABLE)) NOT IN ('Y', 'N')                         THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(SKU_CODE)) = 0
            THEN 'SKU_CODE (FK) is blank'
        WHEN COUNTRY_CODE IS NULL OR LENGTH(TRIM(COUNTRY_CODE)) = 0
            THEN 'COUNTRY_CODE (FK) is null or blank'
        WHEN LOCAL_LAUNCH_DATE IS NULL
            THEN 'LOCAL_LAUNCH_DATE is null'
        WHEN LOCAL_DISCONTINUE_DATE IS NOT NULL
             AND LOCAL_DISCONTINUE_DATE < LOCAL_LAUNCH_DATE
            THEN 'LOCAL_DISCONTINUE_DATE (' || LOCAL_DISCONTINUE_DATE::VARCHAR
                 || ') < LOCAL_LAUNCH_DATE (' || LOCAL_LAUNCH_DATE::VARCHAR || ')'
        WHEN UPPER(TRIM(IS_AVAILABLE)) NOT IN ('Y', 'N')
            THEN 'IS_AVAILABLE invalid: ' || COALESCE(IS_AVAILABLE, 'NULL')
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY
WHERE SKU_CODE IS NOT NULL AND COUNTRY_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SKU_CODE, COUNTRY_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT * FROM SALES_DEV.SILVER.PRODUCT_COUNTRY_AVAILABILITY ORDER BY SKU_CODE, COUNTRY_CODE;
