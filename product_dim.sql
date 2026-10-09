-- =====================================================
-- Gold Layer - DIM_PRODUCT + BRIDGE_PRODUCT_COUNTRY
--
-- DIM_PRODUCT: Flattened product hierarchy
--   Category > Family > Model > SKU (grain = SKU)
--   Sources: PRODUCT_CATEGORY_MASTER, PRODUCT_FAMILY_MASTER,
--            PRODUCT_MODEL_MASTER, PRODUCT_SKU_MASTER
--
-- BRIDGE_PRODUCT_COUNTRY: Many-to-many availability bridge
--   Grain = SKU × Country
--   Source: PRODUCT_COUNTRY_AVAILABILITY
--
-- Design: Kept separate because merging the bridge into
-- the dimension would fan out 53 SKU rows to 371,
-- duplicating product attributes per country.
-- Fact tables join to DIM_PRODUCT on SKU_CODE;
-- only queries needing country availability also join
-- through the bridge.
--
-- Pipeline: BRONZE → SILVER (INCREMENTAL) → GOLD (INCREMENTAL)
-- =====================================================


-- ==========================================================
-- 1. DIM_PRODUCT — Flattened product hierarchy
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Product dimension flattening the Category > Family > Model > SKU hierarchy. Grain is one row per SKU. Use PRODUCT_DIM_KEY for fact table joins.'
AS
SELECT
    -- Surrogate Key
    SHA2(CONCAT(
        COALESCE(s.SKU_CODE, ''),
        COALESCE(TO_VARCHAR(s.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                         AS PRODUCT_DIM_KEY,

    -- SKU Level (leaf / grain)
    s.SKU_CODE                      AS SKU_CODE,
    s.VARIANT                       AS VARIANT,
    s.PRICE_TIER                    AS PRICE_TIER,
    s.GLOBAL_LAUNCH_DATE            AS GLOBAL_LAUNCH_DATE,
    s.IS_ACTIVE                     AS SKU_IS_ACTIVE,

    -- Model Level
    m.MODEL_CODE                    AS MODEL_CODE,
    m.MODEL_NAME                    AS MODEL_NAME,
    m.LAUNCH_DATE                   AS MODEL_LAUNCH_DATE,
    m.DISCONTINUE_DATE              AS MODEL_DISCONTINUE_DATE,
    m.LIFECYCLE_STATUS              AS MODEL_LIFECYCLE_STATUS,

    -- Family Level
    f.FAMILY_CODE                   AS FAMILY_CODE,
    f.FAMILY_NAME                   AS FAMILY_NAME,
    f.LAUNCH_YEAR                   AS FAMILY_LAUNCH_YEAR,
    f.LIFECYCLE_STATUS              AS FAMILY_LIFECYCLE_STATUS,

    -- Category Level (top)
    c.CATEGORY_CODE                 AS CATEGORY_CODE,
    c.CATEGORY_NAME                 AS CATEGORY_NAME,
    c.REPORTING_SEGMENT             AS REPORTING_SEGMENT,

    -- SCD Type 2 Columns
    s.BRONZE_LOAD_TS                AS EFFECTIVE_START_TS,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP_NTZ) AS EFFECTIVE_END_TS,
    TRUE                            AS IS_CURRENT,

    -- Audit
    s.BRONZE_LOAD_TS                AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.PRODUCT_SKU_MASTER s
LEFT JOIN SALES_DEV.SILVER.PRODUCT_MODEL_MASTER m
    ON s.MODEL_CODE = m.MODEL_CODE AND m.DQ_STATUS = 'PASS'
LEFT JOIN SALES_DEV.SILVER.PRODUCT_FAMILY_MASTER f
    ON m.FAMILY_CODE = f.FAMILY_CODE AND f.DQ_STATUS = 'PASS'
LEFT JOIN SALES_DEV.SILVER.PRODUCT_CATEGORY_MASTER c
    ON f.CATEGORY_CODE = c.CATEGORY_CODE AND c.DQ_STATUS = 'PASS'
WHERE s.DQ_STATUS = 'PASS';


-- Column comments for semantic layer
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN PRODUCT_DIM_KEY COMMENT 'Hash-based surrogate key for fact table joins';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN SKU_CODE COMMENT 'Product SKU code - business key at leaf grain';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN VARIANT COMMENT 'SKU variant descriptor (colour, size, config)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN PRICE_TIER COMMENT 'Pricing tier classification for revenue analysis';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN GLOBAL_LAUNCH_DATE COMMENT 'Global launch date for this SKU';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN SKU_IS_ACTIVE COMMENT 'Whether this SKU is currently active (Y/N)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN MODEL_CODE COMMENT 'Product model code - parent of SKU';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN MODEL_NAME COMMENT 'Product model display name';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN MODEL_LAUNCH_DATE COMMENT 'Date the model was launched';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN MODEL_DISCONTINUE_DATE COMMENT 'Date the model was or will be discontinued';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN MODEL_LIFECYCLE_STATUS COMMENT 'Model lifecycle stage (ACTIVE, DISCONTINUED, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN FAMILY_CODE COMMENT 'Product family code - parent of model';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN FAMILY_NAME COMMENT 'Product family display name';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN FAMILY_LAUNCH_YEAR COMMENT 'Year the product family was launched';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN FAMILY_LIFECYCLE_STATUS COMMENT 'Family lifecycle stage (ACTIVE, DISCONTINUED, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN CATEGORY_CODE COMMENT 'Product category code - top of hierarchy';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN CATEGORY_NAME COMMENT 'Product category display name';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN REPORTING_SEGMENT COMMENT 'High-level reporting segment for executive dashboards';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN EFFECTIVE_START_TS COMMENT 'SCD2 record effective start timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN EFFECTIVE_END_TS COMMENT 'SCD2 record effective end timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_PRODUCT ALTER COLUMN IS_CURRENT COMMENT 'SCD2 current record indicator';


-- ==========================================================
-- 2. BRIDGE_PRODUCT_COUNTRY — SKU × Country availability
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Bridge table linking products (SKUs) to countries with local availability dates. Grain is one row per SKU-Country combination. Join to DIM_PRODUCT on SKU_CODE and to DIM_COUNTRY on COUNTRY_CODE.'
AS
SELECT
    -- Composite hash key
    SHA2(CONCAT(
        COALESCE(a.SKU_CODE, ''),
        '|',
        COALESCE(a.COUNTRY_CODE, ''),
        '|',
        COALESCE(TO_VARCHAR(a.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                         AS BRIDGE_KEY,

    -- Foreign keys
    a.SKU_CODE                      AS SKU_CODE,
    a.COUNTRY_CODE                  AS COUNTRY_CODE,

    -- Availability attributes
    a.LOCAL_LAUNCH_DATE             AS LOCAL_LAUNCH_DATE,
    a.LOCAL_DISCONTINUE_DATE        AS LOCAL_DISCONTINUE_DATE,
    a.IS_AVAILABLE                  AS IS_AVAILABLE,

    -- SCD Type 2 Columns
    a.BRONZE_LOAD_TS                AS EFFECTIVE_START_TS,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP_NTZ) AS EFFECTIVE_END_TS,
    TRUE                            AS IS_CURRENT,

    -- Audit
    a.BRONZE_LOAD_TS                AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.PRODUCT_COUNTRY_AVAILABILITY a
WHERE a.DQ_STATUS = 'PASS';


-- Column comments for semantic layer
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN BRIDGE_KEY COMMENT 'Hash-based key for this SKU-Country availability record';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN SKU_CODE COMMENT 'FK to DIM_PRODUCT - identifies the product SKU';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN COUNTRY_CODE COMMENT 'FK to DIM_COUNTRY - identifies the country';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN LOCAL_LAUNCH_DATE COMMENT 'Country-specific product launch date';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN LOCAL_DISCONTINUE_DATE COMMENT 'Country-specific product discontinuation date';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN IS_AVAILABLE COMMENT 'Whether the SKU is currently available in this country (Y/N)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN EFFECTIVE_START_TS COMMENT 'SCD2 record effective start timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN EFFECTIVE_END_TS COMMENT 'SCD2 record effective end timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.BRIDGE_PRODUCT_COUNTRY ALTER COLUMN IS_CURRENT COMMENT 'SCD2 current record indicator';
