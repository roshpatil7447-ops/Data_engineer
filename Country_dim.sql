-- =====================================================
-- Gold Layer - DIM_COUNTRY (SCD Type 2)
-- Combines Region, Country, Currency, Tax masters
-- Hash-based surrogate key for fact table joins
--
-- Pipeline: BRONZE → SILVER (INCREMENTAL) → GOLD (INCREMENTAL)
-- All layers use deterministic functions only to enable
-- incremental refresh throughout the pipeline.
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Country dimension combining region, country, currency and tax attributes for geographic analysis'
AS
SELECT
    -- Surrogate Key (Hash-based for fact table joins)
    SHA2(CONCAT(
        COALESCE(c.COUNTRY_CODE, ''),
        COALESCE(TO_VARCHAR(c.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                         AS COUNTRY_DIM_KEY,

    -- Business Key
    c.COUNTRY_CODE                  AS COUNTRY_CODE,

    -- Country Attributes
    c.COUNTRY_NAME                  AS COUNTRY_NAME,
    c.PRIMARY_LANGUAGE              AS PRIMARY_LANGUAGE,
    c.TIMEZONE                      AS TIMEZONE,
    c.MARKET_TIER                   AS MARKET_TIER,
    c.ECOMMERCE_SUPPORTED           AS ECOMMERCE_SUPPORTED,
    c.RETAIL_STORE_SUPPORTED        AS RETAIL_STORE_SUPPORTED,

    -- Region Attributes
    r.REGION_CODE                   AS REGION_CODE,
    r.REGION_NAME                   AS REGION_NAME,

    -- Currency Attributes
    cu.CURRENCY_CODE                AS CURRENCY_CODE,
    cu.CURRENCY_NAME                AS CURRENCY_NAME,
    cu.CURRENCY_SYMBOL              AS CURRENCY_SYMBOL,

    -- Tax Attributes
    t.TAX_CODE                      AS TAX_CODE,
    t.TAX_TYPE                      AS TAX_TYPE,
    t.TAX_RATE                      AS TAX_RATE,
    t.TAX_INCLUSIVE_FLAG             AS TAX_INCLUSIVE_FLAG,

    -- SCD Type 2 Columns
    c.BRONZE_LOAD_TS                AS EFFECTIVE_START_TS,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP_NTZ) AS EFFECTIVE_END_TS,
    TRUE                            AS IS_CURRENT,

    -- Audit
    c.BRONZE_LOAD_TS                AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.COUNTRY_MASTER c
LEFT JOIN SALES_DEV.SILVER.REGION_MASTER r
    ON c.REGION_CODE = r.REGION_CODE AND r.DQ_STATUS = 'PASS'
LEFT JOIN SALES_DEV.SILVER.CURRENCY_MASTER cu
    ON c.CURRENCY_CODE = cu.CURRENCY_CODE AND cu.DQ_STATUS = 'PASS'
LEFT JOIN SALES_DEV.SILVER.TAX_MASTER t
    ON c.TAX_CODE = t.TAX_CODE AND t.DQ_STATUS = 'PASS'
WHERE c.DQ_STATUS = 'PASS';


-- =====================================================
-- Column Comments for Semantic Layer
-- =====================================================
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN COUNTRY_DIM_KEY COMMENT 'Hash-based surrogate key for fact table joins';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN COUNTRY_CODE COMMENT 'ISO country code - business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN COUNTRY_NAME COMMENT 'Full country name for display';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN PRIMARY_LANGUAGE COMMENT 'Primary spoken language';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN TIMEZONE COMMENT 'Primary timezone identifier';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN MARKET_TIER COMMENT 'Market classification tier for business segmentation';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN ECOMMERCE_SUPPORTED COMMENT 'E-commerce operations enabled flag';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN RETAIL_STORE_SUPPORTED COMMENT 'Physical retail store operations flag';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN REGION_CODE COMMENT 'Geographic region code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN REGION_NAME COMMENT 'Geographic region name for grouping';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN CURRENCY_CODE COMMENT 'ISO currency code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN CURRENCY_NAME COMMENT 'Full currency name';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN CURRENCY_SYMBOL COMMENT 'Currency display symbol';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN TAX_CODE COMMENT 'Tax configuration code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN TAX_TYPE COMMENT 'Type of tax (VAT/GST/SALES_TAX)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN TAX_RATE COMMENT 'Tax rate as decimal for calculations';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN TAX_INCLUSIVE_FLAG COMMENT 'Prices include tax flag';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN EFFECTIVE_START_TS COMMENT 'SCD2 record effective start timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN EFFECTIVE_END_TS COMMENT 'SCD2 record effective end timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_COUNTRY ALTER COLUMN IS_CURRENT COMMENT 'SCD2 current record indicator';
