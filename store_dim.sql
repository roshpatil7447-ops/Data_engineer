-- =====================================================
-- Gold Layer - DIM_STORE
-- Single upstream: SILVER.STORE_MASTER
--
-- Store dimension with location, format, and
-- operational attributes. Grain = one row per store.
--
-- Pipeline: BRONZE → SILVER (INCREMENTAL) → GOLD (INCREMENTAL)
-- =====================================================

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Store dimension with location, format, and operational attributes. Grain is one row per store. Use STORE_DIM_KEY for fact table joins.'
AS
SELECT
    -- Surrogate Key
    SHA2(CONCAT(
        COALESCE(s.STORE_CODE, ''),
        COALESCE(TO_VARCHAR(s.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                         AS STORE_DIM_KEY,

    -- Business Key
    s.STORE_CODE                    AS STORE_CODE,

    -- Store Attributes
    s.STORE_NAME                    AS STORE_NAME,
    s.FORMAT_CODE                   AS FORMAT_CODE,
    s.LIFECYCLE_STATUS              AS LIFECYCLE_STATUS,
    s.IS_ACTIVE                     AS IS_ACTIVE,

    -- Geography
    s.COUNTRY_CODE                  AS COUNTRY_CODE,
    s.REGION_CODE                   AS REGION_CODE,
    s.STATE_CODE                    AS STATE_CODE,
    s.CITY                          AS CITY,
    s.POSTAL_CODE                   AS POSTAL_CODE,
    s.ADDRESS_LINE1                 AS ADDRESS_LINE1,
    s.LATITUDE                      AS LATITUDE,
    s.LONGITUDE                     AS LONGITUDE,

    -- Tax
    s.TAX_JURISDICTION_CODE         AS TAX_JURISDICTION_CODE,

    -- Operational
    s.STORE_OPEN_DATE               AS STORE_OPEN_DATE,
    s.STORE_CLOSE_DATE              AS STORE_CLOSE_DATE,
    s.FLOOR_AREA_SQFT               AS FLOOR_AREA_SQFT,
    s.ANNUAL_RENT_USD               AS ANNUAL_RENT_USD,

    -- SCD Type 2 Columns
    s.BRONZE_LOAD_TS                AS EFFECTIVE_START_TS,
    CAST('9999-12-31 23:59:59' AS TIMESTAMP_NTZ) AS EFFECTIVE_END_TS,
    TRUE                            AS IS_CURRENT,

    -- Audit
    s.BRONZE_LOAD_TS                AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.STORE_MASTER s
WHERE s.DQ_STATUS = 'PASS';


-- =====================================================
-- Column Comments for Semantic Layer
-- =====================================================
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN STORE_DIM_KEY COMMENT 'Hash-based surrogate key for fact table joins';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN STORE_CODE COMMENT 'Store code - business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN STORE_NAME COMMENT 'Full store display name';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN FORMAT_CODE COMMENT 'Store format type (FLAGSHIP, OUTLET, STANDARD, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN LIFECYCLE_STATUS COMMENT 'Store lifecycle stage (OPEN, CLOSED, PLANNED, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN IS_ACTIVE COMMENT 'Whether store is currently active (Y/N)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN COUNTRY_CODE COMMENT 'FK to DIM_COUNTRY - country where store is located';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN REGION_CODE COMMENT 'Geographic region code for the store';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN STATE_CODE COMMENT 'State or province code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN CITY COMMENT 'City where the store is located';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN POSTAL_CODE COMMENT 'Postal or ZIP code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN ADDRESS_LINE1 COMMENT 'Street address of the store';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN LATITUDE COMMENT 'GPS latitude for geospatial analysis';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN LONGITUDE COMMENT 'GPS longitude for geospatial analysis';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN TAX_JURISDICTION_CODE COMMENT 'Tax jurisdiction code applicable to this store location';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN STORE_OPEN_DATE COMMENT 'Date the store opened';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN STORE_CLOSE_DATE COMMENT 'Date the store closed (NULL if still open)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN FLOOR_AREA_SQFT COMMENT 'Store floor area in square feet for capacity analysis';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN ANNUAL_RENT_USD COMMENT 'Annual rent in USD for cost analysis';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN EFFECTIVE_START_TS COMMENT 'SCD2 record effective start timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN EFFECTIVE_END_TS COMMENT 'SCD2 record effective end timestamp';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_STORE ALTER COLUMN IS_CURRENT COMMENT 'SCD2 current record indicator';
