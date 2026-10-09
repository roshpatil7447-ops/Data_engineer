-- =====================================================
-- Task Graph: Bronze Layer Delta Data Loading
--
-- Root task: LOAD_BRONZE_DATA (scheduled, runs daily)
-- Sub-tasks (run in parallel after root):
--   LOAD_COUNTRY_MASTERS       : country, currency, tax  (all in country-master/)
--   LOAD_PRODUCT_MASTERS       : category, family, model, SKU, availability (all in product-master/)
--   LOAD_STORE_CUSTOMER        : store (store-master/), customer (customer-master/)
--   LOAD_SALES_TRANSACTIONS    : sales header, sales item (all in sales-transaction/)
--
-- Stage layout:
--   @STG_SALES_ANALYTICS/delta-load/country-master/       → country, currency, tax
--   @STG_SALES_ANALYTICS/delta-load/product-master/       → category, family, model, SKU, availability
--   @STG_SALES_ANALYTICS/delta-load/store-master/         → store
--   @STG_SALES_ANALYTICS/delta-load/customer-master/      → customer
--   @STG_SALES_ANALYTICS/delta-load/sales-transaction/    → sales header, sales item
--
-- Each COPY uses PATTERN to pick the correct file(s) from a shared folder.
-- Snowflake's COPY load metadata tracks already-loaded files
-- and skips them automatically on subsequent runs.
-- =====================================================


-- ==========================================================
-- 1. ROOT TASK - Entry point for the pipeline
--    Schedule: Daily at 2 AM UTC
--    Body: lightweight check that the stage exists
-- ==========================================================
CREATE OR REPLACE TASK SALES_DEV.BRONZE.LOAD_BRONZE_DATA
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 2 * * * UTC'
  COMMENT = 'Root task for bronze layer data loading. Triggers 4 parallel sub-tasks that COPY data from internal stage to bronze tables.'
AS
  SELECT 1;



-- ==========================================================
-- 2. LOAD_COUNTRY_MASTERS
--    Tables: COUNTRY_MASTER, CURRENCY_MASTER, TAX_MASTER
--    All files live under: delta-load/country-master/
--    Note: No region-master file exists on stage.
-- ==========================================================
CREATE OR REPLACE TASK SALES_DEV.BRONZE.LOAD_COUNTRY_MASTERS
  WAREHOUSE = COMPUTE_WH
  AFTER SALES_DEV.BRONZE.LOAD_BRONZE_DATA
AS
BEGIN
    COPY INTO SALES_DEV.BRONZE.COUNTRY_MASTER
    FROM (
        SELECT
             $1,   -- COUNTRY_CODE
             $2,   -- COUNTRY_NAME
             $3,   -- REGION_CODE
             $4,   -- CURRENCY_CODE
             $5,   -- TAX_CODE
             $6,   -- PRIMARY_LANGUAGE
             $7,   -- TIMEZONE
             $8,   -- ECOMMERCE_SUPPORTED
             $9,   -- RETAIL_STORE_SUPPORTED
             $10,  -- MARKET_TIER
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/country-master/
    )
    PATTERN = '.*country_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.CURRENCY_MASTER
    FROM (
        SELECT
             $1,  -- CURRENCY_CODE
             $2,  -- CURRENCY_NAME
             $3,  -- CURRENCY_SYMBOL
             $4,  -- MINOR_UNIT
             $5,  -- IS_ACTIVE
             $6,  -- EFFECTIVE_START_DATE
             $7,  -- EFFECTIVE_END_DATE
             $8,  -- CREATED_AT
             $9,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/country-master/
    )
    PATTERN = '.*currency_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.TAX_MASTER
    FROM (
        SELECT
             $1,  -- TAX_CODE
             $2,  -- TAX_TYPE
             $3,  -- TAX_RATE
             $4,  -- TAX_INCLUSIVE_FLAG
             $5,  -- EFFECTIVE_START_DATE
             $6,  -- EFFECTIVE_END_DATE
             $7,  -- IS_ACTIVE
             $8,  -- CREATED_AT
             $9,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/country-master/
    )
    PATTERN = '.*tax_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';
END;


-- ==========================================================
-- 3. LOAD_PRODUCT_MASTERS
--    Tables: PRODUCT_CATEGORY_MASTER, PRODUCT_FAMILY_MASTER,
--            PRODUCT_MODEL_MASTER, PRODUCT_SKU_MASTER,
--            PRODUCT_COUNTRY_AVAILABILITY
--    All files live under: delta-load/product-master/
-- ==========================================================
CREATE OR REPLACE TASK SALES_DEV.BRONZE.LOAD_PRODUCT_MASTERS
  WAREHOUSE = COMPUTE_WH
  AFTER SALES_DEV.BRONZE.LOAD_BRONZE_DATA
AS
BEGIN
    COPY INTO SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER
    FROM (
        SELECT
             $1,  -- CATEGORY_CODE
             $2,  -- CATEGORY_NAME
             $3,  -- REPORTING_SEGMENT
             $4,  -- IS_ACTIVE
             $5,  -- EFFECTIVE_START_DATE
             $6,  -- EFFECTIVE_END_DATE
             $7,  -- CREATED_AT
             $8,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/product-master/
    )
    PATTERN = '.*product_category_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER
    FROM (
        SELECT
             $1,  -- FAMILY_CODE
             $2,  -- FAMILY_NAME
             $3,  -- CATEGORY_CODE
             $4,  -- LAUNCH_YEAR
             $5,  -- IS_ACTIVE
             $6,  -- LIFECYCLE_STATUS
             $7,  -- CREATED_AT
             $8,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/product-master/
    )
    PATTERN = '.*product_family_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER
    FROM (
        SELECT
             $1,  -- MODEL_CODE
             $2,  -- MODEL_NAME
             $3,  -- FAMILY_CODE
             $4,  -- LAUNCH_DATE
             $5,  -- DISCONTINUE_DATE
             $6,  -- LIFECYCLE_STATUS
             $7,  -- IS_ACTIVE
             $8,  -- CREATED_AT
             $9,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/product-master/
    )
    PATTERN = '.*product_model_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.PRODUCT_SKU_MASTER
    FROM (
        SELECT
             $1,  -- SKU_CODE
             $2,  -- MODEL_CODE
             $3,  -- VARIANT
             $4,  -- PRICE_TIER
             $5,  -- GLOBAL_LAUNCH_DATE
             $6,  -- IS_ACTIVE
             $7,  -- CREATED_AT
             $8,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/product-master/
    )
    PATTERN = '.*product_sku_master.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY
    FROM (
        SELECT
             $1,  -- SKU_CODE
             $2,  -- COUNTRY_CODE
             $3,  -- LOCAL_LAUNCH_DATE
             $4,  -- LOCAL_DISCONTINUE_DATE
             $5,  -- IS_AVAILABLE
             $6,  -- CREATED_AT
             $7,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/product-master/
    )
    PATTERN = '.*product_country_availability.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';
END;


-- ==========================================================
-- 4. LOAD_STORE_CUSTOMER
--    Tables: STORE_MASTER, CUSTOMER_MASTER
-- ==========================================================
CREATE OR REPLACE TASK SALES_DEV.BRONZE.LOAD_STORE_CUSTOMER
  WAREHOUSE = COMPUTE_WH
  AFTER SALES_DEV.BRONZE.LOAD_BRONZE_DATA
AS
BEGIN
    COPY INTO SALES_DEV.BRONZE.STORE_MASTER
    FROM (
        SELECT
             $1,   -- STORE_CODE
             $2,   -- STORE_NAME
             $3,   -- COUNTRY_CODE
             $4,   -- REGION_CODE
             $5,   -- TAX_JURISDICTION_CODE
             $6,   -- FORMAT_CODE
             $7,   -- CITY
             $8,   -- STATE_CODE
             $9,   -- POSTAL_CODE
             $10,  -- ADDRESS_LINE1
             $11,  -- LATITUDE
             $12,  -- LONGITUDE
             $13,  -- STORE_OPEN_DATE
             $14,  -- STORE_CLOSE_DATE
             $15,  -- LIFECYCLE_STATUS
             $16,  -- FLOOR_AREA_SQFT
             $17,  -- ANNUAL_RENT_USD
             $18,  -- IS_ACTIVE
             $19,  -- EFFECTIVE_START_DATE
             $20,  -- EFFECTIVE_END_DATE
             $21,  -- CREATED_AT
             $22,  -- SOURCE_SYSTEM
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/store-master/
    )
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.CUSTOMER_MASTER
    FROM (
        SELECT
             $1,   -- CUSTOMER_ID
             $2,   -- CUSTOMER_NUMBER
             $3,   -- FIRST_NAME
             $4,   -- LAST_NAME
             $5,   -- FULL_NAME
             $6,   -- GENDER
             $7,   -- DATE_OF_BIRTH
             $8,   -- EMAIL
             $9,   -- PHONE_NUMBER
             $10,  -- STREET_ADDRESS
             $11,  -- CITY
             $12,  -- STATE_PROVINCE
             $13,  -- POSTAL_CODE
             $14,  -- COUNTRY_CODE
             $15,  -- COUNTRY_NAME
             $16,  -- REGION
             $17,  -- PREFERRED_LANGUAGE
             $18,  -- CUSTOMER_SEGMENT
             $19,  -- LOYALTY_TIER
             $20,  -- REGISTRATION_DATE
             $21,  -- IS_ACTIVE
             $22,  -- SOURCE_SYSTEM
             $23,  -- RECORD_SOURCE
             $24,  -- CREATED_AT
             $25,  -- UPDATED_AT
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/customer-master/
    )
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';
END;


-- ==========================================================
-- 5. LOAD_SALES_TRANSACTIONS
--    Tables: SALES_HEADER, SALES_ITEM
--    All files live under: delta-load/sales-transaction/
-- ==========================================================
CREATE OR REPLACE TASK SALES_DEV.BRONZE.LOAD_SALES_TRANSACTIONS
  WAREHOUSE = COMPUTE_WH
  AFTER SALES_DEV.BRONZE.LOAD_BRONZE_DATA
AS
BEGIN
    COPY INTO SALES_DEV.BRONZE.SALES_HEADER
    FROM (
        SELECT
             $1,   -- TRANSACTION_ID
             $2,   -- TRANSACTION_NUMBER
             $3,   -- TRANSACTION_TIMESTAMP
             $4,   -- CUSTOMER_ID
             $5,   -- STORE_ID
             $6,   -- CHANNEL_ID
             $7,   -- PAYMENT_METHOD
             $8,   -- CURRENCY
             $9,   -- GROSS_AMOUNT
             $10,  -- TOTAL_DISCOUNT
             $11,  -- TOTAL_TAX
             $12,  -- NET_TOTAL
             $13,  -- CREATED_AT
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/sales-transaction/
    )
    PATTERN = '.*sales_header.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';

    COPY INTO SALES_DEV.BRONZE.SALES_ITEM
    FROM (
        SELECT
             $1,  -- TRANSACTION_LINE_ID
             $2,  -- TRANSACTION_ID
             $3,  -- SKU_CODE
             $4,  -- QUANTITY
             $5,  -- UNIT_PRICE
             $6,  -- DISCOUNT_AMOUNT
             $7,  -- TAX_AMOUNT
             $8,  -- LINE_TOTAL
             $9,  -- CREATED_AT
             METADATA$FILENAME,
             METADATA$FILE_ROW_NUMBER,
             CURRENT_TIMESTAMP()
        FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/delta-load/sales-transaction/
    )
    PATTERN = '.*sales_item.*[.]csv'
    FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
    ON_ERROR = 'ABORT_STATEMENT';
END;


-- ==========================================================
-- Resume tasks (children first, then root)
-- ==========================================================
ALTER TASK SALES_DEV.BRONZE.LOAD_COUNTRY_MASTERS RESUME;
ALTER TASK SALES_DEV.BRONZE.LOAD_PRODUCT_MASTERS RESUME;
ALTER TASK SALES_DEV.BRONZE.LOAD_STORE_CUSTOMER RESUME;
ALTER TASK SALES_DEV.BRONZE.LOAD_SALES_TRANSACTIONS RESUME;
ALTER TASK SALES_DEV.BRONZE.LOAD_BRONZE_DATA RESUME;


-- ==========================================================
-- Useful commands
-- ==========================================================
-- Execute the pipeline on demand:
-- EXECUTE TASK SALES_DEV.BRONZE.LOAD_BRONZE_DATA;

-- Check task run history:
-- SELECT name, state, scheduled_time, completed_time, error_message
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     TASK_NAME => 'LOAD_BRONZE_DATA',
--     SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
--   ))
--   ORDER BY scheduled_time DESC;

-- Check full task graph history:
-- SELECT name, state, scheduled_time, error_message
--   FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
--     ROOT_TASK_ID => (SELECT id FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))),
--     SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
--   ))
--   ORDER BY scheduled_time DESC;
