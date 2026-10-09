 ----------------------------------------------------------------------
-- 02_stage_data_loading.sql
-- Load country-master and product-master CSV files from STG_SALES_ANALYTICS
-- stage into SALES_DEV.BRONZE layer tables with audit metadata columns.
----------------------------------------------------------------------

USE DATABASE SALES_DEV;
USE SCHEMA BRONZE;
USE WAREHOUSE COMPUTE_WH;

----------------------------------------------------------------------
-- STEP 1: Create / update CSV file format in COMMON schema
----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT SALES_DEV.COMMON.CSV_FORMAT
    TYPE                      = 'CSV'
    FIELD_DELIMITER           = ','
    SKIP_HEADER               = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    TRIM_SPACE                = TRUE
    NULL_IF                   = ('', 'NULL', 'null')
    COMMENT                   = 'Standard CSV file format – comma delimited, first row skipped as header';

----------------------------------------------------------------------
-- STEP 2: Create REGION_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.REGION_MASTER (
    REGION_CODE          VARCHAR(10)      COMMENT 'Unique code identifying a sales region',
    REGION_NAME          VARCHAR(100)     COMMENT 'Descriptive name of the sales region',
    IS_ACTIVE            VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    EFFECTIVE_START_DATE DATE             COMMENT 'Date the region record becomes effective',
    EFFECTIVE_END_DATE   DATE             COMMENT 'Date the region record expires',
    CREATED_AT           TIMESTAMP_NTZ    COMMENT 'Timestamp when the record was created in the source system',
    SOURCE_SYSTEM        VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME          VARCHAR(1000)     COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER         NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS            TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw region master data sourced from country-master CSV files';

COPY INTO SALES_DEV.BRONZE.REGION_MASTER
FROM (
    SELECT
         $1,  -- REGION_CODE
         $2,  -- REGION_NAME
         $3,  -- IS_ACTIVE
         $4,  -- EFFECTIVE_START_DATE
         $5,  -- EFFECTIVE_END_DATE
         $6,  -- CREATED_AT
         $7,  -- SOURCE_SYSTEM
         METADATA$FILENAME,
         METADATA$FILE_ROW_NUMBER,
         CURRENT_TIMESTAMP()
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/country-master/region_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';


select * from SALES_DEV.BRONZE.REGION_MASTER;
----------------------------------------------------------------------
-- STEP 3: Create COUNTRY_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.COUNTRY_MASTER (
    COUNTRY_CODE           VARCHAR(10)    COMMENT 'ISO country code',
    COUNTRY_NAME           VARCHAR(100)   COMMENT 'Full country name',
    REGION_CODE            VARCHAR(10)    COMMENT 'Foreign key to REGION_MASTER',
    CURRENCY_CODE          VARCHAR(10)    COMMENT 'Foreign key to CURRENCY_MASTER',
    TAX_CODE               VARCHAR(20)    COMMENT 'Foreign key to TAX_MASTER',
    PRIMARY_LANGUAGE       VARCHAR(50)    COMMENT 'Primary spoken language in the country',
    TIMEZONE               VARCHAR(50)    COMMENT 'Default IANA timezone for the country',
    ECOMMERCE_SUPPORTED    VARCHAR(1)     COMMENT 'E-commerce availability flag (Y/N)',
    RETAIL_STORE_SUPPORTED VARCHAR(1)     COMMENT 'Physical retail store availability flag (Y/N)',
    MARKET_TIER            VARCHAR(20)    COMMENT 'Market classification tier (e.g. Tier1, Tier2)',
    __FILE_NAME            VARCHAR(500)   COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER           NUMBER(18,0)   COMMENT 'Row number within the source file',
    __LOAD_TS              TIMESTAMP_NTZ  COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw country master data with region, currency, and tax references';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/country-master/country_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';


select * from SALES_DEV.BRONZE.COUNTRY_MASTER;
----------------------------------------------------------------------
-- STEP 4: Create CURRENCY_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.CURRENCY_MASTER (
    CURRENCY_CODE        VARCHAR(10)      COMMENT 'ISO currency code (e.g. USD, GBP)',
    CURRENCY_NAME        VARCHAR(50)      COMMENT 'Full currency name',
    CURRENCY_SYMBOL      VARCHAR(5)       COMMENT 'Display symbol for the currency',
    MINOR_UNIT           NUMBER(1,0)      COMMENT 'Number of decimal places for the currency',
    IS_ACTIVE            VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    EFFECTIVE_START_DATE DATE             COMMENT 'Date the currency record becomes effective',
    EFFECTIVE_END_DATE   DATE             COMMENT 'Date the currency record expires',
    CREATED_AT           TIMESTAMP_NTZ    COMMENT 'Timestamp when the record was created in the source system',
    SOURCE_SYSTEM        VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME          VARCHAR(500)     COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER         NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS            TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw currency master data with symbol and minor unit details';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/country-master/currency_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';


select * from SALES_DEV.BRONZE.CURRENCY_MASTER;

----------------------------------------------------------------------
-- STEP 5: Create TAX_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.TAX_MASTER (
    TAX_CODE             VARCHAR(20)      COMMENT 'Unique tax rule code',
    TAX_TYPE             VARCHAR(20)      COMMENT 'Category of tax (e.g. VAT, GST, SALES_TAX)',
    TAX_RATE             NUMBER(5,4)      COMMENT 'Applicable tax rate as a decimal',
    TAX_INCLUSIVE_FLAG   VARCHAR(1)       COMMENT 'Whether prices include tax (Y/N)',
    EFFECTIVE_START_DATE DATE             COMMENT 'Date the tax rule becomes effective',
    EFFECTIVE_END_DATE   DATE             COMMENT 'Date the tax rule expires',
    IS_ACTIVE            VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    CREATED_AT           TIMESTAMP_NTZ    COMMENT 'Timestamp when the record was created in the source system',
    SOURCE_SYSTEM        VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME          VARCHAR(500)     COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER         NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS            TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw tax master data with rates and inclusivity rules';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/country-master/tax_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 6: Verify loaded row counts
----------------------------------------------------------------------
SELECT 'REGION_MASTER'   AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SALES_DEV.BRONZE.REGION_MASTER
UNION ALL
SELECT 'COUNTRY_MASTER',  COUNT(*) FROM SALES_DEV.BRONZE.COUNTRY_MASTER
UNION ALL
SELECT 'CURRENCY_MASTER', COUNT(*) FROM SALES_DEV.BRONZE.CURRENCY_MASTER
UNION ALL
SELECT 'TAX_MASTER',      COUNT(*) FROM SALES_DEV.BRONZE.TAX_MASTER;


-- =====================================================================
-- PRODUCT MASTER – 5 CSV files from initial-load/product-master/
-- =====================================================================

----------------------------------------------------------------------
-- STEP 7: Create PRODUCT_CATEGORY_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER (
    CATEGORY_CODE        VARCHAR(20)      COMMENT 'Unique product category identifier',
    CATEGORY_NAME        VARCHAR(100)     COMMENT 'Display name of the product category',
    REPORTING_SEGMENT    VARCHAR(50)      COMMENT 'Segment used for financial reporting (e.g. Hardware, Services)',
    IS_ACTIVE            VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    EFFECTIVE_START_DATE DATE             COMMENT 'Date the category becomes effective',
    EFFECTIVE_END_DATE   DATE             COMMENT 'Date the category expires',
    CREATED_AT           TIMESTAMP_NTZ    COMMENT 'Record creation timestamp in the source system',
    SOURCE_SYSTEM        VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME          VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER         NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS            TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw product category master with reporting segment classification';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/product-master/product_category_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 8: Create PRODUCT_FAMILY_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER (
    FAMILY_CODE      VARCHAR(20)      COMMENT 'Unique product family identifier',
    FAMILY_NAME      VARCHAR(100)     COMMENT 'Display name of the product family',
    CATEGORY_CODE    VARCHAR(20)      COMMENT 'Foreign key to PRODUCT_CATEGORY_MASTER',
    LAUNCH_YEAR      NUMBER(4,0)      COMMENT 'Calendar year the family was launched',
    IS_ACTIVE        VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    LIFECYCLE_STATUS VARCHAR(20)      COMMENT 'Current lifecycle stage (e.g. ACTIVE, DISCONTINUED)',
    CREATED_AT       TIMESTAMP_NTZ    COMMENT 'Record creation timestamp in the source system',
    SOURCE_SYSTEM    VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME      VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER     NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS        TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw product family master linking families to categories';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/product-master/product_family_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 9: Create PRODUCT_MODEL_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER (
    MODEL_CODE       VARCHAR(20)      COMMENT 'Unique product model identifier',
    MODEL_NAME       VARCHAR(100)     COMMENT 'Display name of the product model',
    FAMILY_CODE      VARCHAR(20)      COMMENT 'Foreign key to PRODUCT_FAMILY_MASTER',
    LAUNCH_DATE      DATE             COMMENT 'Date the model was launched globally',
    DISCONTINUE_DATE DATE             COMMENT 'Date the model was discontinued (NULL if still active)',
    LIFECYCLE_STATUS VARCHAR(20)      COMMENT 'Current lifecycle stage (e.g. ACTIVE, DISCONTINUED)',
    IS_ACTIVE        VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    CREATED_AT       TIMESTAMP_NTZ    COMMENT 'Record creation timestamp in the source system',
    SOURCE_SYSTEM    VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME      VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER     NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS        TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw product model master with launch and discontinuation dates';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/product-master/product_model_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 10: Create PRODUCT_SKU_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_SKU_MASTER (
    SKU_CODE           VARCHAR(50)      COMMENT 'Unique SKU identifier (model-capacity-color)',
    MODEL_CODE         VARCHAR(20)      COMMENT 'Foreign key to PRODUCT_MODEL_MASTER',
    VARIANT            VARCHAR(50)      COMMENT 'SKU variant descriptor (e.g. 128GB-Black)',
    PRICE_TIER         VARCHAR(20)      COMMENT 'Pricing tier classification (e.g. Standard, Premium)',
    GLOBAL_LAUNCH_DATE DATE             COMMENT 'Date the SKU was launched globally',
    IS_ACTIVE          VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    CREATED_AT         TIMESTAMP_NTZ    COMMENT 'Record creation timestamp in the source system',
    SOURCE_SYSTEM      VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME        VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER       NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS          TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw product SKU master with variant and pricing tier details';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/product-master/product_sku_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 11: Create PRODUCT_COUNTRY_AVAILABILITY table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY (
    SKU_CODE               VARCHAR(50)      COMMENT 'Foreign key to PRODUCT_SKU_MASTER',
    COUNTRY_CODE           VARCHAR(10)      COMMENT 'Foreign key to COUNTRY_MASTER',
    LOCAL_LAUNCH_DATE      DATE             COMMENT 'Date the SKU became available in this country',
    LOCAL_DISCONTINUE_DATE DATE             COMMENT 'Date the SKU was discontinued locally (NULL if still available)',
    IS_AVAILABLE           VARCHAR(1)       COMMENT 'Current availability flag (Y/N)',
    CREATED_AT             TIMESTAMP_NTZ    COMMENT 'Record creation timestamp in the source system',
    SOURCE_SYSTEM          VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME            VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER           NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS              TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw product-country availability mapping with local launch dates';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/product-master/product_country_availability.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 12: Verify product-master loaded row counts
----------------------------------------------------------------------
SELECT 'PRODUCT_CATEGORY_MASTER'      AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SALES_DEV.BRONZE.PRODUCT_CATEGORY_MASTER
UNION ALL
SELECT 'PRODUCT_FAMILY_MASTER',        COUNT(*) FROM SALES_DEV.BRONZE.PRODUCT_FAMILY_MASTER
UNION ALL
SELECT 'PRODUCT_MODEL_MASTER',         COUNT(*) FROM SALES_DEV.BRONZE.PRODUCT_MODEL_MASTER
UNION ALL
SELECT 'PRODUCT_SKU_MASTER',           COUNT(*) FROM SALES_DEV.BRONZE.PRODUCT_SKU_MASTER
UNION ALL
SELECT 'PRODUCT_COUNTRY_AVAILABILITY', COUNT(*) FROM SALES_DEV.BRONZE.PRODUCT_COUNTRY_AVAILABILITY;


-- =====================================================================
-- STORE MASTER – 1 CSV file from initial-load/store-master/
-- =====================================================================

----------------------------------------------------------------------
-- STEP 13: Create STORE_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.STORE_MASTER (
    STORE_CODE            VARCHAR(20)      COMMENT 'Unique store identifier',
    STORE_NAME            VARCHAR(200)     COMMENT 'Display name of the retail store',
    COUNTRY_CODE          VARCHAR(10)      COMMENT 'Foreign key to COUNTRY_MASTER',
    REGION_CODE           VARCHAR(10)      COMMENT 'Foreign key to REGION_MASTER',
    TAX_JURISDICTION_CODE VARCHAR(30)      COMMENT 'Tax jurisdiction applicable to the store',
    FORMAT_CODE           VARCHAR(20)      COMMENT 'Store format type (e.g. MALL, FLAGSHIP, OUTLET)',
    CITY                  VARCHAR(100)     COMMENT 'City where the store is located',
    STATE_CODE            VARCHAR(10)      COMMENT 'State or province code',
    POSTAL_CODE           VARCHAR(20)      COMMENT 'Postal or ZIP code',
    ADDRESS_LINE1         VARCHAR(300)     COMMENT 'Street address of the store',
    LATITUDE              NUMBER(10,6)     COMMENT 'Geographic latitude coordinate',
    LONGITUDE             NUMBER(10,6)     COMMENT 'Geographic longitude coordinate',
    STORE_OPEN_DATE       DATE             COMMENT 'Date the store opened for business',
    STORE_CLOSE_DATE      DATE             COMMENT 'Date the store closed (NULL if still open)',
    LIFECYCLE_STATUS      VARCHAR(20)      COMMENT 'Current status (e.g. ACTIVE, CLOSED, PLANNED)',
    FLOOR_AREA_SQFT       NUMBER(10,0)     COMMENT 'Total floor area in square feet',
    ANNUAL_RENT_USD       NUMBER(12,0)     COMMENT 'Annual rent cost in USD',
    IS_ACTIVE             VARCHAR(1)       COMMENT 'Active flag (Y/N)',
    EFFECTIVE_START_DATE  DATE             COMMENT 'Date the store record becomes effective',
    EFFECTIVE_END_DATE    DATE             COMMENT 'Date the store record expires',
    CREATED_AT            TIMESTAMP_NTZ    COMMENT 'Record creation timestamp in the source system',
    SOURCE_SYSTEM         VARCHAR(50)      COMMENT 'Originating source system identifier',
    __FILE_NAME           VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER          NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS             TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw store master with location, format, and lease details';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/store-master/store_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 14: Verify store-master loaded row count
----------------------------------------------------------------------
SELECT 'STORE_MASTER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SALES_DEV.BRONZE.STORE_MASTER;


-- =====================================================================
-- CUSTOMER MASTER – 1 CSV file from initial-load/customer-master/
-- =====================================================================

----------------------------------------------------------------------
-- STEP 15: Create CUSTOMER_MASTER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.CUSTOMER_MASTER (
    CUSTOMER_ID        VARCHAR(50)      COMMENT 'Unique customer UUID',
    CUSTOMER_NUMBER    VARCHAR(20)      COMMENT 'Business-facing customer number',
    FIRST_NAME         VARCHAR(100)     COMMENT 'Customer first name',
    LAST_NAME          VARCHAR(100)     COMMENT 'Customer last name',
    FULL_NAME          VARCHAR(200)     COMMENT 'Concatenated full name',
    GENDER             VARCHAR(10)      COMMENT 'Customer gender',
    DATE_OF_BIRTH      DATE             COMMENT 'Customer date of birth',
    EMAIL              VARCHAR(200)     COMMENT 'Primary email address',
    PHONE_NUMBER       VARCHAR(50)      COMMENT 'Primary phone number',
    STREET_ADDRESS     VARCHAR(300)     COMMENT 'Street address line',
    CITY               VARCHAR(100)     COMMENT 'City of residence',
    STATE_PROVINCE     VARCHAR(100)     COMMENT 'State or province name',
    POSTAL_CODE        VARCHAR(20)      COMMENT 'Postal or ZIP code',
    COUNTRY_CODE       VARCHAR(10)      COMMENT 'Foreign key to COUNTRY_MASTER',
    COUNTRY_NAME       VARCHAR(100)     COMMENT 'Full country name',
    REGION             VARCHAR(10)      COMMENT 'Foreign key to REGION_MASTER',
    PREFERRED_LANGUAGE VARCHAR(50)      COMMENT 'Customer preferred language',
    CUSTOMER_SEGMENT   VARCHAR(50)      COMMENT 'Segment classification (e.g. Consumer, Enterprise)',
    LOYALTY_TIER       VARCHAR(20)      COMMENT 'Loyalty programme tier (NULL if not enrolled)',
    REGISTRATION_DATE  DATE             COMMENT 'Date the customer registered',
    IS_ACTIVE          VARCHAR(5)       COMMENT 'Active flag (True/False)',
    SOURCE_SYSTEM      VARCHAR(50)      COMMENT 'Originating source system identifier',
    RECORD_SOURCE      VARCHAR(50)      COMMENT 'Channel through which the record was created (e.g. POS, WEB)',
    CREATED_AT         DATE             COMMENT 'Date the record was created in the source',
    UPDATED_AT         TIMESTAMP_NTZ    COMMENT 'Timestamp of last update in the source system',
    __FILE_NAME        VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER       NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS          TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw customer master with demographics, address, and loyalty details';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/customer-master/customer_master.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 16: Verify customer-master loaded row count
----------------------------------------------------------------------
SELECT 'CUSTOMER_MASTER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SALES_DEV.BRONZE.CUSTOMER_MASTER;


-- =====================================================================
-- SALES TRANSACTIONS – 2 CSV files from initial-load/sales-transaction/
-- =====================================================================

----------------------------------------------------------------------
-- STEP 17: Create SALES_HEADER table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.SALES_HEADER (
    TRANSACTION_ID        VARCHAR(50)      COMMENT 'Unique transaction UUID',
    TRANSACTION_NUMBER    VARCHAR(20)      COMMENT 'Business-facing transaction number',
    TRANSACTION_TIMESTAMP DATE             COMMENT 'Date the transaction occurred',
    CUSTOMER_ID           VARCHAR(50)      COMMENT 'Foreign key to CUSTOMER_MASTER',
    STORE_ID              VARCHAR(20)      COMMENT 'Foreign key to STORE_MASTER (store_code)',
    CHANNEL_ID            VARCHAR(20)      COMMENT 'Sales channel (e.g. POS, WEB, APP)',
    PAYMENT_METHOD        VARCHAR(50)      COMMENT 'Payment method used (e.g. Bank EMI, Visa, Amex)',
    CURRENCY              VARCHAR(10)      COMMENT 'Transaction currency code',
    GROSS_AMOUNT          NUMBER(12,2)     COMMENT 'Total amount before discount and tax',
    TOTAL_DISCOUNT        NUMBER(12,2)     COMMENT 'Total discount applied to the transaction',
    TOTAL_TAX             NUMBER(12,2)     COMMENT 'Total tax charged on the transaction',
    NET_TOTAL             NUMBER(12,2)     COMMENT 'Final amount after discount and tax',
    CREATED_AT            DATE             COMMENT 'Date the record was created in the source',
    __FILE_NAME           VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER          NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS             TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw sales transaction headers with totals and payment details';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/sales-transaction/sales_header.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 18: Create SALES_ITEM table and load data
----------------------------------------------------------------------
CREATE OR REPLACE TABLE SALES_DEV.BRONZE.SALES_ITEM (
    TRANSACTION_LINE_ID VARCHAR(50)      COMMENT 'Unique line item identifier',
    TRANSACTION_ID      VARCHAR(50)      COMMENT 'Foreign key to SALES_HEADER',
    SKU_CODE            VARCHAR(50)      COMMENT 'Foreign key to PRODUCT_SKU_MASTER',
    QUANTITY            NUMBER(10,0)     COMMENT 'Number of units purchased',
    UNIT_PRICE          NUMBER(12,2)     COMMENT 'Price per unit before discount',
    DISCOUNT_AMOUNT     NUMBER(12,2)     COMMENT 'Discount applied to this line item',
    TAX_AMOUNT          NUMBER(12,2)     COMMENT 'Tax charged on this line item',
    LINE_TOTAL          NUMBER(12,2)     COMMENT 'Final line amount after discount and tax',
    CREATED_AT          DATE             COMMENT 'Date the record was created in the source',
    __FILE_NAME         VARCHAR(1000)    COMMENT 'Stage file path from which the row was loaded',
    __ROW_NUMBER        NUMBER(18,0)     COMMENT 'Row number within the source file',
    __LOAD_TS           TIMESTAMP_NTZ    COMMENT 'Timestamp when the row was loaded into the bronze layer'
)
COMMENT = 'Bronze layer – raw sales line items with SKU, quantity, pricing, and tax';

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
    FROM @SALES_DEV.BRONZE.STG_SALES_ANALYTICS/initial-load/sales-transaction/sales_item.csv
)
FILE_FORMAT = (FORMAT_NAME = 'SALES_DEV.COMMON.CSV_FORMAT')
ON_ERROR = 'ABORT_STATEMENT';

----------------------------------------------------------------------
-- STEP 19: Verify sales-transaction loaded row counts
----------------------------------------------------------------------
SELECT 'SALES_HEADER' AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM SALES_DEV.BRONZE.SALES_HEADER
UNION ALL
SELECT 'SALES_ITEM',   COUNT(*) FROM SALES_DEV.BRONZE.SALES_ITEM