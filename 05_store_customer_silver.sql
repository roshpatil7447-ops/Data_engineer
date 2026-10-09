-- Bronze-to-Silver: Store & Customer Master Dynamic Tables – dedup via QUALIFY, normalise, DQ checks
-- Co-authored with CoCo
--
-- Same patterns as previous silver DTs:
--   • QUALIFY ROW_NUMBER() for dedup (no CTEs)
--   • TARGET_LAG = DOWNSTREAM
--   • REFRESH_MODE = AUTO
--   • DQ_STATUS / DQ_FAIL_REASON columns on every record


-- ====================================================================
-- 1. STORE MASTER
-- ====================================================================
-- Natural key: STORE_CODE   |   FKs: COUNTRY_CODE, REGION_CODE   |   80 rows
-- Domain checks: geo coordinates, floor area > 0, rent >= 0,
--                close_date >= open_date, effective date range

ALTER TABLE SALES_DEV.BRONZE.STORE_MASTER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.STORE_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked store master.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(STORE_CODE)               AS STORE_CODE,
    TRIM(STORE_NAME)               AS STORE_NAME,
    TRIM(COUNTRY_CODE)             AS COUNTRY_CODE,
    TRIM(REGION_CODE)              AS REGION_CODE,
    TRIM(TAX_JURISDICTION_CODE)    AS TAX_JURISDICTION_CODE,
    UPPER(TRIM(FORMAT_CODE))       AS FORMAT_CODE,
    TRIM(CITY)                     AS CITY,
    TRIM(STATE_CODE)               AS STATE_CODE,
    TRIM(POSTAL_CODE)              AS POSTAL_CODE,
    TRIM(ADDRESS_LINE1)            AS ADDRESS_LINE1,
    LATITUDE,
    LONGITUDE,
    STORE_OPEN_DATE,
    STORE_CLOSE_DATE,
    UPPER(TRIM(LIFECYCLE_STATUS))  AS LIFECYCLE_STATUS,
    FLOOR_AREA_SQFT,
    ANNUAL_RENT_USD,
    UPPER(TRIM(IS_ACTIVE))         AS IS_ACTIVE,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    CREATED_AT,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(STORE_CODE)) = 0                                        THEN 'FAIL'
        WHEN STORE_NAME IS NULL OR LENGTH(TRIM(STORE_NAME)) = 0                  THEN 'FAIL'
        WHEN COUNTRY_CODE IS NULL OR LENGTH(TRIM(COUNTRY_CODE)) = 0              THEN 'FAIL'
        WHEN REGION_CODE IS NULL OR LENGTH(TRIM(REGION_CODE)) = 0                THEN 'FAIL'
        WHEN FORMAT_CODE IS NULL OR LENGTH(TRIM(FORMAT_CODE)) = 0                THEN 'FAIL'
        WHEN STORE_OPEN_DATE IS NULL                                              THEN 'FAIL'
        WHEN STORE_CLOSE_DATE IS NOT NULL
             AND STORE_CLOSE_DATE < STORE_OPEN_DATE                              THEN 'FAIL'
        WHEN LATITUDE IS NOT NULL AND (LATITUDE < -90 OR LATITUDE > 90)          THEN 'FAIL'
        WHEN LONGITUDE IS NOT NULL AND (LONGITUDE < -180 OR LONGITUDE > 180)     THEN 'FAIL'
        WHEN FLOOR_AREA_SQFT IS NOT NULL AND FLOOR_AREA_SQFT <= 0               THEN 'FAIL'
        WHEN ANNUAL_RENT_USD IS NOT NULL AND ANNUAL_RENT_USD < 0                 THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('Y', 'N')                           THEN 'FAIL'
        WHEN EFFECTIVE_START_DATE IS NULL                                         THEN 'FAIL'
        WHEN EFFECTIVE_END_DATE IS NOT NULL
             AND EFFECTIVE_END_DATE < EFFECTIVE_START_DATE                        THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(STORE_CODE)) = 0
            THEN 'STORE_CODE is blank'
        WHEN STORE_NAME IS NULL OR LENGTH(TRIM(STORE_NAME)) = 0
            THEN 'STORE_NAME is null or blank'
        WHEN COUNTRY_CODE IS NULL OR LENGTH(TRIM(COUNTRY_CODE)) = 0
            THEN 'COUNTRY_CODE (FK) is null or blank'
        WHEN REGION_CODE IS NULL OR LENGTH(TRIM(REGION_CODE)) = 0
            THEN 'REGION_CODE (FK) is null or blank'
        WHEN FORMAT_CODE IS NULL OR LENGTH(TRIM(FORMAT_CODE)) = 0
            THEN 'FORMAT_CODE is null or blank'
        WHEN STORE_OPEN_DATE IS NULL
            THEN 'STORE_OPEN_DATE is null'
        WHEN STORE_CLOSE_DATE IS NOT NULL
             AND STORE_CLOSE_DATE < STORE_OPEN_DATE
            THEN 'STORE_CLOSE_DATE (' || STORE_CLOSE_DATE::VARCHAR
                 || ') < STORE_OPEN_DATE (' || STORE_OPEN_DATE::VARCHAR || ')'
        WHEN LATITUDE IS NOT NULL AND (LATITUDE < -90 OR LATITUDE > 90)
            THEN 'LATITUDE out of range (-90 to 90): ' || LATITUDE::VARCHAR
        WHEN LONGITUDE IS NOT NULL AND (LONGITUDE < -180 OR LONGITUDE > 180)
            THEN 'LONGITUDE out of range (-180 to 180): ' || LONGITUDE::VARCHAR
        WHEN FLOOR_AREA_SQFT IS NOT NULL AND FLOOR_AREA_SQFT <= 0
            THEN 'FLOOR_AREA_SQFT must be positive: ' || FLOOR_AREA_SQFT::VARCHAR
        WHEN ANNUAL_RENT_USD IS NOT NULL AND ANNUAL_RENT_USD < 0
            THEN 'ANNUAL_RENT_USD is negative: ' || ANNUAL_RENT_USD::VARCHAR
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

FROM SALES_DEV.BRONZE.STORE_MASTER
WHERE STORE_CODE IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY STORE_CODE
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT * FROM SALES_DEV.SILVER.STORE_MASTER ORDER BY STORE_CODE;


-- ====================================================================
-- 2. CUSTOMER MASTER
-- ====================================================================
-- Natural key: CUSTOMER_ID (UUID)   |   FKs: COUNTRY_CODE, REGION   |   50 000 rows
-- Domain checks: email format, IS_ACTIVE normalised from True/False → Y/N,
--                registration_date not null, customer_segment populated
-- Note: IS_ACTIVE is stored as 'True'/'False' in bronze (VARCHAR(5)),
--       normalised to 'Y'/'N' in silver for consistency with other masters.

ALTER TABLE SALES_DEV.BRONZE.CUSTOMER_MASTER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.CUSTOMER_MASTER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked customer master.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(CUSTOMER_ID)              AS CUSTOMER_ID,
    TRIM(CUSTOMER_NUMBER)          AS CUSTOMER_NUMBER,
    TRIM(FIRST_NAME)               AS FIRST_NAME,
    TRIM(LAST_NAME)                AS LAST_NAME,
    TRIM(FULL_NAME)                AS FULL_NAME,
    UPPER(TRIM(GENDER))            AS GENDER,
    DATE_OF_BIRTH,
    LOWER(TRIM(EMAIL))             AS EMAIL,
    TRIM(PHONE_NUMBER)             AS PHONE_NUMBER,
    TRIM(STREET_ADDRESS)           AS STREET_ADDRESS,
    TRIM(CITY)                     AS CITY,
    TRIM(STATE_PROVINCE)           AS STATE_PROVINCE,
    TRIM(POSTAL_CODE)              AS POSTAL_CODE,
    TRIM(COUNTRY_CODE)             AS COUNTRY_CODE,
    TRIM(COUNTRY_NAME)             AS COUNTRY_NAME,
    TRIM(REGION)                   AS REGION,
    TRIM(PREFERRED_LANGUAGE)       AS PREFERRED_LANGUAGE,
    UPPER(TRIM(CUSTOMER_SEGMENT))  AS CUSTOMER_SEGMENT,
    UPPER(TRIM(LOYALTY_TIER))      AS LOYALTY_TIER,
    REGISTRATION_DATE,
    -- Normalise True/False → Y/N for consistency with other masters
    CASE UPPER(TRIM(IS_ACTIVE))
        WHEN 'TRUE'  THEN 'Y'
        WHEN 'FALSE' THEN 'N'
        ELSE UPPER(TRIM(IS_ACTIVE))
    END                            AS IS_ACTIVE,
    TRIM(SOURCE_SYSTEM)            AS SOURCE_SYSTEM,
    UPPER(TRIM(RECORD_SOURCE))     AS RECORD_SOURCE,
    CREATED_AT,
    UPDATED_AT,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(CUSTOMER_ID)) = 0                                       THEN 'FAIL'
        WHEN CUSTOMER_NUMBER IS NULL OR LENGTH(TRIM(CUSTOMER_NUMBER)) = 0        THEN 'FAIL'
        WHEN FULL_NAME IS NULL OR LENGTH(TRIM(FULL_NAME)) = 0                    THEN 'FAIL'
        WHEN EMAIL IS NULL OR LENGTH(TRIM(EMAIL)) = 0                            THEN 'FAIL'
        WHEN EMAIL IS NOT NULL AND EMAIL NOT LIKE '%@%.%'                        THEN 'FAIL'
        WHEN COUNTRY_CODE IS NULL OR LENGTH(TRIM(COUNTRY_CODE)) = 0              THEN 'FAIL'
        WHEN REGISTRATION_DATE IS NULL                                            THEN 'FAIL'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('TRUE', 'FALSE')                    THEN 'FAIL'
        WHEN CUSTOMER_SEGMENT IS NULL OR LENGTH(TRIM(CUSTOMER_SEGMENT)) = 0     THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(CUSTOMER_ID)) = 0
            THEN 'CUSTOMER_ID is blank'
        WHEN CUSTOMER_NUMBER IS NULL OR LENGTH(TRIM(CUSTOMER_NUMBER)) = 0
            THEN 'CUSTOMER_NUMBER is null or blank'
        WHEN FULL_NAME IS NULL OR LENGTH(TRIM(FULL_NAME)) = 0
            THEN 'FULL_NAME is null or blank'
        WHEN EMAIL IS NULL OR LENGTH(TRIM(EMAIL)) = 0
            THEN 'EMAIL is null or blank'
        WHEN EMAIL IS NOT NULL AND EMAIL NOT LIKE '%@%.%'
            THEN 'EMAIL format invalid: ' || EMAIL
        WHEN COUNTRY_CODE IS NULL OR LENGTH(TRIM(COUNTRY_CODE)) = 0
            THEN 'COUNTRY_CODE (FK) is null or blank'
        WHEN REGISTRATION_DATE IS NULL
            THEN 'REGISTRATION_DATE is null'
        WHEN UPPER(TRIM(IS_ACTIVE)) NOT IN ('TRUE', 'FALSE')
            THEN 'IS_ACTIVE invalid – expected True/False, got: '
                 || COALESCE(IS_ACTIVE, 'NULL')
        WHEN CUSTOMER_SEGMENT IS NULL OR LENGTH(TRIM(CUSTOMER_SEGMENT)) = 0
            THEN 'CUSTOMER_SEGMENT is null or blank'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.CUSTOMER_MASTER
WHERE CUSTOMER_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CUSTOMER_ID
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT COUNT(*) AS ROW_COUNT, COUNT(CASE WHEN DQ_STATUS = 'FAIL' THEN 1 END) AS FAILED_ROWS
FROM SALES_DEV.SILVER.CUSTOMER_MASTER;
