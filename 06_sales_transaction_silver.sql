-- Bronze-to-Silver: Sales Transaction Dynamic Tables – dedup via QUALIFY, normalise, DQ checks
-- Co-authored with CoCo
--
-- Transactional tables (86 107 rows each).  Same DT patterns as master tables:
--   • QUALIFY ROW_NUMBER() for dedup
--   • TARGET_LAG = DOWNSTREAM
--   • REFRESH_MODE = AUTO
--   • DQ_STATUS / DQ_FAIL_REASON on every record
--
-- Additional cross-field DQ: arithmetic consistency checks
--   SALES_HEADER:  gross - discount + tax ≈ net_total   (tolerance ±0.02)
--   SALES_ITEM:    qty * unit_price - discount + tax ≈ line_total


-- ====================================================================
-- 1. SALES HEADER
-- ====================================================================
-- Natural key: TRANSACTION_ID (UUID)
-- FKs: CUSTOMER_ID, STORE_ID, CURRENCY

ALTER TABLE SALES_DEV.BRONZE.SALES_HEADER
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.SALES_HEADER
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked sales transaction headers.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(TRANSACTION_ID)           AS TRANSACTION_ID,
    TRIM(TRANSACTION_NUMBER)       AS TRANSACTION_NUMBER,
    TRANSACTION_TIMESTAMP,
    TRIM(CUSTOMER_ID)              AS CUSTOMER_ID,
    TRIM(STORE_ID)                 AS STORE_ID,
    UPPER(TRIM(CHANNEL_ID))        AS CHANNEL_ID,
    TRIM(PAYMENT_METHOD)           AS PAYMENT_METHOD,
    UPPER(TRIM(CURRENCY))          AS CURRENCY,
    GROSS_AMOUNT,
    TOTAL_DISCOUNT,
    TOTAL_TAX,
    NET_TOTAL,
    CREATED_AT,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(TRANSACTION_ID)) = 0                                    THEN 'FAIL'
        WHEN TRANSACTION_NUMBER IS NULL OR LENGTH(TRIM(TRANSACTION_NUMBER)) = 0  THEN 'FAIL'
        WHEN TRANSACTION_TIMESTAMP IS NULL                                        THEN 'FAIL'
        WHEN CUSTOMER_ID IS NULL OR LENGTH(TRIM(CUSTOMER_ID)) = 0               THEN 'FAIL'
        WHEN STORE_ID IS NULL OR LENGTH(TRIM(STORE_ID)) = 0                     THEN 'FAIL'
        WHEN CHANNEL_ID IS NULL OR LENGTH(TRIM(CHANNEL_ID)) = 0                 THEN 'FAIL'
        WHEN CURRENCY IS NULL OR LENGTH(TRIM(CURRENCY)) = 0                     THEN 'FAIL'
        WHEN GROSS_AMOUNT IS NULL OR GROSS_AMOUNT < 0                            THEN 'FAIL'
        WHEN NET_TOTAL IS NULL OR NET_TOTAL < 0                                  THEN 'FAIL'
        WHEN TOTAL_DISCOUNT IS NOT NULL AND TOTAL_DISCOUNT < 0                   THEN 'FAIL'
        WHEN TOTAL_TAX IS NOT NULL AND TOTAL_TAX < 0                             THEN 'FAIL'
        WHEN ABS(GROSS_AMOUNT - TOTAL_DISCOUNT + TOTAL_TAX - NET_TOTAL) > 0.02  THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(TRANSACTION_ID)) = 0
            THEN 'TRANSACTION_ID is blank'
        WHEN TRANSACTION_NUMBER IS NULL OR LENGTH(TRIM(TRANSACTION_NUMBER)) = 0
            THEN 'TRANSACTION_NUMBER is null or blank'
        WHEN TRANSACTION_TIMESTAMP IS NULL
            THEN 'TRANSACTION_TIMESTAMP is null'
        WHEN CUSTOMER_ID IS NULL OR LENGTH(TRIM(CUSTOMER_ID)) = 0
            THEN 'CUSTOMER_ID (FK) is null or blank'
        WHEN STORE_ID IS NULL OR LENGTH(TRIM(STORE_ID)) = 0
            THEN 'STORE_ID (FK) is null or blank'
        WHEN CHANNEL_ID IS NULL OR LENGTH(TRIM(CHANNEL_ID)) = 0
            THEN 'CHANNEL_ID is null or blank'
        WHEN CURRENCY IS NULL OR LENGTH(TRIM(CURRENCY)) = 0
            THEN 'CURRENCY (FK) is null or blank'
        WHEN GROSS_AMOUNT IS NULL OR GROSS_AMOUNT < 0
            THEN 'GROSS_AMOUNT is null or negative'
        WHEN NET_TOTAL IS NULL OR NET_TOTAL < 0
            THEN 'NET_TOTAL is null or negative'
        WHEN TOTAL_DISCOUNT IS NOT NULL AND TOTAL_DISCOUNT < 0
            THEN 'TOTAL_DISCOUNT is negative'
        WHEN TOTAL_TAX IS NOT NULL AND TOTAL_TAX < 0
            THEN 'TOTAL_TAX is negative'
        WHEN ABS(GROSS_AMOUNT - TOTAL_DISCOUNT + TOTAL_TAX - NET_TOTAL) > 0.02
            THEN 'Amount mismatch: gross(' || GROSS_AMOUNT::VARCHAR
                 || ') - disc(' || TOTAL_DISCOUNT::VARCHAR
                 || ') + tax(' || TOTAL_TAX::VARCHAR
                 || ') != net(' || NET_TOTAL::VARCHAR || ')'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.SALES_HEADER
WHERE TRANSACTION_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRANSACTION_ID
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT COUNT(*) AS ROW_COUNT, COUNT(CASE WHEN DQ_STATUS = 'FAIL' THEN 1 END) AS FAILED_ROWS
FROM SALES_DEV.SILVER.SALES_HEADER;


-- ====================================================================
-- 2. SALES ITEM (line items)
-- ====================================================================
-- Natural key: TRANSACTION_LINE_ID (UUID)
-- FKs: TRANSACTION_ID, SKU_CODE

ALTER TABLE SALES_DEV.BRONZE.SALES_ITEM
    SET CHANGE_TRACKING = TRUE;

CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.SILVER.SALES_ITEM
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = COMPUTE_WH
    REFRESH_MODE = AUTO
    INITIALIZE   = ON_CREATE
    COMMENT = 'Silver – deduplicated, normalised, DQ-checked sales line items.'
AS
SELECT
    -- ── Business columns ─────────────────────────────────────────
    TRIM(TRANSACTION_LINE_ID)      AS TRANSACTION_LINE_ID,
    TRIM(TRANSACTION_ID)           AS TRANSACTION_ID,
    TRIM(SKU_CODE)                 AS SKU_CODE,
    QUANTITY,
    UNIT_PRICE,
    DISCOUNT_AMOUNT,
    TAX_AMOUNT,
    LINE_TOTAL,
    CREATED_AT,

    -- ── Data Quality ─────────────────────────────────────────────
    CASE
        WHEN LENGTH(TRIM(TRANSACTION_LINE_ID)) = 0                               THEN 'FAIL'
        WHEN TRANSACTION_ID IS NULL OR LENGTH(TRIM(TRANSACTION_ID)) = 0          THEN 'FAIL'
        WHEN SKU_CODE IS NULL OR LENGTH(TRIM(SKU_CODE)) = 0                      THEN 'FAIL'
        WHEN QUANTITY IS NULL OR QUANTITY <= 0                                     THEN 'FAIL'
        WHEN UNIT_PRICE IS NULL OR UNIT_PRICE < 0                                THEN 'FAIL'
        WHEN DISCOUNT_AMOUNT IS NOT NULL AND DISCOUNT_AMOUNT < 0                 THEN 'FAIL'
        WHEN TAX_AMOUNT IS NOT NULL AND TAX_AMOUNT < 0                           THEN 'FAIL'
        WHEN LINE_TOTAL IS NULL OR LINE_TOTAL < 0                                THEN 'FAIL'
        WHEN ABS((QUANTITY * UNIT_PRICE)
                 - COALESCE(DISCOUNT_AMOUNT, 0)
                 + COALESCE(TAX_AMOUNT, 0)
                 - LINE_TOTAL) > 0.02                                            THEN 'FAIL'
        ELSE 'PASS'
    END                            AS DQ_STATUS,

    CASE
        WHEN LENGTH(TRIM(TRANSACTION_LINE_ID)) = 0
            THEN 'TRANSACTION_LINE_ID is blank'
        WHEN TRANSACTION_ID IS NULL OR LENGTH(TRIM(TRANSACTION_ID)) = 0
            THEN 'TRANSACTION_ID (FK) is null or blank'
        WHEN SKU_CODE IS NULL OR LENGTH(TRIM(SKU_CODE)) = 0
            THEN 'SKU_CODE (FK) is null or blank'
        WHEN QUANTITY IS NULL OR QUANTITY <= 0
            THEN 'QUANTITY is null or non-positive: ' || COALESCE(QUANTITY::VARCHAR, 'NULL')
        WHEN UNIT_PRICE IS NULL OR UNIT_PRICE < 0
            THEN 'UNIT_PRICE is null or negative: ' || COALESCE(UNIT_PRICE::VARCHAR, 'NULL')
        WHEN DISCOUNT_AMOUNT IS NOT NULL AND DISCOUNT_AMOUNT < 0
            THEN 'DISCOUNT_AMOUNT is negative: ' || DISCOUNT_AMOUNT::VARCHAR
        WHEN TAX_AMOUNT IS NOT NULL AND TAX_AMOUNT < 0
            THEN 'TAX_AMOUNT is negative: ' || TAX_AMOUNT::VARCHAR
        WHEN LINE_TOTAL IS NULL OR LINE_TOTAL < 0
            THEN 'LINE_TOTAL is null or negative'
        WHEN ABS((QUANTITY * UNIT_PRICE)
                 - COALESCE(DISCOUNT_AMOUNT, 0)
                 + COALESCE(TAX_AMOUNT, 0)
                 - LINE_TOTAL) > 0.02
            THEN 'Line total mismatch: qty(' || QUANTITY::VARCHAR
                 || ') * price(' || UNIT_PRICE::VARCHAR
                 || ') - disc(' || COALESCE(DISCOUNT_AMOUNT, 0)::VARCHAR
                 || ') + tax(' || COALESCE(TAX_AMOUNT, 0)::VARCHAR
                 || ') != line_total(' || LINE_TOTAL::VARCHAR || ')'
        ELSE NULL
    END                            AS DQ_FAIL_REASON,

    -- ── Audit / lineage ──────────────────────────────────────────
    __FILE_NAME                    AS BRONZE_SOURCE_FILE,
    __LOAD_TS                      AS BRONZE_LOAD_TS,
    CURRENT_TIMESTAMP()            AS SILVER_LOAD_TS

FROM SALES_DEV.BRONZE.SALES_ITEM
WHERE TRANSACTION_LINE_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY TRANSACTION_LINE_ID
    ORDER BY __LOAD_TS DESC, __ROW_NUMBER ASC
) = 1;

SELECT COUNT(*) AS ROW_COUNT, COUNT(CASE WHEN DQ_STATUS = 'FAIL' THEN 1 END) AS FAILED_ROWS
FROM SALES_DEV.SILVER.SALES_ITEM;
