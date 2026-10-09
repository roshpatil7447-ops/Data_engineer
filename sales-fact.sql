-- =====================================================
-- Gold Layer - DIM_DATE + FACT_SALES
--
-- DIM_DATE   : Dynamic calendar; date range = MIN/MAX transaction
--              date in SILVER.SALES_HEADER (grows automatically)
-- FACT_SALES : Line-item grain (SALES_ITEM x SALES_HEADER)
--              joined to GOLD DIM_DATE, DIM_CUSTOMER, DIM_PRODUCT,
--              DIM_STORE and DIM_COUNTRY via hash-based foreign keys
--
-- Pipeline (fully dynamic, all INCREMENTAL, all DOWNSTREAM):
--   BRONZE -> SILVER DTs -> GOLD DIM DTs -> GOLD FACT_SALES DT
-- NOTE: With every DT on DOWNSTREAM, nothing refreshes on a schedule.
--       Refresh manually (ALTER DYNAMIC TABLE ... REFRESH) or add a
--       downstream consumer DT with a time-based lag.
-- =====================================================


-- ==========================================================
-- 1. DIM_DATE - dynamic calendar driven by sales date range
--    (ARRAY_GENERATE_RANGE + FLATTEN keeps it deterministic so
--     incremental refresh works; SEQ4/GENERATOR would force FULL)
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Date dimension with calendar attributes. Date range is derived dynamically from MIN/MAX transaction dates in SILVER.SALES_HEADER. Use DATE_DIM_KEY (hash) for fact table joins.'
AS
WITH bounds AS (
    SELECT MIN(TRANSACTION_TIMESTAMP) AS MIN_DT,
           MAX(TRANSACTION_TIMESTAMP) AS MAX_DT
    FROM SALES_DEV.SILVER.SALES_HEADER
    WHERE DQ_STATUS = 'PASS'
),
date_spine AS (
    SELECT DATEADD(DAY, f.VALUE::INT, b.MIN_DT)::DATE AS CAL_DATE
    FROM bounds b,
         LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, DATEDIFF(DAY, b.MIN_DT, b.MAX_DT) + 1)) f
)
SELECT
    SHA2(TO_VARCHAR(CAL_DATE, 'YYYY-MM-DD'), 256)      AS DATE_DIM_KEY,
    TO_NUMBER(TO_VARCHAR(CAL_DATE, 'YYYYMMDD'))       AS DATE_KEY,
    CAL_DATE                                           AS CAL_DATE,

    -- Day attributes
    DAY(CAL_DATE)                                      AS DAY_OF_MONTH,
    DAYOFWEEK(CAL_DATE)                                AS DAY_OF_WEEK_NUM,
    DAYNAME(CAL_DATE)                                  AS DAY_NAME,
    DAYOFYEAR(CAL_DATE)                                AS DAY_OF_YEAR,
    CASE WHEN DAYOFWEEK(CAL_DATE) IN (0, 6) THEN TRUE ELSE FALSE END AS IS_WEEKEND,

    -- Week attributes
    WEEKOFYEAR(CAL_DATE)                               AS WEEK_OF_YEAR,
    DATE_TRUNC('WEEK', CAL_DATE)                       AS WEEK_START_DATE,

    -- Month attributes
    MONTH(CAL_DATE)                                    AS MONTH_NUM,
    MONTHNAME(CAL_DATE)                                AS MONTH_NAME,
    DATE_TRUNC('MONTH', CAL_DATE)                      AS MONTH_START_DATE,
    LAST_DAY(CAL_DATE, 'MONTH')                        AS MONTH_END_DATE,

    -- Quarter attributes
    QUARTER(CAL_DATE)                                  AS QUARTER_NUM,
    'Q' || QUARTER(CAL_DATE)                           AS QUARTER_NAME,
    DATE_TRUNC('QUARTER', CAL_DATE)                    AS QUARTER_START_DATE,

    -- Year attributes
    YEAR(CAL_DATE)                                     AS YEAR_NUM,
    DATE_TRUNC('YEAR', CAL_DATE)                       AS YEAR_START_DATE,

    -- Fiscal year (Apr-Mar)
    CASE WHEN MONTH(CAL_DATE) >= 4 THEN YEAR(CAL_DATE) ELSE YEAR(CAL_DATE) - 1 END AS FISCAL_YEAR,
    CASE WHEN MONTH(CAL_DATE) >= 4 THEN CEIL((MONTH(CAL_DATE) - 3) / 3.0)
         ELSE CEIL((MONTH(CAL_DATE) + 9) / 3.0) END    AS FISCAL_QUARTER_NUM,

    TO_VARCHAR(CAL_DATE, 'YYYY-MM')                    AS YEAR_MONTH_LABEL
FROM date_spine;


ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN DATE_DIM_KEY COMMENT 'Hash-based surrogate key (SHA-256 of YYYY-MM-DD) for fact table joins';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN DATE_KEY COMMENT 'Readable integer date (YYYYMMDD) - use DATE_DIM_KEY for joins';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN CAL_DATE COMMENT 'Calendar date value';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN DAY_OF_MONTH COMMENT 'Day number within month (1-31)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN DAY_OF_WEEK_NUM COMMENT 'Day of week number (0=Sun, 6=Sat)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN DAY_NAME COMMENT 'Day name abbreviation (Mon, Tue, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN DAY_OF_YEAR COMMENT 'Day number within year (1-366)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN IS_WEEKEND COMMENT 'TRUE if Saturday or Sunday';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN WEEK_OF_YEAR COMMENT 'Week number within year';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN WEEK_START_DATE COMMENT 'Start date of the week';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN MONTH_NUM COMMENT 'Month number (1-12)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN MONTH_NAME COMMENT 'Month name abbreviation (Jan, Feb, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN MONTH_START_DATE COMMENT 'First day of the month';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN MONTH_END_DATE COMMENT 'Last day of the month';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN QUARTER_NUM COMMENT 'Calendar quarter number (1-4)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN QUARTER_NAME COMMENT 'Quarter label (Q1-Q4)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN QUARTER_START_DATE COMMENT 'First day of the calendar quarter';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN YEAR_NUM COMMENT 'Calendar year number';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN YEAR_START_DATE COMMENT 'First day of the calendar year';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN FISCAL_YEAR COMMENT 'Fiscal year (Apr-Mar)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN FISCAL_QUARTER_NUM COMMENT 'Fiscal quarter number (1-4, starting Apr)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.DIM_DATE ALTER COLUMN YEAR_MONTH_LABEL COMMENT 'Year-month label (YYYY-MM) for reporting';


-- ==========================================================
-- 2. FACT_SALES - line-item grain; all dimension keys come
--    from GOLD dimension tables
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Sales fact table at line-item grain. Each row is one product sold in a transaction. Joins to DIM_DATE, DIM_CUSTOMER, DIM_STORE, DIM_PRODUCT and DIM_COUNTRY via hash-based dimension keys.'
AS
SELECT
    -- Fact surrogate key
    SHA2(CONCAT(
        COALESCE(i.TRANSACTION_LINE_ID, ''),
        COALESCE(TO_VARCHAR(i.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                                             AS SALES_FACT_KEY,

    -- Dimension keys (Hash-based foreign keys)
    dd.DATE_DIM_KEY                                     AS DATE_DIM_KEY,
    dc.CUSTOMER_DIM_KEY                                 AS CUSTOMER_DIM_KEY,
    dp.PRODUCT_DIM_KEY                                  AS PRODUCT_DIM_KEY,
    ds.STORE_DIM_KEY                                    AS STORE_DIM_KEY,
    dcn.COUNTRY_DIM_KEY                                 AS COUNTRY_DIM_KEY,

    -- Degenerate dimensions
    h.TRANSACTION_ID                                    AS TRANSACTION_ID,
    h.TRANSACTION_NUMBER                                AS TRANSACTION_NUMBER,
    i.TRANSACTION_LINE_ID                               AS TRANSACTION_LINE_ID,

    -- Business keys (convenience)
    h.CUSTOMER_ID                                       AS CUSTOMER_ID,
    i.SKU_CODE                                          AS SKU_CODE,
    h.STORE_ID                                          AS STORE_CODE,
    ds.COUNTRY_CODE                                     AS COUNTRY_CODE,
    dd.CAL_DATE                                         AS TRANSACTION_DATE,

    -- Transaction attributes
    h.CHANNEL_ID                                        AS CHANNEL_ID,
    h.PAYMENT_METHOD                                    AS PAYMENT_METHOD,
    h.CURRENCY                                          AS CURRENCY_CODE,

    -- Measures
    i.QUANTITY                                          AS QUANTITY,
    i.UNIT_PRICE                                        AS UNIT_PRICE,
    (i.QUANTITY * i.UNIT_PRICE)                         AS GROSS_AMOUNT,
    i.DISCOUNT_AMOUNT                                   AS DISCOUNT_AMOUNT,
    i.TAX_AMOUNT                                        AS TAX_AMOUNT,
    i.LINE_TOTAL                                        AS LINE_TOTAL,

    -- Audit
    i.BRONZE_LOAD_TS                                    AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.SALES_ITEM i
INNER JOIN SALES_DEV.SILVER.SALES_HEADER h
    ON i.TRANSACTION_ID = h.TRANSACTION_ID AND h.DQ_STATUS = 'PASS'
LEFT JOIN SALES_DEV.GOLD.DIM_DATE dd
    ON h.TRANSACTION_TIMESTAMP = dd.CAL_DATE
LEFT JOIN SALES_DEV.GOLD.DIM_CUSTOMER dc
    ON h.CUSTOMER_ID = dc.CUSTOMER_ID AND dc.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_PRODUCT dp
    ON i.SKU_CODE = dp.SKU_CODE AND dp.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_STORE ds
    ON h.STORE_ID = ds.STORE_CODE AND ds.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_COUNTRY dcn
    ON ds.COUNTRY_CODE = dcn.COUNTRY_CODE AND dcn.IS_CURRENT = TRUE
WHERE i.DQ_STATUS = 'PASS';


ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN SALES_FACT_KEY COMMENT 'Hash-based surrogate key for the sales line item';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN DATE_DIM_KEY COMMENT 'Hash FK to DIM_DATE.DATE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN CUSTOMER_DIM_KEY COMMENT 'Hash FK to DIM_CUSTOMER.CUSTOMER_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN PRODUCT_DIM_KEY COMMENT 'Hash FK to DIM_PRODUCT.PRODUCT_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN STORE_DIM_KEY COMMENT 'Hash FK to DIM_STORE.STORE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN COUNTRY_DIM_KEY COMMENT 'Hash FK to DIM_COUNTRY.COUNTRY_DIM_KEY (derived from store location)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN TRANSACTION_ID COMMENT 'Degenerate dimension - sales transaction identifier';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN TRANSACTION_NUMBER COMMENT 'Degenerate dimension - customer-facing receipt number';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN TRANSACTION_LINE_ID COMMENT 'Degenerate dimension - line item identifier (grain)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN CUSTOMER_ID COMMENT 'Customer business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN SKU_CODE COMMENT 'Product SKU business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN STORE_CODE COMMENT 'Store business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN COUNTRY_CODE COMMENT 'Country business key of the selling store';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN TRANSACTION_DATE COMMENT 'Date of the sales transaction';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN CHANNEL_ID COMMENT 'Sales channel (STORE, ONLINE, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN PAYMENT_METHOD COMMENT 'Payment method used for the transaction';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN CURRENCY_CODE COMMENT 'Transaction currency code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN QUANTITY COMMENT 'Units sold on this line (additive measure)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN UNIT_PRICE COMMENT 'Price per unit (non-additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN GROSS_AMOUNT COMMENT 'Quantity x unit price before discount and tax (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN DISCOUNT_AMOUNT COMMENT 'Discount applied to the line (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN TAX_AMOUNT COMMENT 'Tax charged on the line (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES ALTER COLUMN LINE_TOTAL COMMENT 'Net line amount = gross - discount + tax (additive)';


-- ==========================================================
-- 3. FACT_SALES_HEADER - transaction (receipt) grain
--    Basket-level KPIs: transaction count, AOV, channel/payment mix.
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Sales header fact at transaction grain (one row per receipt). Use for basket-level KPIs: transaction count, average order value, payment/channel mix, header totals. Joins to dimensions via hash keys; joins to FACT_SALES_ITEM on TRANSACTION_FACT_KEY.'
AS
WITH item_agg AS (
    SELECT TRANSACTION_ID,
           COUNT(*)       AS LINE_COUNT,
           SUM(QUANTITY)  AS TOTAL_QUANTITY,
           SUM(LINE_TOTAL) AS ITEM_LINE_TOTAL
    FROM SALES_DEV.SILVER.SALES_ITEM
    WHERE DQ_STATUS = 'PASS'
    GROUP BY TRANSACTION_ID
)
SELECT
    -- Fact surrogate key
    SHA2(CONCAT(
        COALESCE(h.TRANSACTION_ID, ''),
        COALESCE(TO_VARCHAR(h.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                                             AS TRANSACTION_FACT_KEY,

    -- Dimension keys (Hash-based foreign keys)
    dd.DATE_DIM_KEY                                     AS DATE_DIM_KEY,
    dc.CUSTOMER_DIM_KEY                                 AS CUSTOMER_DIM_KEY,
    ds.STORE_DIM_KEY                                    AS STORE_DIM_KEY,
    dcn.COUNTRY_DIM_KEY                                 AS COUNTRY_DIM_KEY,

    -- Degenerate dimensions
    h.TRANSACTION_ID                                    AS TRANSACTION_ID,
    h.TRANSACTION_NUMBER                                AS TRANSACTION_NUMBER,

    -- Business keys (convenience)
    h.CUSTOMER_ID                                       AS CUSTOMER_ID,
    h.STORE_ID                                          AS STORE_CODE,
    ds.COUNTRY_CODE                                     AS COUNTRY_CODE,
    dd.CAL_DATE                                         AS TRANSACTION_DATE,

    -- Transaction attributes
    h.CHANNEL_ID                                        AS CHANNEL_ID,
    h.PAYMENT_METHOD                                    AS PAYMENT_METHOD,
    h.CURRENCY                                          AS CURRENCY_CODE,

    -- Header measures
    h.GROSS_AMOUNT                                      AS GROSS_AMOUNT,
    h.TOTAL_DISCOUNT                                    AS TOTAL_DISCOUNT,
    h.TOTAL_TAX                                         AS TOTAL_TAX,
    h.NET_TOTAL                                         AS NET_TOTAL,

    -- Basket measures rolled up from items
    COALESCE(a.LINE_COUNT, 0)                           AS LINE_COUNT,
    COALESCE(a.TOTAL_QUANTITY, 0)                       AS TOTAL_QUANTITY,
    a.ITEM_LINE_TOTAL                                   AS ITEM_LINE_TOTAL,

    -- Audit
    h.BRONZE_LOAD_TS                                    AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.SALES_HEADER h
LEFT JOIN item_agg a
    ON h.TRANSACTION_ID = a.TRANSACTION_ID
LEFT JOIN SALES_DEV.GOLD.DIM_DATE dd
    ON h.TRANSACTION_TIMESTAMP = dd.CAL_DATE
LEFT JOIN SALES_DEV.GOLD.DIM_CUSTOMER dc
    ON h.CUSTOMER_ID = dc.CUSTOMER_ID AND dc.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_STORE ds
    ON h.STORE_ID = ds.STORE_CODE AND ds.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_COUNTRY dcn
    ON ds.COUNTRY_CODE = dcn.COUNTRY_CODE AND dcn.IS_CURRENT = TRUE
WHERE h.DQ_STATUS = 'PASS';


ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TRANSACTION_FACT_KEY COMMENT 'Hash-based surrogate key for the transaction; FACT_SALES_ITEM joins on this';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN DATE_DIM_KEY COMMENT 'Hash FK to DIM_DATE.DATE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN CUSTOMER_DIM_KEY COMMENT 'Hash FK to DIM_CUSTOMER.CUSTOMER_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN STORE_DIM_KEY COMMENT 'Hash FK to DIM_STORE.STORE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN COUNTRY_DIM_KEY COMMENT 'Hash FK to DIM_COUNTRY.COUNTRY_DIM_KEY (derived from store location)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TRANSACTION_ID COMMENT 'Degenerate dimension - sales transaction identifier';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TRANSACTION_NUMBER COMMENT 'Degenerate dimension - customer-facing receipt number';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN CUSTOMER_ID COMMENT 'Customer business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN STORE_CODE COMMENT 'Store business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN COUNTRY_CODE COMMENT 'Country business key of the selling store';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TRANSACTION_DATE COMMENT 'Date of the sales transaction';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN CHANNEL_ID COMMENT 'Sales channel (STORE, ONLINE, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN PAYMENT_METHOD COMMENT 'Payment method used for the transaction';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN CURRENCY_CODE COMMENT 'Transaction currency code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN GROSS_AMOUNT COMMENT 'Receipt gross amount before discount and tax (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TOTAL_DISCOUNT COMMENT 'Total discount on the receipt (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TOTAL_TAX COMMENT 'Total tax on the receipt (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN NET_TOTAL COMMENT 'Receipt net total = gross - discount + tax (additive); use for revenue and average order value';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN LINE_COUNT COMMENT 'Number of line items on the receipt (basket size)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN TOTAL_QUANTITY COMMENT 'Total units on the receipt (sum of item quantities)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_HEADER ALTER COLUMN ITEM_LINE_TOTAL COMMENT 'Sum of item LINE_TOTAL; should equal NET_TOTAL (reconciliation check)';


-- ==========================================================
-- 4. FACT_SALES_ITEM - line-item grain
--    Product-level KPIs; links to its receipt via TRANSACTION_FACT_KEY.
--    (Same grain as FACT_SALES above, plus the header link.)
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = INCREMENTAL
  INITIALIZE = ON_CREATE
  COMMENT = 'Sales item fact at line-item grain (one row per product on a receipt). Use for product-level KPIs: units, revenue by SKU/model/category, discounts by product. Carries header dimension keys so it can be sliced by date, customer, store and country without joining the header fact.'
AS
SELECT
    -- Fact surrogate key
    SHA2(CONCAT(
        COALESCE(i.TRANSACTION_LINE_ID, ''),
        COALESCE(TO_VARCHAR(i.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                                             AS SALES_ITEM_FACT_KEY,

    -- Parent header fact key
    SHA2(CONCAT(
        COALESCE(h.TRANSACTION_ID, ''),
        COALESCE(TO_VARCHAR(h.BRONZE_LOAD_TS, 'YYYY-MM-DD HH24:MI:SS.FF6'), '')
    ), 256)                                             AS TRANSACTION_FACT_KEY,

    -- Dimension keys (Hash-based foreign keys)
    dd.DATE_DIM_KEY                                     AS DATE_DIM_KEY,
    dc.CUSTOMER_DIM_KEY                                 AS CUSTOMER_DIM_KEY,
    dp.PRODUCT_DIM_KEY                                  AS PRODUCT_DIM_KEY,
    ds.STORE_DIM_KEY                                    AS STORE_DIM_KEY,
    dcn.COUNTRY_DIM_KEY                                 AS COUNTRY_DIM_KEY,

    -- Degenerate dimensions
    h.TRANSACTION_ID                                    AS TRANSACTION_ID,
    h.TRANSACTION_NUMBER                                AS TRANSACTION_NUMBER,
    i.TRANSACTION_LINE_ID                               AS TRANSACTION_LINE_ID,

    -- Business keys (convenience)
    h.CUSTOMER_ID                                       AS CUSTOMER_ID,
    i.SKU_CODE                                          AS SKU_CODE,
    h.STORE_ID                                          AS STORE_CODE,
    ds.COUNTRY_CODE                                     AS COUNTRY_CODE,
    dd.CAL_DATE                                         AS TRANSACTION_DATE,

    -- Transaction attributes
    h.CHANNEL_ID                                        AS CHANNEL_ID,
    h.PAYMENT_METHOD                                    AS PAYMENT_METHOD,
    h.CURRENCY                                          AS CURRENCY_CODE,

    -- Line measures
    i.QUANTITY                                          AS QUANTITY,
    i.UNIT_PRICE                                        AS UNIT_PRICE,
    (i.QUANTITY * i.UNIT_PRICE)                         AS GROSS_AMOUNT,
    i.DISCOUNT_AMOUNT                                   AS DISCOUNT_AMOUNT,
    i.TAX_AMOUNT                                        AS TAX_AMOUNT,
    i.LINE_TOTAL                                        AS LINE_TOTAL,

    -- Audit
    i.BRONZE_LOAD_TS                                    AS __SOURCE_LOAD_TS

FROM SALES_DEV.SILVER.SALES_ITEM i
INNER JOIN SALES_DEV.SILVER.SALES_HEADER h
    ON i.TRANSACTION_ID = h.TRANSACTION_ID AND h.DQ_STATUS = 'PASS'
LEFT JOIN SALES_DEV.GOLD.DIM_DATE dd
    ON h.TRANSACTION_TIMESTAMP = dd.CAL_DATE
LEFT JOIN SALES_DEV.GOLD.DIM_CUSTOMER dc
    ON h.CUSTOMER_ID = dc.CUSTOMER_ID AND dc.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_PRODUCT dp
    ON i.SKU_CODE = dp.SKU_CODE AND dp.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_STORE ds
    ON h.STORE_ID = ds.STORE_CODE AND ds.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_COUNTRY dcn
    ON ds.COUNTRY_CODE = dcn.COUNTRY_CODE AND dcn.IS_CURRENT = TRUE
WHERE i.DQ_STATUS = 'PASS';




ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN SALES_ITEM_FACT_KEY COMMENT 'Hash-based surrogate key for the sales line item';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN TRANSACTION_FACT_KEY COMMENT 'Hash FK to FACT_SALES_HEADER.TRANSACTION_FACT_KEY (parent receipt)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN DATE_DIM_KEY COMMENT 'Hash FK to DIM_DATE.DATE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN CUSTOMER_DIM_KEY COMMENT 'Hash FK to DIM_CUSTOMER.CUSTOMER_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN PRODUCT_DIM_KEY COMMENT 'Hash FK to DIM_PRODUCT.PRODUCT_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN STORE_DIM_KEY COMMENT 'Hash FK to DIM_STORE.STORE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN COUNTRY_DIM_KEY COMMENT 'Hash FK to DIM_COUNTRY.COUNTRY_DIM_KEY (derived from store location)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN TRANSACTION_ID COMMENT 'Degenerate dimension - sales transaction identifier';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN TRANSACTION_NUMBER COMMENT 'Degenerate dimension - customer-facing receipt number';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN TRANSACTION_LINE_ID COMMENT 'Degenerate dimension - line item identifier (grain)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN CUSTOMER_ID COMMENT 'Customer business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN SKU_CODE COMMENT 'Product SKU business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN STORE_CODE COMMENT 'Store business key';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN COUNTRY_CODE COMMENT 'Country business key of the selling store';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN TRANSACTION_DATE COMMENT 'Date of the sales transaction';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN CHANNEL_ID COMMENT 'Sales channel (STORE, ONLINE, etc.)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN PAYMENT_METHOD COMMENT 'Payment method used for the transaction';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN CURRENCY_CODE COMMENT 'Transaction currency code';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN QUANTITY COMMENT 'Units sold on this line (additive measure)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN UNIT_PRICE COMMENT 'Price per unit (non-additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN GROSS_AMOUNT COMMENT 'Quantity x unit price before discount and tax (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN DISCOUNT_AMOUNT COMMENT 'Discount applied to the line (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN TAX_AMOUNT COMMENT 'Tax charged on the line (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.FACT_SALES_ITEM ALTER COLUMN LINE_TOTAL COMMENT 'Net line amount = gross - discount + tax (additive)';


-- ==========================================================
-- 5. AGG_DAILY_SALES - pre-aggregated summary at
--    Date + Store + Product grain
--    Roll up to weekly/monthly/quarterly using DIM_DATE columns.
--    Sources exclusively from GOLD layer for full lineage.
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES
  TARGET_LAG = DOWNSTREAM
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = FULL
  INITIALIZE = ON_CREATE
  COMMENT = 'Pre-aggregated daily sales at Date + Store + Product grain. Use DIM_DATE calendar columns (WEEK_START_DATE, MONTH_START_DATE, QUARTER_START_DATE) to roll up to weekly, monthly, or quarterly summaries without re-scanning line-item facts.'
AS
SELECT
    -- Dimension Keys (for star-schema joins)
    dd.DATE_DIM_KEY,
    f.STORE_DIM_KEY,
    f.PRODUCT_DIM_KEY,
    f.COUNTRY_DIM_KEY,

    -- Date rollup columns (denormalized from DIM_DATE for fast GROUP BY)
    dd.CAL_DATE,
    dd.DAY_NAME,
    dd.WEEK_START_DATE,
    dd.WEEK_OF_YEAR,
    dd.MONTH_START_DATE,
    dd.MONTH_NUM,
    dd.MONTH_NAME,
    dd.QUARTER_START_DATE,
    dd.QUARTER_NAME,
    dd.YEAR_NUM,
    dd.FISCAL_YEAR,
    dd.FISCAL_QUARTER_NUM,
    dd.IS_WEEKEND,

    -- Business keys (convenience for filtering without joining dims)
    f.STORE_CODE,
    f.SKU_CODE,
    f.COUNTRY_CODE,

    -- Additive measures
    COUNT(DISTINCT f.TRANSACTION_ID)                    AS TRANSACTION_COUNT,
    COUNT(*)                                            AS LINE_COUNT,
    SUM(f.QUANTITY)                                     AS TOTAL_QUANTITY,
    SUM(f.QUANTITY * f.UNIT_PRICE)                      AS GROSS_AMOUNT,
    SUM(f.DISCOUNT_AMOUNT)                              AS TOTAL_DISCOUNT,
    SUM(f.TAX_AMOUNT)                                   AS TOTAL_TAX,
    SUM(f.LINE_TOTAL)                                   AS NET_TOTAL,

    -- Derived measures
    DIV0NULL(SUM(f.QUANTITY * f.UNIT_PRICE), NULLIF(SUM(f.QUANTITY), 0))
                                                        AS AVG_UNIT_PRICE,
    DIV0NULL(SUM(f.LINE_TOTAL), NULLIF(COUNT(DISTINCT f.TRANSACTION_ID), 0))
                                                        AS AVG_ORDER_VALUE,
    MIN(f.UNIT_PRICE)                                   AS MIN_UNIT_PRICE,
    MAX(f.UNIT_PRICE)                                   AS MAX_UNIT_PRICE

FROM SALES_DEV.GOLD.FACT_SALES f
INNER JOIN SALES_DEV.GOLD.DIM_DATE dd
    ON f.DATE_DIM_KEY = dd.DATE_DIM_KEY
GROUP BY
    dd.DATE_DIM_KEY,
    f.STORE_DIM_KEY,
    f.PRODUCT_DIM_KEY,
    f.COUNTRY_DIM_KEY,
    dd.CAL_DATE,
    dd.DAY_NAME,
    dd.WEEK_START_DATE,
    dd.WEEK_OF_YEAR,
    dd.MONTH_START_DATE,
    dd.MONTH_NUM,
    dd.MONTH_NAME,
    dd.QUARTER_START_DATE,
    dd.QUARTER_NAME,
    dd.YEAR_NUM,
    dd.FISCAL_YEAR,
    dd.FISCAL_QUARTER_NUM,
    dd.IS_WEEKEND,
    f.STORE_CODE,
    f.SKU_CODE,
    f.COUNTRY_CODE;


ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN DATE_DIM_KEY COMMENT 'Hash FK to DIM_DATE.DATE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN STORE_DIM_KEY COMMENT 'Hash FK to DIM_STORE.STORE_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN PRODUCT_DIM_KEY COMMENT 'Hash FK to DIM_PRODUCT.PRODUCT_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN COUNTRY_DIM_KEY COMMENT 'Hash FK to DIM_COUNTRY.COUNTRY_DIM_KEY';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN CAL_DATE COMMENT 'Calendar date (base grain)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN WEEK_START_DATE COMMENT 'Week start date - GROUP BY this for weekly rollup';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN MONTH_START_DATE COMMENT 'Month start date - GROUP BY this for monthly rollup';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN QUARTER_START_DATE COMMENT 'Quarter start date - GROUP BY this for quarterly rollup';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN TRANSACTION_COUNT COMMENT 'Distinct transactions for the day/store/product (semi-additive across products)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN LINE_COUNT COMMENT 'Number of line items sold';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN TOTAL_QUANTITY COMMENT 'Total units sold (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN GROSS_AMOUNT COMMENT 'Quantity x unit price before discount (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN TOTAL_DISCOUNT COMMENT 'Total discount (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN TOTAL_TAX COMMENT 'Total tax (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN NET_TOTAL COMMENT 'Net sales = gross - discount + tax (additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN AVG_UNIT_PRICE COMMENT 'Weighted average unit price (non-additive - do not SUM across rows)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN AVG_ORDER_VALUE COMMENT 'Average order value per transaction (non-additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN MIN_UNIT_PRICE COMMENT 'Lowest unit price sold that day (non-additive)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_DAILY_SALES ALTER COLUMN MAX_UNIT_PRICE COMMENT 'Highest unit price sold that day (non-additive)';
