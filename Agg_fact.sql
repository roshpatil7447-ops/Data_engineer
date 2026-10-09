-- =====================================================
-- Aggregated Fact Tables - SALES_DEV.GOLD
--
-- Pre-aggregated summaries for dashboard and reporting:
--   AGG_SALES_DAILY   : One row per date + store + channel
--   AGG_SALES_WEEKLY  : One row per week + store + channel
--   AGG_SALES_MONTHLY : One row per month + store + channel
--
-- All source from GOLD fact/dim tables for full lineage:
--   FACT_SALES_HEADER + DIM_DATE -> AGG tables
-- =====================================================


-- ==========================================================
-- 1. AGG_SALES_DAILY
--    Grain: date + store + channel
--    Use for: daily dashboards, trend analysis, daily KPIs
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY
  TARGET_LAG = '1 day'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = FULL
  INITIALIZE = ON_CREATE
  COMMENT = 'Daily aggregated sales summary. Grain: one row per date + store + channel. Use for daily dashboards, trend charts, and operational KPIs.'
AS
SELECT
    -- Date grain
    d.DATE_DIM_KEY,
    fh.TRANSACTION_DATE,
    d.DAY_NAME,
    d.DAY_OF_WEEK_NUM,
    d.IS_WEEKEND,

    -- Slice dimensions
    fh.STORE_CODE,
    s.STORE_NAME,
    fh.COUNTRY_CODE,
    cn.COUNTRY_NAME,
    fh.CHANNEL_ID,
    fh.CURRENCY_CODE,

    -- Measures
    COUNT(DISTINCT fh.TRANSACTION_ID)                   AS TRANSACTION_COUNT,
    COUNT(DISTINCT fh.CUSTOMER_ID)                      AS UNIQUE_CUSTOMERS,
    SUM(fh.GROSS_AMOUNT)                                AS TOTAL_GROSS_AMOUNT,
    SUM(fh.TOTAL_DISCOUNT)                              AS TOTAL_DISCOUNT,
    SUM(fh.TOTAL_TAX)                                   AS TOTAL_TAX,
    SUM(fh.NET_TOTAL)                                   AS TOTAL_NET_REVENUE,
    SUM(fh.LINE_COUNT)                                  AS TOTAL_LINE_ITEMS,
    SUM(fh.TOTAL_QUANTITY)                               AS TOTAL_UNITS_SOLD,

    -- Derived KPIs
    DIV0NULL(SUM(fh.NET_TOTAL), COUNT(DISTINCT fh.TRANSACTION_ID))
                                                        AS AVG_ORDER_VALUE,
    DIV0NULL(SUM(fh.TOTAL_QUANTITY), COUNT(DISTINCT fh.TRANSACTION_ID))
                                                        AS AVG_UNITS_PER_TRANSACTION,
    DIV0NULL(SUM(fh.TOTAL_DISCOUNT), SUM(fh.GROSS_AMOUNT))
                                                        AS DISCOUNT_RATE

FROM SALES_DEV.GOLD.FACT_SALES_HEADER fh
JOIN SALES_DEV.GOLD.DIM_DATE d
    ON fh.TRANSACTION_DATE = d.CAL_DATE
LEFT JOIN SALES_DEV.GOLD.DIM_STORE s
    ON fh.STORE_CODE = s.STORE_CODE AND s.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_COUNTRY cn
    ON fh.COUNTRY_CODE = cn.COUNTRY_CODE AND cn.IS_CURRENT = TRUE
GROUP BY
    d.DATE_DIM_KEY, fh.TRANSACTION_DATE, d.DAY_NAME, d.DAY_OF_WEEK_NUM, d.IS_WEEKEND,
    fh.STORE_CODE, s.STORE_NAME, fh.COUNTRY_CODE, cn.COUNTRY_NAME,
    fh.CHANNEL_ID, fh.CURRENCY_CODE;


ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN DATE_DIM_KEY COMMENT 'Hash FK to DIM_DATE';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN TRANSACTION_DATE COMMENT 'Calendar date (grain)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN TRANSACTION_COUNT COMMENT 'Distinct transactions on this date/store/channel';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN UNIQUE_CUSTOMERS COMMENT 'Distinct customers who transacted';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN TOTAL_NET_REVENUE COMMENT 'Sum of net total (gross - discount + tax)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN AVG_ORDER_VALUE COMMENT 'Net revenue / transaction count';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN AVG_UNITS_PER_TRANSACTION COMMENT 'Total units / transaction count';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_DAILY ALTER COLUMN DISCOUNT_RATE COMMENT 'Total discount / gross amount';


-- ==========================================================
-- 2. AGG_SALES_WEEKLY
--    Grain: week start date + store + channel
--    Use for: weekly reports, week-over-week trends
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY
  TARGET_LAG = '7 days'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = FULL
  INITIALIZE = ON_CREATE
  COMMENT = 'Weekly aggregated sales summary. Grain: one row per week + store + channel. Use for weekly reports and week-over-week trend analysis.'
AS
SELECT
    -- Week grain
    d.WEEK_START_DATE,
    d.WEEK_OF_YEAR,
    d.YEAR_NUM,

    -- Slice dimensions
    fh.STORE_CODE,
    s.STORE_NAME,
    fh.COUNTRY_CODE,
    cn.COUNTRY_NAME,
    fh.CHANNEL_ID,
    fh.CURRENCY_CODE,

    -- Measures
    COUNT(DISTINCT fh.TRANSACTION_DATE)                 AS TRADING_DAYS,
    COUNT(DISTINCT fh.TRANSACTION_ID)                   AS TRANSACTION_COUNT,
    COUNT(DISTINCT fh.CUSTOMER_ID)                      AS UNIQUE_CUSTOMERS,
    SUM(fh.GROSS_AMOUNT)                                AS TOTAL_GROSS_AMOUNT,
    SUM(fh.TOTAL_DISCOUNT)                              AS TOTAL_DISCOUNT,
    SUM(fh.TOTAL_TAX)                                   AS TOTAL_TAX,
    SUM(fh.NET_TOTAL)                                   AS TOTAL_NET_REVENUE,
    SUM(fh.LINE_COUNT)                                  AS TOTAL_LINE_ITEMS,
    SUM(fh.TOTAL_QUANTITY)                               AS TOTAL_UNITS_SOLD,

    -- Derived KPIs
    DIV0NULL(SUM(fh.NET_TOTAL), COUNT(DISTINCT fh.TRANSACTION_ID))
                                                        AS AVG_ORDER_VALUE,
    DIV0NULL(SUM(fh.TOTAL_QUANTITY), COUNT(DISTINCT fh.TRANSACTION_ID))
                                                        AS AVG_UNITS_PER_TRANSACTION,
    DIV0NULL(SUM(fh.TOTAL_DISCOUNT), SUM(fh.GROSS_AMOUNT))
                                                        AS DISCOUNT_RATE,
    DIV0NULL(SUM(fh.NET_TOTAL), COUNT(DISTINCT fh.TRANSACTION_DATE))
                                                        AS AVG_DAILY_REVENUE

FROM SALES_DEV.GOLD.FACT_SALES_HEADER fh
JOIN SALES_DEV.GOLD.DIM_DATE d
    ON fh.TRANSACTION_DATE = d.CAL_DATE
LEFT JOIN SALES_DEV.GOLD.DIM_STORE s
    ON fh.STORE_CODE = s.STORE_CODE AND s.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_COUNTRY cn
    ON fh.COUNTRY_CODE = cn.COUNTRY_CODE AND cn.IS_CURRENT = TRUE
GROUP BY
    d.WEEK_START_DATE, d.WEEK_OF_YEAR, d.YEAR_NUM,
    fh.STORE_CODE, s.STORE_NAME, fh.COUNTRY_CODE, cn.COUNTRY_NAME,
    fh.CHANNEL_ID, fh.CURRENCY_CODE;


ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN WEEK_START_DATE COMMENT 'Monday of the week (grain)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN TRADING_DAYS COMMENT 'Number of distinct days with transactions in this week';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN TRANSACTION_COUNT COMMENT 'Distinct transactions in the week';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN UNIQUE_CUSTOMERS COMMENT 'Distinct customers who transacted in the week';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN TOTAL_NET_REVENUE COMMENT 'Sum of net total for the week';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN AVG_ORDER_VALUE COMMENT 'Net revenue / transaction count';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN AVG_DAILY_REVENUE COMMENT 'Net revenue / trading days';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_WEEKLY ALTER COLUMN DISCOUNT_RATE COMMENT 'Total discount / gross amount';


-- ==========================================================
-- 3. AGG_SALES_MONTHLY
--    Grain: month + store + channel
--    Use for: monthly reports, MoM trends, executive summaries
-- ==========================================================
CREATE OR REPLACE DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY
  TARGET_LAG = '30 days'
  WAREHOUSE = COMPUTE_WH
  REFRESH_MODE = FULL
  INITIALIZE = ON_CREATE
  COMMENT = 'Monthly aggregated sales summary. Grain: one row per month + store + channel. Use for monthly reports, MoM trends, and executive summaries.'
AS
SELECT
    -- Month grain
    d.MONTH_START_DATE,
    d.MONTH_END_DATE,
    d.MONTH_NUM,
    d.MONTH_NAME,
    d.QUARTER_NUM,
    d.QUARTER_NAME,
    d.YEAR_NUM,
    d.FISCAL_YEAR,
    d.FISCAL_QUARTER_NUM,
    d.YEAR_MONTH_LABEL,

    -- Slice dimensions
    fh.STORE_CODE,
    s.STORE_NAME,
    fh.COUNTRY_CODE,
    cn.COUNTRY_NAME,
    fh.CHANNEL_ID,
    fh.CURRENCY_CODE,

    -- Measures
    COUNT(DISTINCT fh.TRANSACTION_DATE)                 AS TRADING_DAYS,
    COUNT(DISTINCT fh.TRANSACTION_ID)                   AS TRANSACTION_COUNT,
    COUNT(DISTINCT fh.CUSTOMER_ID)                      AS UNIQUE_CUSTOMERS,
    SUM(fh.GROSS_AMOUNT)                                AS TOTAL_GROSS_AMOUNT,
    SUM(fh.TOTAL_DISCOUNT)                              AS TOTAL_DISCOUNT,
    SUM(fh.TOTAL_TAX)                                   AS TOTAL_TAX,
    SUM(fh.NET_TOTAL)                                   AS TOTAL_NET_REVENUE,
    SUM(fh.LINE_COUNT)                                  AS TOTAL_LINE_ITEMS,
    SUM(fh.TOTAL_QUANTITY)                               AS TOTAL_UNITS_SOLD,

    -- Derived KPIs
    DIV0NULL(SUM(fh.NET_TOTAL), COUNT(DISTINCT fh.TRANSACTION_ID))
                                                        AS AVG_ORDER_VALUE,
    DIV0NULL(SUM(fh.TOTAL_QUANTITY), COUNT(DISTINCT fh.TRANSACTION_ID))
                                                        AS AVG_UNITS_PER_TRANSACTION,
    DIV0NULL(SUM(fh.TOTAL_DISCOUNT), SUM(fh.GROSS_AMOUNT))
                                                        AS DISCOUNT_RATE,
    DIV0NULL(SUM(fh.NET_TOTAL), COUNT(DISTINCT fh.TRANSACTION_DATE))
                                                        AS AVG_DAILY_REVENUE

FROM SALES_DEV.GOLD.FACT_SALES_HEADER fh
JOIN SALES_DEV.GOLD.DIM_DATE d
    ON fh.TRANSACTION_DATE = d.CAL_DATE
LEFT JOIN SALES_DEV.GOLD.DIM_STORE s
    ON fh.STORE_CODE = s.STORE_CODE AND s.IS_CURRENT = TRUE
LEFT JOIN SALES_DEV.GOLD.DIM_COUNTRY cn
    ON fh.COUNTRY_CODE = cn.COUNTRY_CODE AND cn.IS_CURRENT = TRUE
GROUP BY
    d.MONTH_START_DATE, d.MONTH_END_DATE, d.MONTH_NUM, d.MONTH_NAME,
    d.QUARTER_NUM, d.QUARTER_NAME, d.YEAR_NUM,
    d.FISCAL_YEAR, d.FISCAL_QUARTER_NUM, d.YEAR_MONTH_LABEL,
    fh.STORE_CODE, s.STORE_NAME, fh.COUNTRY_CODE, cn.COUNTRY_NAME,
    fh.CHANNEL_ID, fh.CURRENCY_CODE;


ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN MONTH_START_DATE COMMENT 'First day of the month (grain)';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN YEAR_MONTH_LABEL COMMENT 'YYYY-MM label for reporting';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN TRADING_DAYS COMMENT 'Number of distinct days with transactions in this month';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN TRANSACTION_COUNT COMMENT 'Distinct transactions in the month';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN UNIQUE_CUSTOMERS COMMENT 'Distinct customers who transacted in the month';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN TOTAL_NET_REVENUE COMMENT 'Sum of net total for the month';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN AVG_ORDER_VALUE COMMENT 'Net revenue / transaction count';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN AVG_DAILY_REVENUE COMMENT 'Net revenue / trading days';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN DISCOUNT_RATE COMMENT 'Total discount / gross amount';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN FISCAL_YEAR COMMENT 'Fiscal year (Apr-Mar) for financial reporting';
ALTER DYNAMIC TABLE SALES_DEV.GOLD.AGG_SALES_MONTHLY ALTER COLUMN FISCAL_QUARTER_NUM COMMENT 'Fiscal quarter (1-4, starting Apr)';
