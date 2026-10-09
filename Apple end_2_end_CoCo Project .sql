----------------------------------------------------------------------
-- Snowflake Cloud Data Platform – Medallion Architecture DDL
-- DEV & QA: Transient databases (no Time Travel / no Fail-Safe)
-- PROD:     Permanent database with 7-day Time Travel
----------------------------------------------------------------------

-- ============================================================
-- 1. DEV DATABASE (Transient)
-- ============================================================
CREATE OR REPLACE TRANSIENT DATABASE SALES_DEV
    COMMENT = 'Sales – Development context (transient, no time travel)';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.BRONZE
    COMMENT = 'Raw / landing zone – ingested data as-is from source systems';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.SILVER
    COMMENT = 'Cleansed & curated zone – SCD, dimensional models, conforming';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.GOLD
    COMMENT = 'Business-ready zone – KPIs, aggregations, reporting/dashboard tables';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_DEV.COMMON
    COMMENT = 'Shared utilities – reusable functions and stored procedures';


-- ============================================================
-- 2. QA DATABASE (Transient)
-- ============================================================
CREATE OR REPLACE TRANSIENT DATABASE SALES_QA
    COMMENT = 'Sales – QA context (transient, no time travel)';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.BRONZE
    COMMENT = 'Raw / landing zone – ingested data as-is from source systems';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.SILVER
    COMMENT = 'Cleansed & curated zone – SCD, dimensional models, conforming';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.GOLD
    COMMENT = 'Business-ready zone – KPIs, aggregations, reporting/dashboard tables';

CREATE OR REPLACE TRANSIENT SCHEMA SALES_QA.COMMON
    COMMENT = 'Shared utilities – reusable functions and stored procedures';


-- ============================================================
-- 3. PROD DATABASE (Permanent – 7-day Time Travel)
-- ============================================================
CREATE OR REPLACE DATABASE SALES_PROD
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Sales – Production context (permanent, 7-day time travel)';

CREATE OR REPLACE SCHEMA SALES_PROD.BRONZE
    COMMENT = 'Raw / landing zone – ingested data as-is from source systems';

CREATE OR REPLACE SCHEMA SALES_PROD.SILVER
    COMMENT = 'Cleansed & curated zone – SCD, dimensional models, conforming';

CREATE OR REPLACE SCHEMA SALES_PROD.GOLD
    COMMENT = 'Business-ready zone – KPIs, aggregations, reporting/dashboard tables';

CREATE OR REPLACE SCHEMA SALES_PROD.COMMON
    COMMENT = 'Shared utilities – reusable functions and stored procedures';
SALES_QA.BRONZE