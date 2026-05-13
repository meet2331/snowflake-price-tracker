USE ROLE SYSADMIN;

DROP DATABASE IF EXISTS price_tracker_db;
CREATE DATABASE price_tracker_db;
USE DATABASE price_tracker_db;

CREATE WAREHOUSE IF NOT EXISTS price_wh
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE;

USE WAREHOUSE price_wh;

CREATE SCHEMA stage;
CREATE SCHEMA clean;
CREATE SCHEMA mart;

USE SCHEMA stage;
CREATE FILE FORMAT csv_ff
    TYPE = 'CSV'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"';

CREATE STAGE product_stg FILE_FORMAT = csv_ff;

-- ============================================================
-- STOP HERE — upload products_day1.csv and products_day2.csv
-- to @price_tracker_db.stage.product_stg, then continue
-- ============================================================

-- STAGE TABLES & STREAMS
USE SCHEMA stage;
CREATE TABLE product_raw (
    product_id   TEXT,
    product_name TEXT,
    category     TEXT,
    price        TEXT,
    updated_at   TEXT,
    _file_name   TEXT,
    _load_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE STREAM product_stm ON TABLE product_raw;

-- CLEAN TABLES & STREAMS
USE SCHEMA clean;
CREATE TABLE product (
    product_sk   NUMBER AUTOINCREMENT PRIMARY KEY,
    product_id   NUMBER        NOT NULL UNIQUE,
    product_name STRING        NOT NULL,
    category     STRING        NOT NULL,
    price        NUMBER(10,2)  NOT NULL,
    updated_at   TIMESTAMP_NTZ NOT NULL,
    _load_ts     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
CREATE STREAM product_stm ON TABLE product;

-- MART TABLE
USE SCHEMA mart;
CREATE TABLE dim_product (
    product_hk   NUMBER        PRIMARY KEY,
    product_id   NUMBER        NOT NULL,
    product_name STRING        NOT NULL,
    category     STRING        NOT NULL,
    price        NUMBER(10,2)  NOT NULL,
    eff_start_dt TIMESTAMP_NTZ NOT NULL,
    eff_end_dt   TIMESTAMP_NTZ,
    is_current   BOOLEAN       NOT NULL DEFAULT TRUE
);

-- DAY 1 LOAD

-- 1a. Load raw
USE SCHEMA stage;
COPY INTO product_raw
FROM (
    SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, CURRENT_TIMESTAMP()
    FROM @product_stg/products_day1.csv
);
-- autocommit → stage.product_stm now has data

-- 1b. Stage → Clean  (all new rows on Day 1)
USE SCHEMA clean;
MERGE INTO product tgt
USING (
    SELECT
        CAST(product_id   AS NUMBER)       AS product_id,
        CAST(product_name AS STRING)       AS product_name,
        CAST(category     AS STRING)       AS category,
        CAST(price        AS NUMBER(10,2)) AS price,
        TO_TIMESTAMP_NTZ(updated_at)       AS updated_at
    FROM stage.product_stm
) src ON tgt.product_id = src.product_id
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, price, updated_at)
    VALUES (src.product_id, src.product_name, src.category, src.price, src.updated_at);
-- autocommit → clean.product_stm now has data

-- 1c. Clean → Mart  (Day 1: only new inserts, single MERGE is safe)
USE SCHEMA mart;
MERGE INTO dim_product tgt
USING clean.product_stm src
    ON tgt.product_id = src.product_id AND tgt.is_current = TRUE
WHEN NOT MATCHED AND src.METADATA$ACTION = 'INSERT' THEN
    INSERT (product_hk, product_id, product_name, category, price, eff_start_dt, eff_end_dt, is_current)
    VALUES (
        HASH(src.product_id, src.price, src.updated_at),
        src.product_id, src.product_name, src.category, src.price,
        src.updated_at, NULL, TRUE
    );
-- autocommit → clean.product_stm consumed


-- DAY 2 LOAD

-- 2a. Load raw
USE SCHEMA stage;
COPY INTO product_raw
FROM (
    SELECT $1, $2, $3, $4, $5, METADATA$FILENAME, CURRENT_TIMESTAMP()
    FROM @product_stg/products_day2.csv
);
-- autocommit → stage.product_stm now has data

-- 2b. Stage → Clean  (update existing rows, insert new ones)
USE SCHEMA clean;
MERGE INTO product tgt
USING (
    SELECT
        CAST(product_id   AS NUMBER)       AS product_id,
        CAST(product_name AS STRING)       AS product_name,
        CAST(category     AS STRING)       AS category,
        CAST(price        AS NUMBER(10,2)) AS price,
        TO_TIMESTAMP_NTZ(updated_at)       AS updated_at
    FROM stage.product_stm
) src ON tgt.product_id = src.product_id
WHEN MATCHED THEN
    UPDATE SET tgt.price = src.price, tgt.updated_at = src.updated_at
WHEN NOT MATCHED THEN
    INSERT (product_id, product_name, category, price, updated_at)
    VALUES (src.product_id, src.product_name, src.category, src.price, src.updated_at);


BEGIN;

-- Expire old rows
UPDATE mart.dim_product tgt
SET    tgt.eff_end_dt  = CURRENT_TIMESTAMP(),
       tgt.is_current  = FALSE
FROM   clean.product_stm src
WHERE  tgt.product_id        = src.product_id
  AND  tgt.is_current        = TRUE
  AND  src.METADATA$ACTION   = 'DELETE'
  AND  src.METADATA$ISUPDATE = TRUE;

-- Insert new current rows (covers both updated products and brand-new ones)
INSERT INTO mart.dim_product
    (product_hk, product_id, product_name, category, price, eff_start_dt, eff_end_dt, is_current)
SELECT
    HASH(src.product_id, src.price, src.updated_at),
    src.product_id,
    src.product_name,
    src.category,
    src.price,
    src.updated_at,
    NULL,
    TRUE
FROM clean.product_stm src
WHERE src.METADATA$ACTION = 'INSERT';

COMMIT;

-- stream consumed once here, both statements saw the same delta ✓
-- ANALYTICS

USE SCHEMA mart;

CREATE OR REPLACE VIEW v_price_changes AS
WITH hist AS (
    SELECT
        product_id,
        product_name,
        price,
        eff_start_dt,
        LAG(price) OVER (PARTITION BY product_id ORDER BY eff_start_dt) AS prev_price
    FROM dim_product
)
SELECT
    product_id,
    product_name,
    prev_price,
    price                                                  AS current_price,
    ROUND((price - prev_price) / prev_price * 100, 2)     AS pct_change,
    eff_start_dt                                           AS change_dt,
    CASE
        WHEN price < prev_price THEN 'DROP'
        WHEN price > prev_price THEN 'INCREASE'
        ELSE 'UNCHANGED'
    END                                                    AS change_type
FROM hist
WHERE prev_price IS NOT NULL;

-- VERIFY

-- Full SCD2 history for product 101 (should show 2 rows: one expired, one current)
SELECT 'Product 101 History' AS test, product_id, price, eff_start_dt, eff_end_dt, is_current
FROM   mart.dim_product
WHERE  product_id = 101
ORDER  BY eff_start_dt;

-- All price changes
SELECT 'All Price Changes' AS test, *
FROM   mart.v_price_changes
ORDER  BY product_id, change_dt;

-- Only drops >= 10%
SELECT 'Drops >= 10%' AS test, *
FROM   mart.v_price_changes
WHERE  pct_change <= -10
ORDER  BY pct_change;
