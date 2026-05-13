# Snowflake Price Tracker Pipeline

An end-to-end data pipeline built in Snowflake demonstrating
SCD Type 2, Change Data Capture using Streams, and a 
three-layer medallion architecture.

## Architecture
Stage (raw) → Clean (validated) → Mart (star schema)

## Key Concepts Demonstrated
- Snowflake Streams for Change Data Capture (CDC)
- SCD Type 2 for full historical tracking of price changes
- Three-layer medallion architecture (Stage → Clean → Mart)
- MERGE statements for upsert logic
- Analytical views with window functions (LAG) for % change calculation

## How to Run
1. Upload `data/products_day1.csv` and `data/products_day2.csv`
   to a Snowflake internal stage
2. Run `sql/price_tracker.sql` in a Snowflake worksheet

## Results
- Product 101 price drop from 99.99 → 79.99 (-20%) correctly
  captured as two SCD2 rows
- Historical rows preserved with eff_start_dt / eff_end_dt
- Price change view surfaces all movements with percentage delta

## Tech Stack
- Snowflake (Streams, Stages, Merge, Views)
- SQL
