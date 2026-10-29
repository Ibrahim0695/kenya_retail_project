-- Analytical examples for the PostgreSQL warehouse schema.


-- Query 1: Regional sales performance.
-- fact_sales is at transaction-line grain, so count distinct transaction IDs.
SELECT
    s.region,
    s.county,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.total_amount) AS total_revenue
FROM warehouse.fact_sales AS f
JOIN warehouse.dim_store AS s
    ON f.store_key = s.store_key
GROUP BY s.region, s.county
ORDER BY total_revenue DESC;

-- Query 2: Price competitiveness (supermarket vs. KAMIS).
-- Prices are compared only when both sides use the same normalized basis unit.
WITH latest_kamis_date AS (
    SELECT
        LOWER(BTRIM(f.commodity)) AS commodity_key,
        f.price_basis_unit,
        MAX(d.full_date) AS full_date
    FROM warehouse.fact_market_prices AS f
    JOIN warehouse.dim_date AS d USING (date_key)
    WHERE f.retail_price_per_base_unit IS NOT NULL
      AND f.price_basis_unit IN ('kg', 'l')
    GROUP BY LOWER(BTRIM(f.commodity)), f.price_basis_unit
),
supermarket_prices AS (
    SELECT
        f.product_key,
        p.base_unit,
        SUM(f.total_amount) / NULLIF(SUM(f.base_quantity_sold), 0)
            AS average_supermarket_price_per_base_unit,
        SUM(f.base_quantity_sold) AS base_units_sold
    FROM warehouse.fact_sales AS f
    JOIN warehouse.dim_date AS d USING (date_key)
    JOIN warehouse.dim_product AS p USING (product_key)
    WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
      AND f.unit_price_per_base_unit IS NOT NULL
    GROUP BY f.product_key, p.base_unit
),
kamis_prices AS (
    SELECT
        LOWER(BTRIM(p.market_mapping_code)) AS market_mapping_code,
        f.price_basis_unit,
        latest.full_date AS kamis_survey_date,
        AVG(f.retail_price_per_base_unit)
            AS average_kamis_retail_price_per_base_unit,
        COUNT(DISTINCT f.market_key) AS kamis_market_count
    FROM warehouse.fact_market_prices AS f
    JOIN warehouse.dim_date AS d USING (date_key)
    JOIN warehouse.dim_product AS p
        ON LOWER(BTRIM(p.market_mapping_code)) = LOWER(BTRIM(f.commodity))
    JOIN latest_kamis_date AS latest
        ON LOWER(BTRIM(f.commodity)) = latest.commodity_key
        AND f.price_basis_unit = latest.price_basis_unit
        AND d.full_date = latest.full_date
    WHERE f.retail_price_per_base_unit IS NOT NULL
    GROUP BY
        LOWER(BTRIM(p.market_mapping_code)),
        f.price_basis_unit,
        latest.full_date
)
SELECT
    p.product_id,
    p.product_name,
    p.market_mapping_code,
    p.base_unit AS comparison_basis_unit,
    sp.average_supermarket_price_per_base_unit,
    kp.average_kamis_retail_price_per_base_unit,
    kp.kamis_survey_date,
    kp.kamis_market_count,
    sp.base_units_sold AS supermarket_base_units_sold_30d,
    ROUND((
        100.0 * (
            sp.average_supermarket_price_per_base_unit
            - kp.average_kamis_retail_price_per_base_unit
        ) / NULLIF(kp.average_kamis_retail_price_per_base_unit, 0)
    )::NUMERIC, 2) AS supermarket_premium_pct,
    CASE
        WHEN p.market_mapping_code IS NULL THEN 'Needs verified commodity mapping'
        WHEN sp.average_supermarket_price_per_base_unit IS NULL
            THEN 'No supermarket sales in the last 30 days'
        WHEN kp.average_kamis_retail_price_per_base_unit IS NULL
            THEN 'No latest comparable KAMIS price'
        WHEN sp.base_unit <> kp.price_basis_unit
            THEN 'Incompatible comparison units'
        WHEN sp.average_supermarket_price_per_base_unit
             < kp.average_kamis_retail_price_per_base_unit
            THEN 'Supermarket below KAMIS'
        WHEN sp.average_supermarket_price_per_base_unit
             > kp.average_kamis_retail_price_per_base_unit
            THEN 'Supermarket above KAMIS'
        ELSE 'Prices are equal'
    END AS comparison_status
FROM warehouse.dim_product AS p
LEFT JOIN supermarket_prices AS sp USING (product_key)
LEFT JOIN kamis_prices AS kp
    ON LOWER(BTRIM(p.market_mapping_code)) = kp.market_mapping_code
    AND sp.base_unit = kp.price_basis_unit
ORDER BY p.product_name;

-- Query 3: Daily sales trend by store.
SELECT
    d.full_date,
    s.store_name,
    s.county,
    SUM(f.total_amount) AS revenue,
    SUM(f.quantity_sold) AS units_sold,
    COUNT(DISTINCT f.transaction_id) AS transactions
FROM warehouse.fact_sales AS f
JOIN warehouse.dim_date AS d USING (date_key)
JOIN warehouse.dim_store AS s USING (store_key)
GROUP BY d.full_date, s.store_name, s.county
ORDER BY d.full_date DESC, revenue DESC;

-- Query 4: Monthly KNBS inflation by economic division.
SELECT
    d.year,
    d.month,
    f.series_id,
    f.series_name,
    f.inflation_rate_yoy,
    f.month_on_month_pct,
    f.weight_pct,
    f.cpi_index
FROM warehouse.fact_macro_indicators AS f
JOIN warehouse.dim_date AS d USING (date_key)
ORDER BY d.full_date DESC, f.series_name;

-- Query 5: Latest stock position by store and product.
SELECT
    s.store_name,
    s.county,
    p.product_name,
    p.category,
    f.on_hand_quantity,
    f.delivered_quantity,
    f.sold_quantity
FROM warehouse.fact_inventory_position AS f
JOIN warehouse.dim_store AS s USING (store_key)
JOIN warehouse.dim_product AS p USING (product_key)
ORDER BY s.store_name, p.product_name;
