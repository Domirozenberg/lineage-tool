"""Seed the lineage-postgres container with a realistic e-commerce + analytics schema.

Creates three schemas:
  raw  — 20 staging tables
  dw   — 15 dimension + fact tables
  rpt  — 15 views/materialized views + 2 stored functions

Usage:
  python3 scripts/seed_test_db.py [--host localhost] [--port 5433] \
      [--dbname lineage_sample] [--user lineage] [--password lineage]
"""

import argparse
import sys

import psycopg2
from psycopg2.extensions import connection as PgConnection


# ---------------------------------------------------------------------------
# DDL helpers
# ---------------------------------------------------------------------------

RAW_TABLES = """
-- ---- raw schema --------------------------------------------------------

CREATE TABLE raw.customers (
    customer_id   SERIAL PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    phone         VARCHAR(20),
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_raw_customers_email ON raw.customers(email);

CREATE TABLE raw.addresses (
    address_id    SERIAL PRIMARY KEY,
    customer_id   INT REFERENCES raw.customers(customer_id),
    address_line1 VARCHAR(255) NOT NULL,
    address_line2 VARCHAR(255),
    city          VARCHAR(100) NOT NULL,
    state         VARCHAR(100),
    country       VARCHAR(100) NOT NULL,
    postal_code   VARCHAR(20),
    is_default    BOOLEAN DEFAULT FALSE
);

CREATE TABLE raw.categories (
    category_id   SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    parent_id     INT REFERENCES raw.categories(category_id),
    description   TEXT,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.suppliers (
    supplier_id   SERIAL PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    email         VARCHAR(255),
    phone         VARCHAR(20),
    country       VARCHAR(100),
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.products (
    product_id    SERIAL PRIMARY KEY,
    sku           VARCHAR(100) NOT NULL UNIQUE,
    name          VARCHAR(255) NOT NULL,
    description   TEXT,
    category_id   INT REFERENCES raw.categories(category_id),
    supplier_id   INT REFERENCES raw.suppliers(supplier_id),
    price         NUMERIC(12,2) NOT NULL,
    cost          NUMERIC(12,2),
    weight_kg     NUMERIC(8,3),
    is_active     BOOLEAN DEFAULT TRUE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_raw_products_sku ON raw.products(sku);
CREATE INDEX idx_raw_products_category ON raw.products(category_id);

CREATE TABLE raw.warehouses (
    warehouse_id  SERIAL PRIMARY KEY,
    name          VARCHAR(255) NOT NULL,
    country       VARCHAR(100) NOT NULL,
    city          VARCHAR(100) NOT NULL,
    is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE raw.inventory (
    inventory_id  SERIAL PRIMARY KEY,
    product_id    INT REFERENCES raw.products(product_id),
    warehouse_id  INT REFERENCES raw.warehouses(warehouse_id),
    quantity      INT NOT NULL DEFAULT 0,
    reserved_qty  INT NOT NULL DEFAULT 0,
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(product_id, warehouse_id)
);

CREATE TABLE raw.departments (
    department_id SERIAL PRIMARY KEY,
    name          VARCHAR(100) NOT NULL,
    manager_id    INT,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.employees (
    employee_id   SERIAL PRIMARY KEY,
    email         VARCHAR(255) NOT NULL UNIQUE,
    first_name    VARCHAR(100) NOT NULL,
    last_name     VARCHAR(100) NOT NULL,
    department_id INT REFERENCES raw.departments(department_id),
    hire_date     DATE,
    salary        NUMERIC(12,2),
    is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE raw.coupons (
    coupon_id     SERIAL PRIMARY KEY,
    code          VARCHAR(50) NOT NULL UNIQUE,
    discount_pct  NUMERIC(5,2),
    discount_amt  NUMERIC(12,2),
    valid_from    DATE,
    valid_until   DATE,
    max_uses      INT,
    times_used    INT DEFAULT 0,
    is_active     BOOLEAN DEFAULT TRUE
);

CREATE TABLE raw.orders (
    order_id      SERIAL PRIMARY KEY,
    customer_id   INT REFERENCES raw.customers(customer_id),
    employee_id   INT REFERENCES raw.employees(employee_id),
    order_date    TIMESTAMP NOT NULL DEFAULT NOW(),
    status        VARCHAR(30) NOT NULL DEFAULT 'pending',
    shipping_address_id INT REFERENCES raw.addresses(address_id),
    subtotal      NUMERIC(12,2),
    discount_amt  NUMERIC(12,2) DEFAULT 0,
    tax_amt       NUMERIC(12,2) DEFAULT 0,
    total_amt     NUMERIC(12,2),
    notes         TEXT
);
CREATE INDEX idx_raw_orders_customer ON raw.orders(customer_id);
CREATE INDEX idx_raw_orders_date     ON raw.orders(order_date);
CREATE INDEX idx_raw_orders_status   ON raw.orders(status);

CREATE TABLE raw.order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id      INT REFERENCES raw.orders(order_id),
    product_id    INT REFERENCES raw.products(product_id),
    quantity      INT NOT NULL,
    unit_price    NUMERIC(12,2) NOT NULL,
    discount_pct  NUMERIC(5,2) DEFAULT 0,
    line_total    NUMERIC(12,2)
);
CREATE INDEX idx_raw_order_items_order   ON raw.order_items(order_id);
CREATE INDEX idx_raw_order_items_product ON raw.order_items(product_id);

CREATE TABLE raw.payments (
    payment_id    SERIAL PRIMARY KEY,
    order_id      INT REFERENCES raw.orders(order_id),
    amount        NUMERIC(12,2) NOT NULL,
    method        VARCHAR(30) NOT NULL,
    status        VARCHAR(30) NOT NULL DEFAULT 'pending',
    gateway_ref   VARCHAR(255),
    paid_at       TIMESTAMP,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.shipments (
    shipment_id   SERIAL PRIMARY KEY,
    order_id      INT REFERENCES raw.orders(order_id),
    warehouse_id  INT REFERENCES raw.warehouses(warehouse_id),
    carrier       VARCHAR(100),
    tracking_no   VARCHAR(255),
    shipped_at    TIMESTAMP,
    delivered_at  TIMESTAMP,
    status        VARCHAR(30) DEFAULT 'pending'
);

CREATE TABLE raw.returns (
    return_id     SERIAL PRIMARY KEY,
    order_id      INT REFERENCES raw.orders(order_id),
    order_item_id INT REFERENCES raw.order_items(order_item_id),
    reason        VARCHAR(255),
    status        VARCHAR(30) DEFAULT 'pending',
    refund_amt    NUMERIC(12,2),
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE raw.reviews (
    review_id     SERIAL PRIMARY KEY,
    product_id    INT REFERENCES raw.products(product_id),
    customer_id   INT REFERENCES raw.customers(customer_id),
    rating        SMALLINT CHECK(rating BETWEEN 1 AND 5),
    title         VARCHAR(255),
    body          TEXT,
    is_verified   BOOLEAN DEFAULT FALSE,
    created_at    TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_raw_reviews_product ON raw.reviews(product_id);

CREATE TABLE raw.sessions (
    session_id    VARCHAR(64) PRIMARY KEY,
    customer_id   INT REFERENCES raw.customers(customer_id),
    started_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    ended_at      TIMESTAMP,
    device_type   VARCHAR(30),
    referrer      VARCHAR(512),
    ip_address    INET
);

CREATE TABLE raw.page_views (
    page_view_id  BIGSERIAL PRIMARY KEY,
    session_id    VARCHAR(64) REFERENCES raw.sessions(session_id),
    page_url      VARCHAR(2048) NOT NULL,
    viewed_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    time_on_page  INT,
    source        VARCHAR(100)
);
CREATE INDEX idx_raw_page_views_session ON raw.page_views(session_id);
CREATE INDEX idx_raw_page_views_url     ON raw.page_views(page_url);

CREATE TABLE raw.cart_items (
    cart_item_id  SERIAL PRIMARY KEY,
    session_id    VARCHAR(64) REFERENCES raw.sessions(session_id),
    customer_id   INT REFERENCES raw.customers(customer_id),
    product_id    INT REFERENCES raw.products(product_id),
    quantity      INT NOT NULL DEFAULT 1,
    added_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    removed_at    TIMESTAMP
);

CREATE TABLE raw.events (
    event_id      BIGSERIAL PRIMARY KEY,
    session_id    VARCHAR(64) REFERENCES raw.sessions(session_id),
    event_type    VARCHAR(100) NOT NULL,
    event_data    JSONB,
    occurred_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_raw_events_session ON raw.events(session_id);
CREATE INDEX idx_raw_events_type    ON raw.events(event_type);
CREATE INDEX idx_raw_events_ts      ON raw.events(occurred_at);
"""

DW_TABLES = """
-- ---- dw schema ---------------------------------------------------------

CREATE TABLE dw.dim_date (
    date_key      INT PRIMARY KEY,
    full_date     DATE NOT NULL,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    VARCHAR(20) NOT NULL,
    week          SMALLINT NOT NULL,
    day_of_month  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,
    day_name      VARCHAR(20) NOT NULL,
    is_weekend    BOOLEAN NOT NULL,
    is_holiday    BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE dw.dim_customer (
    customer_key  SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL,
    email         VARCHAR(255),
    full_name     VARCHAR(255),
    country       VARCHAR(100),
    city          VARCHAR(100),
    first_order_date DATE,
    customer_segment VARCHAR(50),
    valid_from    DATE NOT NULL,
    valid_to      DATE,
    is_current    BOOLEAN NOT NULL DEFAULT TRUE
);
CREATE INDEX idx_dw_dim_customer_id ON dw.dim_customer(customer_id);

CREATE TABLE dw.dim_product (
    product_key   SERIAL PRIMARY KEY,
    product_id    INT NOT NULL,
    sku           VARCHAR(100),
    product_name  VARCHAR(255),
    category_name VARCHAR(100),
    supplier_name VARCHAR(255),
    price         NUMERIC(12,2),
    cost          NUMERIC(12,2),
    margin_pct    NUMERIC(8,4),
    is_active     BOOLEAN,
    valid_from    DATE NOT NULL,
    valid_to      DATE,
    is_current    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dw.dim_supplier (
    supplier_key  SERIAL PRIMARY KEY,
    supplier_id   INT NOT NULL,
    name          VARCHAR(255),
    country       VARCHAR(100),
    valid_from    DATE NOT NULL,
    is_current    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dw.dim_category (
    category_key  SERIAL PRIMARY KEY,
    category_id   INT NOT NULL,
    name          VARCHAR(100),
    parent_name   VARCHAR(100),
    full_path     VARCHAR(500)
);

CREATE TABLE dw.dim_warehouse (
    warehouse_key SERIAL PRIMARY KEY,
    warehouse_id  INT NOT NULL,
    name          VARCHAR(255),
    country       VARCHAR(100),
    city          VARCHAR(100)
);

CREATE TABLE dw.dim_employee (
    employee_key  SERIAL PRIMARY KEY,
    employee_id   INT NOT NULL,
    full_name     VARCHAR(255),
    department    VARCHAR(100),
    hire_date     DATE,
    valid_from    DATE NOT NULL,
    is_current    BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE dw.dim_location (
    location_key  SERIAL PRIMARY KEY,
    country       VARCHAR(100),
    state         VARCHAR(100),
    city          VARCHAR(100),
    postal_code   VARCHAR(20)
);

CREATE TABLE dw.dim_coupon (
    coupon_key    SERIAL PRIMARY KEY,
    coupon_id     INT NOT NULL,
    code          VARCHAR(50),
    discount_type VARCHAR(20),
    discount_value NUMERIC(12,2),
    valid_from    DATE,
    valid_until   DATE
);

CREATE TABLE dw.fact_sales (
    sale_key        BIGSERIAL PRIMARY KEY,
    order_id        INT NOT NULL,
    order_item_id   INT NOT NULL,
    order_date_key  INT REFERENCES dw.dim_date(date_key),
    customer_key    INT REFERENCES dw.dim_customer(customer_key),
    product_key     INT REFERENCES dw.dim_product(product_key),
    employee_key    INT REFERENCES dw.dim_employee(employee_key),
    location_key    INT REFERENCES dw.dim_location(location_key),
    quantity        INT,
    unit_price      NUMERIC(12,2),
    discount_pct    NUMERIC(5,2),
    discount_amt    NUMERIC(12,2),
    line_total      NUMERIC(12,2),
    cost_total      NUMERIC(12,2),
    gross_margin    NUMERIC(12,2)
);
CREATE INDEX idx_dw_fact_sales_date     ON dw.fact_sales(order_date_key);
CREATE INDEX idx_dw_fact_sales_customer ON dw.fact_sales(customer_key);
CREATE INDEX idx_dw_fact_sales_product  ON dw.fact_sales(product_key);

CREATE TABLE dw.fact_inventory (
    inventory_key   BIGSERIAL PRIMARY KEY,
    snapshot_date_key INT REFERENCES dw.dim_date(date_key),
    product_key     INT REFERENCES dw.dim_product(product_key),
    warehouse_key   INT REFERENCES dw.dim_warehouse(warehouse_key),
    quantity_on_hand INT,
    quantity_reserved INT,
    quantity_available INT,
    stock_value     NUMERIC(14,2)
);

CREATE TABLE dw.fact_payments (
    payment_key     BIGSERIAL PRIMARY KEY,
    payment_id      INT NOT NULL,
    order_id        INT NOT NULL,
    payment_date_key INT REFERENCES dw.dim_date(date_key),
    customer_key    INT REFERENCES dw.dim_customer(customer_key),
    amount          NUMERIC(12,2),
    method          VARCHAR(30),
    status          VARCHAR(30)
);

CREATE TABLE dw.fact_events (
    event_key       BIGSERIAL PRIMARY KEY,
    event_id        BIGINT NOT NULL,
    event_date_key  INT REFERENCES dw.dim_date(date_key),
    customer_key    INT REFERENCES dw.dim_customer(customer_key),
    event_type      VARCHAR(100),
    session_count   INT DEFAULT 1
);

CREATE TABLE dw.fact_returns (
    return_key      BIGSERIAL PRIMARY KEY,
    return_id       INT NOT NULL,
    return_date_key INT REFERENCES dw.dim_date(date_key),
    customer_key    INT REFERENCES dw.dim_customer(customer_key),
    product_key     INT REFERENCES dw.dim_product(product_key),
    refund_amt      NUMERIC(12,2),
    reason          VARCHAR(255)
);

CREATE TABLE dw.bridge_order_coupon (
    bridge_key      BIGSERIAL PRIMARY KEY,
    order_id        INT NOT NULL,
    coupon_key      INT REFERENCES dw.dim_coupon(coupon_key),
    discount_applied NUMERIC(12,2)
);
"""

RPT_VIEWS = """
-- ---- rpt schema — views, mat views, and functions ---------------------

-- Simple join view: customer orders summary
CREATE VIEW rpt.v_customer_orders AS
SELECT
    c.customer_id,
    c.email,
    c.first_name || ' ' || c.last_name AS full_name,
    COUNT(DISTINCT o.order_id)          AS total_orders,
    SUM(o.total_amt)                    AS total_spent,
    MIN(o.order_date)                   AS first_order_date,
    MAX(o.order_date)                   AS last_order_date
FROM raw.customers c
LEFT JOIN raw.orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.email, c.first_name, c.last_name;

-- Product performance view with joins to categories and suppliers
CREATE VIEW rpt.v_product_performance AS
SELECT
    p.product_id,
    p.sku,
    p.name                              AS product_name,
    cat.name                            AS category_name,
    sup.name                            AS supplier_name,
    p.price,
    p.cost,
    ROUND((p.price - p.cost) / NULLIF(p.price, 0) * 100, 2) AS margin_pct,
    COALESCE(SUM(oi.quantity), 0)       AS units_sold,
    COALESCE(SUM(oi.line_total), 0)     AS revenue
FROM raw.products p
LEFT JOIN raw.categories    cat ON cat.category_id  = p.category_id
LEFT JOIN raw.suppliers     sup ON sup.supplier_id  = p.supplier_id
LEFT JOIN raw.order_items   oi  ON oi.product_id    = p.product_id
GROUP BY p.product_id, p.sku, p.name, cat.name, sup.name, p.price, p.cost;

-- CTE-based view: top customers by LTV with segmentation
CREATE VIEW rpt.v_customer_ltv AS
WITH order_totals AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS order_count,
        SUM(o.total_amt)           AS lifetime_value,
        AVG(o.total_amt)           AS avg_order_value
    FROM raw.orders o
    WHERE o.status NOT IN ('cancelled', 'refunded')
    GROUP BY o.customer_id
),
ranked AS (
    SELECT
        ot.customer_id,
        ot.order_count,
        ot.lifetime_value,
        ot.avg_order_value,
        NTILE(4) OVER (ORDER BY ot.lifetime_value DESC) AS ltv_quartile
    FROM order_totals ot
)
SELECT
    c.customer_id,
    c.email,
    c.first_name || ' ' || c.last_name AS full_name,
    r.order_count,
    r.lifetime_value,
    r.avg_order_value,
    CASE r.ltv_quartile
        WHEN 1 THEN 'VIP'
        WHEN 2 THEN 'High'
        WHEN 3 THEN 'Medium'
        ELSE       'Low'
    END AS ltv_segment
FROM ranked r
JOIN raw.customers c ON c.customer_id = r.customer_id;

-- Window function view: product sales rank within category
CREATE VIEW rpt.v_product_sales_rank AS
SELECT
    p.product_id,
    p.name                              AS product_name,
    cat.name                            AS category_name,
    COALESCE(SUM(oi.line_total), 0)     AS revenue,
    RANK() OVER (
        PARTITION BY cat.category_id
        ORDER BY COALESCE(SUM(oi.line_total), 0) DESC
    ) AS rank_in_category,
    ROW_NUMBER() OVER (
        ORDER BY COALESCE(SUM(oi.line_total), 0) DESC
    ) AS overall_rank
FROM raw.products p
LEFT JOIN raw.categories  cat ON cat.category_id = p.category_id
LEFT JOIN raw.order_items oi  ON oi.product_id   = p.product_id
GROUP BY p.product_id, p.name, cat.category_id, cat.name;

-- Window function view: monthly revenue trend with LAG
CREATE VIEW rpt.v_monthly_revenue AS
SELECT
    DATE_TRUNC('month', o.order_date)::DATE                     AS month,
    COUNT(DISTINCT o.order_id)                                   AS order_count,
    SUM(o.total_amt)                                             AS revenue,
    LAG(SUM(o.total_amt)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)) AS prev_month_revenue,
    SUM(o.total_amt)
        - LAG(SUM(o.total_amt)) OVER (ORDER BY DATE_TRUNC('month', o.order_date)) AS revenue_delta
FROM raw.orders o
WHERE o.status NOT IN ('cancelled')
GROUP BY DATE_TRUNC('month', o.order_date);

-- View referencing another view (multi-hop lineage): VIP customer orders
CREATE VIEW rpt.v_vip_customer_orders AS
SELECT
    vco.customer_id,
    vco.full_name,
    vco.total_orders,
    vco.total_spent,
    vltv.ltv_segment
FROM rpt.v_customer_orders    vco
JOIN rpt.v_customer_ltv       vltv ON vltv.customer_id = vco.customer_id
WHERE vltv.ltv_segment = 'VIP';

-- View referencing another view: top-ranked products in each category
CREATE VIEW rpt.v_top_products_per_category AS
SELECT
    category_name,
    product_id,
    product_name,
    revenue,
    rank_in_category
FROM rpt.v_product_sales_rank
WHERE rank_in_category <= 5;

-- Inventory health view (joins raw tables)
CREATE VIEW rpt.v_inventory_health AS
SELECT
    p.product_id,
    p.sku,
    p.name                                  AS product_name,
    w.name                                  AS warehouse_name,
    w.country                               AS warehouse_country,
    i.quantity,
    i.reserved_qty,
    i.quantity - i.reserved_qty             AS available_qty,
    CASE
        WHEN i.quantity - i.reserved_qty <= 0 THEN 'out_of_stock'
        WHEN i.quantity - i.reserved_qty < 10  THEN 'low_stock'
        ELSE 'in_stock'
    END AS stock_status
FROM raw.inventory i
JOIN raw.products   p ON p.product_id   = i.product_id
JOIN raw.warehouses w ON w.warehouse_id = i.warehouse_id;

-- Payment summary view
CREATE VIEW rpt.v_payment_summary AS
SELECT
    o.order_id,
    o.customer_id,
    o.total_amt,
    SUM(CASE WHEN py.status = 'completed' THEN py.amount ELSE 0 END) AS paid_amt,
    o.total_amt - SUM(CASE WHEN py.status = 'completed' THEN py.amount ELSE 0 END) AS outstanding_amt,
    COUNT(py.payment_id) AS payment_attempts,
    MAX(py.paid_at)      AS last_payment_date
FROM raw.orders   o
LEFT JOIN raw.payments py ON py.order_id = o.order_id
GROUP BY o.order_id, o.customer_id, o.total_amt;

-- Return rate by product
CREATE VIEW rpt.v_return_rate AS
SELECT
    p.product_id,
    p.name   AS product_name,
    COUNT(DISTINCT oi.order_item_id)                                AS items_sold,
    COUNT(DISTINCT r.return_id)                                     AS items_returned,
    ROUND(
        COUNT(DISTINCT r.return_id)::NUMERIC
        / NULLIF(COUNT(DISTINCT oi.order_item_id), 0) * 100, 2
    ) AS return_rate_pct
FROM raw.products     p
LEFT JOIN raw.order_items oi ON oi.product_id = p.product_id
LEFT JOIN raw.returns  r    ON r.order_item_id = oi.order_item_id
GROUP BY p.product_id, p.name;

-- Session funnel view with window functions
CREATE VIEW rpt.v_session_funnel AS
WITH session_events AS (
    SELECT
        s.session_id,
        s.customer_id,
        COUNT(DISTINCT pv.page_view_id) AS page_views,
        COUNT(DISTINCT ci.cart_item_id) AS cart_adds,
        COUNT(DISTINCT o.order_id)      AS orders_placed
    FROM raw.sessions    s
    LEFT JOIN raw.page_views  pv ON pv.session_id  = s.session_id
    LEFT JOIN raw.cart_items  ci ON ci.session_id  = s.session_id
    LEFT JOIN raw.orders      o  ON o.customer_id  = s.customer_id
    GROUP BY s.session_id, s.customer_id
)
SELECT
    se.session_id,
    se.customer_id,
    se.page_views,
    se.cart_adds,
    se.orders_placed,
    SUM(se.page_views) OVER ()   AS total_page_views,
    SUM(se.cart_adds)  OVER ()   AS total_cart_adds,
    SUM(se.orders_placed) OVER() AS total_orders
FROM session_events se;

-- Review summary (CTE + aggregate)
CREATE VIEW rpt.v_review_summary AS
WITH product_ratings AS (
    SELECT
        product_id,
        COUNT(*)                     AS review_count,
        AVG(rating)                  AS avg_rating,
        COUNT(*) FILTER (WHERE rating = 5) AS five_star_count
    FROM raw.reviews
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.name                      AS product_name,
    COALESCE(pr.review_count, 0) AS review_count,
    ROUND(COALESCE(pr.avg_rating, 0), 2) AS avg_rating,
    COALESCE(pr.five_star_count, 0) AS five_star_count
FROM raw.products      p
LEFT JOIN product_ratings pr ON pr.product_id = p.product_id;

-- Coupon effectiveness view
CREATE VIEW rpt.v_coupon_effectiveness AS
SELECT
    cp.coupon_id,
    cp.code,
    cp.discount_pct,
    cp.discount_amt,
    COUNT(DISTINCT o.order_id)  AS orders_using_coupon,
    SUM(o.discount_amt)         AS total_discount_given,
    SUM(o.total_amt)            AS total_revenue_with_coupon
FROM raw.coupons cp
LEFT JOIN raw.orders o ON o.notes ILIKE '%' || cp.code || '%'
GROUP BY cp.coupon_id, cp.code, cp.discount_pct, cp.discount_amt;

-- Employee performance: multi-level view referencing rpt.v_customer_orders
CREATE VIEW rpt.v_employee_performance AS
SELECT
    e.employee_id,
    e.first_name || ' ' || e.last_name AS employee_name,
    d.name                              AS department,
    COUNT(DISTINCT o.order_id)          AS orders_processed,
    SUM(o.total_amt)                    AS total_revenue,
    AVG(o.total_amt)                    AS avg_order_value
FROM raw.employees   e
JOIN raw.departments d ON d.department_id = e.department_id
LEFT JOIN raw.orders o ON o.employee_id   = e.employee_id
GROUP BY e.employee_id, e.first_name, e.last_name, d.name;

-- Materialized view: daily sales summary (for performance)
CREATE MATERIALIZED VIEW rpt.mv_daily_sales AS
SELECT
    o.order_date::DATE              AS sale_date,
    COUNT(DISTINCT o.order_id)      AS order_count,
    COUNT(DISTINCT o.customer_id)   AS unique_customers,
    SUM(oi.quantity)                AS units_sold,
    SUM(oi.line_total)              AS gross_revenue,
    SUM(o.discount_amt)             AS total_discounts,
    SUM(o.total_amt)                AS net_revenue
FROM raw.orders      o
JOIN raw.order_items oi ON oi.order_id = o.order_id
WHERE o.status NOT IN ('cancelled')
GROUP BY o.order_date::DATE;
CREATE UNIQUE INDEX idx_mv_daily_sales_date ON rpt.mv_daily_sales(sale_date);

-- Materialized view: product inventory snapshot referencing rpt.v_inventory_health
CREATE MATERIALIZED VIEW rpt.mv_inventory_snapshot AS
SELECT
    product_id,
    sku,
    product_name,
    SUM(quantity)        AS total_qty,
    SUM(available_qty)   AS total_available,
    COUNT(*)             AS warehouse_count,
    MAX(CASE WHEN stock_status = 'out_of_stock' THEN 1 ELSE 0 END) AS has_stockout
FROM rpt.v_inventory_health
GROUP BY product_id, sku, product_name;
CREATE UNIQUE INDEX idx_mv_inventory_product ON rpt.mv_inventory_snapshot(product_id);
"""

FUNCTIONS = """
-- ---- stored functions --------------------------------------------------

CREATE OR REPLACE FUNCTION rpt.fn_customer_order_stats(p_customer_id INT)
RETURNS TABLE(
    order_count     BIGINT,
    total_spent     NUMERIC,
    avg_order_value NUMERIC,
    first_order     TIMESTAMP,
    last_order      TIMESTAMP
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        COUNT(o.order_id)::BIGINT,
        SUM(o.total_amt),
        AVG(o.total_amt),
        MIN(o.order_date),
        MAX(o.order_date)
    FROM raw.orders o
    WHERE o.customer_id = p_customer_id
      AND o.status NOT IN ('cancelled');
END;
$$;

CREATE OR REPLACE FUNCTION rpt.fn_product_revenue_rank(p_category_id INT)
RETURNS TABLE(
    product_id   INT,
    product_name VARCHAR,
    revenue      NUMERIC,
    rank_pos     BIGINT
) LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        p.product_id,
        p.name,
        COALESCE(SUM(oi.line_total), 0)::NUMERIC,
        RANK() OVER (ORDER BY COALESCE(SUM(oi.line_total), 0) DESC)
    FROM raw.products   p
    LEFT JOIN raw.order_items oi ON oi.product_id = p.product_id
    WHERE p.category_id = p_category_id
    GROUP BY p.product_id, p.name;
END;
$$;
"""


# ---------------------------------------------------------------------------
# Main logic
# ---------------------------------------------------------------------------

def run(conn: PgConnection) -> None:
    cur = conn.cursor()

    print("Dropping and recreating schemas...")
    for schema in ("rpt", "dw", "raw"):
        cur.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    for schema in ("raw", "dw", "rpt"):
        cur.execute(f"CREATE SCHEMA {schema}")
    conn.commit()

    print("Creating raw tables (20)...")
    cur.execute(RAW_TABLES)
    conn.commit()

    print("Creating dw tables (15)...")
    cur.execute(DW_TABLES)
    conn.commit()

    print("Creating rpt views, materialized views, and functions...")
    cur.execute(RPT_VIEWS)
    cur.execute(FUNCTIONS)
    conn.commit()

    # Summary counts
    cur.execute("""
        SELECT table_schema, table_type, COUNT(*)
        FROM information_schema.tables
        WHERE table_schema IN ('raw','dw','rpt')
        GROUP BY table_schema, table_type
        ORDER BY table_schema, table_type
    """)
    rows = cur.fetchall()

    cur.execute("""
        SELECT COUNT(*) FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'rpt'
    """)
    func_count = cur.fetchone()[0]

    cur.close()

    print("\n=== Seed Summary ===")
    schema_names = set()
    table_count = view_count = matview_count = 0
    for schema, ttype, cnt in rows:
        schema_names.add(schema)
        if ttype == "BASE TABLE":
            table_count += cnt
        elif ttype == "VIEW":
            view_count += cnt
    # mat views show up in pg_matviews, not information_schema.tables with VIEW type
    print(f"  Schemas: {len(schema_names)} ({', '.join(sorted(schema_names))})")
    print(f"  Tables:  {table_count}")
    print(f"  Views:   {view_count}")
    print(f"  Materialized views: 2 (mv_daily_sales, mv_inventory_snapshot)")
    print(f"  Functions: {func_count}")
    print("Done.\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed the lineage-postgres test database.")
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     type=int, default=5433)
    parser.add_argument("--dbname",   default="lineage_sample")
    parser.add_argument("--user",     default="lineage")
    parser.add_argument("--password", default="lineage")
    args = parser.parse_args()

    dsn = f"host={args.host} port={args.port} dbname={args.dbname} user={args.user} password={args.password}"
    print(f"Connecting to PostgreSQL at {args.host}:{args.port}/{args.dbname}...")
    try:
        conn = psycopg2.connect(dsn)
    except Exception as exc:
        print(f"Connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
