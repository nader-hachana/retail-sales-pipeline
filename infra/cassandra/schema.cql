CREATE KEYSPACE IF NOT EXISTS sales WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS sales.raw_sales (
    sku text,
    invoice_date timestamp,
    invoice_id text,
    description text,
    quantity int,
    unit_price double,
    customer_id int,
    country text,
    PRIMARY KEY ((sku), invoice_date, invoice_id)
);

CREATE TABLE IF NOT EXISTS sales.raw_products (
    sku text,
    description text,
    min_price double,
    max_price double,
    first_seen timestamp,
    last_seen timestamp,
    PRIMARY KEY (sku)
);

CREATE TABLE IF NOT EXISTS sales.raw_calendar (
    date date,
    year int,
    month int,
    day int,
    week_of_year int,
    day_of_week text,
    is_weekend boolean,
    PRIMARY KEY (date)
);

CREATE KEYSPACE IF NOT EXISTS sales_metrics WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 };

CREATE TABLE IF NOT EXISTS sales_metrics.weekly_sales (
    sku text,
    week timestamp,
    weekly_sales bigint,
    had_promo boolean,
    PRIMARY KEY ((sku), week)
);

CREATE TABLE IF NOT EXISTS sales_metrics.baseline (
    sku text,
    baseline double,
    PRIMARY KEY (sku)
);

CREATE TABLE IF NOT EXISTS sales_metrics.lift (
    sku text,
    week timestamp,
    weekly_sales bigint,
    had_promo boolean,
    baseline double,
    lift_unit double,
    lift_percentage double,
    PRIMARY KEY ((sku), week)
);

CREATE TABLE IF NOT EXISTS sales_metrics.sales_error (
    sku text,
    week timestamp,
    weekly_sales bigint,
    had_promo boolean,
    forecast double,
    error double,
    PRIMARY KEY ((sku), week)
);

CREATE TABLE IF NOT EXISTS sales_metrics.absolute_diff (
    sku text,
    week timestamp,
    weekly_sales bigint,
    had_promo boolean,
    forecast double,
    absolute_diff double,
    PRIMARY KEY ((sku), week)
);

CREATE TABLE IF NOT EXISTS sales_metrics.profit_error (
    sku text,
    week timestamp,
    weekly_sales bigint,
    had_promo boolean,
    forecast double,
    reference_price double,
    profit_error double,
    PRIMARY KEY ((sku), week)
);

CREATE TABLE IF NOT EXISTS sales_metrics.price_history (
    sku text,
    price_history list<frozen<tuple<timestamp, double>>>,
    PRIMARY KEY (sku)
);
