CREATE SCHEMA IF NOT EXISTS sales_lab;

CREATE TABLE IF NOT EXISTS sales_lab.sales_transactions (
    transaction_id BIGINT PRIMARY KEY,
    transaction_date DATE NOT NULL,
    branch VARCHAR(80) NOT NULL,
    category VARCHAR(80) NOT NULL,
    product_name VARCHAR(120) NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price NUMERIC(14,2) NOT NULL CHECK (unit_price >= 0),
    discount_rate NUMERIC(5,4) NOT NULL CHECK (discount_rate >= 0 AND discount_rate <= 1),
    gross_sales NUMERIC(16,2) NOT NULL CHECK (gross_sales >= 0),
    net_sales NUMERIC(16,2) NOT NULL CHECK (net_sales >= 0),
    payment_method VARCHAR(40) NOT NULL,
    customer_segment VARCHAR(40) NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sales_date ON sales_lab.sales_transactions(transaction_date);
CREATE INDEX IF NOT EXISTS idx_sales_branch ON sales_lab.sales_transactions(branch);
CREATE INDEX IF NOT EXISTS idx_sales_category ON sales_lab.sales_transactions(category);

CREATE TABLE IF NOT EXISTS sales_lab.etl_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    job_name VARCHAR(100) NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    input_rows BIGINT,
    output_rows BIGINT,
    status VARCHAR(20) NOT NULL,
    details TEXT
);

-- Opsional: buat role baca untuk analisis; ganti password yang kuat.
-- CREATE ROLE sales_analyst LOGIN PASSWORD 'GANTI_PASSWORD_KUAT';
-- GRANT CONNECT ON DATABASE big_data_sales_lab TO sales_analyst;
-- GRANT USAGE ON SCHEMA sales_lab TO sales_analyst;
-- GRANT SELECT ON ALL TABLES IN SCHEMA sales_lab TO sales_analyst;
