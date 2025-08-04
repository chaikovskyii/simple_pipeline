CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

DROP TABLE IF EXISTS orders_eur CASCADE;
DROP VIEW IF EXISTS data_quality_metrics CASCADE;

CREATE TABLE orders_eur (
    order_id UUID PRIMARY KEY,
    customer_email VARCHAR(255) NOT NULL,
    order_date TIMESTAMP NOT NULL,
    amount DECIMAL(10, 2) NOT NULL CHECK (amount > 0),
    currency CHAR(3) NOT NULL DEFAULT 'EUR'
);

CREATE INDEX IF NOT EXISTS idx_orders_eur_order_date ON orders_eur(order_date);
CREATE INDEX IF NOT EXISTS idx_orders_eur_currency ON orders_eur(currency);
CREATE INDEX IF NOT EXISTS idx_orders_eur_customer_email ON orders_eur(customer_email);

GRANT ALL PRIVILEGES ON TABLE orders_eur TO postgres;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO postgres;