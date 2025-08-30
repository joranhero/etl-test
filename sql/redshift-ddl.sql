CREATE TABLE fact_order (
    order_id VARCHAR(64) PRIMARY KEY,
    user_id VARCHAR(64),
    amount DECIMAL(12,2),
    currency VARCHAR(3),
    created_at TIMESTAMP,
    sku VARCHAR(64),
    qty INTEGER,
    price DECIMAL(12,2),
    metadata VARCHAR(MAX), -- para almacenar JSON serializado
    email VARCHAR(255),
    country VARCHAR(8),
    product_name VARCHAR(255),
    category VARCHAR(100)
);
