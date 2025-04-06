-- Drop tables if they exist
DROP TABLE IF EXISTS sales_team_incentives;
DROP TABLE IF EXISTS customer_monthly_purchases;
DROP TABLE IF EXISTS store_performance;
DROP TABLE IF EXISTS product_performance;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS customer;

-- Create tables
CREATE TABLE IF NOT EXISTS customer (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS store (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    location VARCHAR(255) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sales (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    store_id INT NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    sales_date DATE NOT NULL,
    sales_person_id INT NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INT NOT NULL,
    total_cost DECIMAL(10, 2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES customer(id),
    FOREIGN KEY (store_id) REFERENCES store(id)
);

CREATE TABLE IF NOT EXISTS customer_monthly_purchases (
    customer_id INT NOT NULL,
    purchase_month VARCHAR(7) NOT NULL,
    total_purchases DECIMAL(10, 2) NOT NULL,
    purchase_count INT NOT NULL,
    avg_purchase_amount DECIMAL(10, 2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (customer_id, purchase_month),
    FOREIGN KEY (customer_id) REFERENCES customer(id)
);

CREATE TABLE IF NOT EXISTS sales_team_incentives (
    sales_person_id INT NOT NULL,
    sales_month VARCHAR(7) NOT NULL,
    total_sales DECIMAL(10, 2) NOT NULL,
    transaction_count INT NOT NULL,
    avg_transaction_value DECIMAL(10, 2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (sales_person_id, sales_month)
);

CREATE TABLE IF NOT EXISTS store_performance (
    store_id INT NOT NULL,
    sales_month VARCHAR(7) NOT NULL,
    total_sales DECIMAL(10, 2) NOT NULL,
    transaction_count INT NOT NULL,
    avg_transaction_value DECIMAL(10, 2) NOT NULL,
    unique_customers INT NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id, sales_month),
    FOREIGN KEY (store_id) REFERENCES store(id)
);

CREATE TABLE IF NOT EXISTS product_performance (
    product_name VARCHAR(255) NOT NULL,
    sales_month VARCHAR(7) NOT NULL,
    total_sales DECIMAL(10, 2) NOT NULL,
    total_quantity INT NOT NULL,
    transaction_count INT NOT NULL,
    avg_price DECIMAL(10, 2) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_name, sales_month)
);

-- Add more table creation scripts as needed 