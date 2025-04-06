-- Drop tables in correct order to handle foreign key constraints
DROP TABLE IF EXISTS sales_team_incentives;
DROP TABLE IF EXISTS customer_monthly_purchases;
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS product;
DROP TABLE IF EXISTS store;
DROP TABLE IF EXISTS customer;

-- Create tables in correct order
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

-- Insert data
INSERT INTO customer (id, name, email) VALUES
(1, 'John Doe', 'john.doe@example.com'),
(2, 'Jane Smith', 'jane.smith@example.com'),
(3, 'Robert Johnson', 'robert.johnson@example.com'),
(4, 'Emily Davis', 'emily.davis@example.com'),
(5, 'Michael Wilson', 'michael.wilson@example.com'),
(6, 'Sarah Brown', 'sarah.brown@example.com'),
(7, 'David Miller', 'david.miller@example.com'),
(8, 'Lisa Anderson', 'lisa.anderson@example.com'),
(9, 'James Taylor', 'james.taylor@example.com'),
(10, 'Patricia Moore', 'patricia.moore@example.com');

INSERT INTO store (id, name, location) VALUES
(121, 'Downtown Store', '123 Main Street'),
(122, 'Mall Store', '456 Shopping Center'),
(123, 'Suburban Store', '789 Oak Avenue');

INSERT INTO product (id, name, price) VALUES
(1, 'quaker oats', 100.00),
(2, 'refined oil', 120.00),
(3, 'sugar', 150.00),
(4, 'flour', 80.00);

INSERT INTO sales (customer_id, store_id, product_name, sales_date, sales_person_id, price, quantity, total_cost) VALUES
(1, 121, 'quaker oats', '2025-04-03', 8, 191.9, 2, 383.8),
(2, 121, 'refined oil', '2025-04-02', 2, 122.76, 1, 122.76),
(3, 121, 'refined oil', '2025-04-01', 6, 11.78, 7, 82.46),
(4, 122, 'refined oil', '2025-03-31', 3, 165.38, 2, 330.76),
(5, 123, 'sugar', '2025-03-30', 2, 166.12, 8, 1328.96),
(6, 123, 'refined oil', '2025-03-29', 9, 125.61, 8, 1004.88),
(7, 121, 'quaker oats', '2025-03-28', 9, 131.49, 6, 788.94),
(8, 121, 'sugar', '2025-03-27', 4, 60.24, 6, 361.44),
(9, 123, 'flour', '2025-03-26', 4, 32.18, 4, 128.72),
(10, 122, 'flour', '2025-03-25', 9, 55.81, 2, 111.62);

INSERT INTO customer_monthly_purchases (customer_id, purchase_month, total_purchases, purchase_count, avg_purchase_amount) VALUES
(1, '2025-04', 383.8, 1, 383.8),
(2, '2025-04', 122.76, 1, 122.76),
(3, '2025-04', 82.46, 1, 82.46),
(4, '2025-03', 330.76, 1, 330.76),
(5, '2025-03', 1328.96, 1, 1328.96);

INSERT INTO sales_team_incentives (sales_person_id, sales_month, total_sales, transaction_count, avg_transaction_value) VALUES
(8, '2025-04', 383.8, 1, 383.8),
(2, '2025-04', 122.76, 1, 122.76),
(6, '2025-04', 82.46, 1, 82.46),
(3, '2025-03', 330.76, 1, 330.76),
(9, '2025-03', 1004.88, 1, 1004.88); 