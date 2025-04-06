-- Insert sample customers
INSERT INTO customer (id, name, email) VALUES
(1, 'John Doe', 'john@example.com'),
(2, 'Jane Smith', 'jane@example.com'),
(3, 'Bob Johnson', 'bob@example.com'),
(4, 'Alice Brown', 'alice@example.com'),
(5, 'Charlie Davis', 'charlie@example.com'),
(6, 'Diana Wilson', 'diana@example.com'),
(7, 'Edward Miller', 'edward@example.com'),
(8, 'Fiona Clark', 'fiona@example.com'),
(9, 'George White', 'george@example.com'),
(10, 'Helen Green', 'helen@example.com');

-- Insert sample stores
INSERT INTO store (id, name, location) VALUES
(121, 'Downtown Store', 'Main Street'),
(122, 'Uptown Store', 'North Avenue'),
(123, 'Westside Store', 'West Boulevard'),
(124, 'Eastside Store', 'East Road'),
(125, 'Central Store', 'Central Square');

-- Insert sample products
INSERT INTO product (id, name, price) VALUES
(1, 'flour', 150.33),
(2, 'sugar', 130.56),
(3, 'rice', 120.83),
(4, 'salt', 118.70),
(5, 'oil', 145.90);

-- Insert into sales table
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

-- Insert into customer_monthly_purchases (sample data for first customer)
INSERT INTO customer_monthly_purchases (customer_id, purchase_month, total_purchases, purchase_count, avg_purchase_amount) VALUES
(1, '2025-04', 383.8, 1, 383.8),
(2, '2025-04', 122.76, 1, 122.76),
(3, '2025-04', 82.46, 1, 82.46),
(4, '2025-03', 330.76, 1, 330.76),
(5, '2025-03', 1328.96, 1, 1328.96);

-- Insert into sales_team_incentives (sample data for first few sales persons)
INSERT INTO sales_team_incentives (sales_person_id, sales_month, total_sales, transaction_count, avg_transaction_value) VALUES
(8, '2025-04', 383.8, 1, 383.8),
(2, '2025-04', 122.76, 1, 122.76),
(6, '2025-04', 82.46, 1, 82.46),
(3, '2025-03', 330.76, 1, 330.76),
(9, '2025-03', 1004.88, 1, 1004.88); 