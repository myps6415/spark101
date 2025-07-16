-- PostgreSQL 初始化腳本
-- 為 Spark101 高級練習創建示例數據庫和表

-- 創建數據庫（如果不存在）
-- 註：CREATE DATABASE 只能在 docker-entrypoint-initdb.d 中執行

-- 創建示例表
CREATE TABLE IF NOT EXISTS employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    age INTEGER NOT NULL,
    department VARCHAR(50) NOT NULL,
    salary DECIMAL(10, 2) NOT NULL,
    hire_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入示例數據
INSERT INTO employees (name, age, department, salary, hire_date) VALUES
    ('張三', 28, '工程部', 75000.00, '2022-01-15'),
    ('李四', 32, '產品部', 85000.00, '2021-08-20'),
    ('王五', 29, '設計部', 68000.00, '2022-03-10'),
    ('趙六', 35, '銷售部', 72000.00, '2020-11-05'),
    ('孫七', 26, '工程部', 65000.00, '2023-01-20'),
    ('周八', 31, '產品部', 90000.00, '2021-05-15'),
    ('吳九', 27, '設計部', 70000.00, '2022-09-08'),
    ('鄭十', 33, '銷售部', 78000.00, '2020-12-12'),
    ('朱十一', 30, '工程部', 82000.00, '2021-07-25'),
    ('林十二', 34, '產品部', 88000.00, '2021-02-18');

-- 創建銷售數據表
CREATE TABLE IF NOT EXISTS sales (
    id SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL,
    product_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    quantity INTEGER NOT NULL,
    sale_date DATE NOT NULL,
    region VARCHAR(50) NOT NULL,
    salesperson VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入銷售示例數據
INSERT INTO sales (product_id, product_name, category, price, quantity, sale_date, region, salesperson) VALUES
    (1, '筆記本電腦', '電腦', 25000.00, 2, '2024-01-10', '台北', '張三'),
    (2, '手機', '電子產品', 15000.00, 1, '2024-01-12', '台中', '李四'),
    (3, '平板', '電子產品', 12000.00, 1, '2024-01-15', '高雄', '王五'),
    (4, '鍵盤', '配件', 2500.00, 3, '2024-01-18', '台北', '趙六'),
    (5, '滑鼠', '配件', 1200.00, 5, '2024-01-20', '台中', '孫七'),
    (1, '筆記本電腦', '電腦', 25000.00, 1, '2024-01-22', '高雄', '周八'),
    (2, '手機', '電子產品', 15000.00, 2, '2024-01-25', '台北', '吳九'),
    (6, '耳機', '配件', 3500.00, 2, '2024-01-28', '台中', '鄭十'),
    (3, '平板', '電子產品', 12000.00, 1, '2024-02-01', '高雄', '朱十一'),
    (7, '充電器', '配件', 800.00, 4, '2024-02-05', '台北', '林十二');

-- 創建用戶行為日誌表
CREATE TABLE IF NOT EXISTS user_logs (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL,
    action VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    ip_address INET,
    user_agent TEXT,
    page_url TEXT,
    session_id VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 插入用戶行為示例數據
INSERT INTO user_logs (user_id, action, timestamp, ip_address, user_agent, page_url, session_id) VALUES
    (1, 'login', '2024-01-10 09:00:00', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '/login', 'sess_001'),
    (1, 'view_product', '2024-01-10 09:05:00', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '/product/1', 'sess_001'),
    (1, 'add_to_cart', '2024-01-10 09:10:00', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '/cart/add', 'sess_001'),
    (2, 'login', '2024-01-10 10:00:00', '192.168.1.101', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '/login', 'sess_002'),
    (2, 'search', '2024-01-10 10:05:00', '192.168.1.101', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '/search?q=phone', 'sess_002'),
    (3, 'register', '2024-01-10 11:00:00', '192.168.1.102', 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)', '/register', 'sess_003'),
    (3, 'view_product', '2024-01-10 11:15:00', '192.168.1.102', 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X)', '/product/2', 'sess_003'),
    (1, 'checkout', '2024-01-10 09:20:00', '192.168.1.100', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)', '/checkout', 'sess_001'),
    (2, 'view_product', '2024-01-10 10:15:00', '192.168.1.101', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', '/product/3', 'sess_002'),
    (4, 'login', '2024-01-10 14:00:00', '192.168.1.103', 'Mozilla/5.0 (X11; Linux x86_64)', '/login', 'sess_004');

-- 創建索引以提高查詢性能
CREATE INDEX IF NOT EXISTS idx_employees_department ON employees(department);
CREATE INDEX IF NOT EXISTS idx_employees_hire_date ON employees(hire_date);
CREATE INDEX IF NOT EXISTS idx_sales_product_id ON sales(product_id);
CREATE INDEX IF NOT EXISTS idx_sales_sale_date ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_region ON sales(region);
CREATE INDEX IF NOT EXISTS idx_user_logs_user_id ON user_logs(user_id);
CREATE INDEX IF NOT EXISTS idx_user_logs_timestamp ON user_logs(timestamp);
CREATE INDEX IF NOT EXISTS idx_user_logs_action ON user_logs(action);

-- 創建視圖
CREATE OR REPLACE VIEW employee_summary AS
SELECT 
    department,
    COUNT(*) as employee_count,
    AVG(salary) as avg_salary,
    MIN(salary) as min_salary,
    MAX(salary) as max_salary
FROM employees
GROUP BY department;

CREATE OR REPLACE VIEW sales_summary AS
SELECT 
    DATE_TRUNC('month', sale_date) as month,
    region,
    category,
    SUM(price * quantity) as total_revenue,
    COUNT(*) as transaction_count
FROM sales
GROUP BY DATE_TRUNC('month', sale_date), region, category;

-- 輸出完成信息
SELECT 'PostgreSQL 數據庫初始化完成！' AS message;
SELECT 'Employees 表記錄數：' || COUNT(*) AS employees_count FROM employees;
SELECT 'Sales 表記錄數：' || COUNT(*) AS sales_count FROM sales;
SELECT 'User Logs 表記錄數：' || COUNT(*) AS user_logs_count FROM user_logs;