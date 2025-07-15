# 第4章練習：Spark SQL 進階操作

## 練習目標
深入學習 Spark SQL 的進階查詢技巧、窗口函數、複雜分析，以及與 DataFrame API 的結合使用。

## 練習1：基本 SQL 查詢和視圖管理

### 任務描述
學習使用 Spark SQL 進行基本查詢操作，創建和管理臨時視圖。

### 要求
1. 創建臨時視圖和全局臨時視圖
2. 執行基本 SQL 查詢（SELECT, WHERE, GROUP BY）
3. 使用子查詢和 CTE（Common Table Expressions）
4. 實現複雜的 JOIN 操作
5. 管理視圖的生命週期

### 程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("Spark SQL基本操作") \
    .master("local[*]") \
    .getOrCreate()

# 員工數據
employees_data = [
    (1, "John Doe", "Engineering", 75000, "Manager", "2020-01-15"),
    (2, "Alice Smith", "Marketing", 65000, "Analyst", "2021-03-10"),
    (3, "Bob Johnson", "Engineering", 80000, "Senior Engineer", "2019-06-20"),
    (4, "Charlie Brown", "Sales", 60000, "Representative", "2022-01-05"),
    (5, "Diana Wilson", "HR", 70000, "Specialist", "2020-11-30"),
    (6, "Eve Davis", "Engineering", 85000, "Lead Engineer", "2018-09-15"),
    (7, "Frank Miller", "Marketing", 58000, "Coordinator", "2021-08-22"),
    (8, "Grace Lee", "Sales", 72000, "Manager", "2019-12-01"),
    (9, "Henry Taylor", "Engineering", 78000, "Engineer", "2020-05-18"),
    (10, "Ivy Chen", "HR", 68000, "Generalist", "2021-02-14")
]

employees_columns = ["emp_id", "name", "department", "salary", "position", "hire_date"]

# 部門數據
departments_data = [
    ("Engineering", "John Doe", "Tech Tower", 50),
    ("Marketing", "Grace Lee", "Marketing Plaza", 25),
    ("Sales", "Charlie Brown", "Sales Center", 30),
    ("HR", "Diana Wilson", "Admin Building", 15)
]

departments_columns = ["dept_name", "manager", "location", "budget"]

# 專案數據
projects_data = [
    (101, "Web Platform", "Engineering", "2024-01-01", "2024-06-30"),
    (102, "Mobile App", "Engineering", "2024-02-15", "2024-08-15"),
    (103, "Marketing Campaign", "Marketing", "2024-01-10", "2024-04-10"),
    (104, "Sales System", "Sales", "2024-03-01", "2024-09-01")
]

projects_columns = ["project_id", "project_name", "department", "start_date", "end_date"]

# 創建 DataFrame
employees_df = spark.createDataFrame(employees_data, employees_columns)
departments_df = spark.createDataFrame(departments_data, departments_columns)
projects_df = spark.createDataFrame(projects_data, projects_columns)

# 完成以下任務：
# 1. 創建臨時視圖
# 2. 基本 SQL 查詢
# 3. 複雜 JOIN 查詢
# 4. 使用 CTE
# 5. 子查詢操作

# 你的程式碼在這裡

spark.stop()
```

### 預期輸出
- 各種 SQL 查詢結果
- JOIN 操作的結果
- CTE 和子查詢的使用示例

## 練習2：窗口函數和分析函數

### 任務描述
學習使用 Spark SQL 的窗口函數進行複雜的分析計算。

### 要求
1. 使用 ROW_NUMBER, RANK, DENSE_RANK
2. 實現累計和移動平均計算
3. 使用 LAG/LEAD 進行時間序列分析
4. 計算百分位數和分佈統計
5. 創建自定義窗口規範

### 程式碼模板

```python
# 銷售數據
sales_data = [
    ("2024-01-01", "North", "Alice", "Product A", 1000),
    ("2024-01-01", "South", "Bob", "Product A", 1200),
    ("2024-01-01", "East", "Charlie", "Product B", 800),
    ("2024-01-02", "North", "Alice", "Product A", 1100),
    ("2024-01-02", "South", "Bob", "Product B", 900),
    ("2024-01-02", "West", "Diana", "Product A", 1300),
    ("2024-01-03", "North", "Alice", "Product B", 950),
    ("2024-01-03", "South", "Bob", "Product A", 1250),
    ("2024-01-03", "East", "Charlie", "Product A", 1050),
    ("2024-01-03", "West", "Diana", "Product B", 1150)
]

sales_columns = ["sale_date", "region", "salesperson", "product", "amount"]
sales_df = spark.createDataFrame(sales_data, sales_columns)

# 創建臨時視圖
sales_df.createOrReplaceTempView("sales")

# 完成以下任務：
# 1. 銷售排名分析
# 2. 累計銷售額計算
# 3. 移動平均和趨勢分析
# 4. 同期比較分析
# 5. 百分位數統計

# 你的 SQL 查詢在這裡
```

## 練習3：複雜數據分析和報表

### 任務描述
使用 Spark SQL 創建複雜的業務報表和分析查詢。

### 要求
1. 創建多維度數據透視表
2. 實現層次化數據分析
3. 計算業務 KPI 指標
4. 生成時間序列報告
5. 創建動態查詢和參數化報表

### 程式碼模板

```python
# 訂單明細數據
order_details = [
    ("O001", "2024-01-15", "C001", "John", "North", "Electronics", "Laptop", 2, 1200.00),
    ("O001", "2024-01-15", "C001", "John", "North", "Electronics", "Mouse", 1, 25.00),
    ("O002", "2024-01-16", "C002", "Alice", "South", "Books", "Python Guide", 3, 45.00),
    ("O003", "2024-01-17", "C003", "Bob", "East", "Electronics", "Keyboard", 2, 75.00),
    ("O004", "2024-01-18", "C001", "John", "North", "Clothing", "T-Shirt", 5, 20.00),
    ("O005", "2024-01-19", "C004", "Charlie", "West", "Electronics", "Monitor", 1, 300.00),
    ("O006", "2024-01-20", "C002", "Alice", "South", "Electronics", "Tablet", 1, 400.00),
    ("O007", "2024-01-21", "C005", "Diana", "North", "Books", "Java Manual", 2, 50.00),
    ("O008", "2024-01-22", "C003", "Bob", "East", "Clothing", "Jeans", 3, 60.00),
    ("O009", "2024-01-23", "C006", "Eve", "West", "Electronics", "Phone", 1, 800.00)
]

order_columns = ["order_id", "order_date", "customer_id", "customer_name", 
                "region", "category", "product", "quantity", "unit_price"]

orders_df = spark.createDataFrame(order_details, order_columns)
orders_df.createOrReplaceTempView("orders")

# 完成以下任務：
# 1. 銷售數據透視表
# 2. 客戶 RFM 分析
# 3. 產品績效分析
# 4. 地區比較報告
# 5. 時間趨勢分析

# 你的程式碼在這裡
```

## 練習4：數據倉庫操作和 ETL

### 任務描述
學習使用 Spark SQL 進行數據倉庫相關的操作，包括 ETL 流程。

### 要求
1. 實現 SCD（Slowly Changing Dimensions）
2. 創建事實表和維度表
3. 執行數據清洗和轉換
4. 實現增量數據載入
5. 創建數據質量檢查

### 程式碼模板

```python
# 客戶維度數據（包含歷史變更）
customer_history = [
    (1, "John Doe", "john@email.com", "New York", "Premium", "2023-01-01", None, True),
    (1, "John Doe", "john.doe@email.com", "New York", "Premium", "2024-01-01", "2023-12-31", False),
    (2, "Alice Smith", "alice@email.com", "Los Angeles", "Standard", "2023-06-15", None, True),
    (3, "Bob Johnson", "bob@email.com", "Chicago", "Premium", "2023-03-10", "2023-12-31", False),
    (3, "Bob Johnson", "bob@email.com", "Chicago", "VIP", "2024-01-01", None, True)
]

customer_columns = ["customer_id", "name", "email", "city", "tier", "effective_date", "end_date", "is_current"]

# 事實表數據
fact_sales = [
    (1, 1, 101, "2024-01-15", 2, 1200.00),
    (2, 2, 102, "2024-01-16", 1, 45.00),
    (3, 3, 103, "2024-01-17", 2, 75.00),
    (4, 1, 104, "2024-01-18", 5, 20.00),
    (5, 2, 105, "2024-01-19", 1, 300.00)
]

fact_columns = ["sale_id", "customer_id", "product_id", "sale_date", "quantity", "amount"]

# 完成以下任務：
# 1. 實現 SCD Type 2
# 2. 創建星型架構
# 3. 數據質量檢查
# 4. 增量更新邏輯
# 5. ETL 流程自動化

# 你的程式碼在這裡
```

## 練習答案

### 練習1解答

```sql
-- 1. 創建臨時視圖
employees_df.createOrReplaceTempView("employees")
departments_df.createOrReplaceTempView("departments")
projects_df.createOrReplaceTempView("projects")

-- 2. 基本查詢
print("部門薪資統計:")
result1 = spark.sql("""
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary
    FROM employees
    GROUP BY department
    ORDER BY avg_salary DESC
""")
result1.show()

-- 3. 複雜 JOIN 查詢
print("員工部門完整信息:")
result2 = spark.sql("""
    SELECT 
        e.name,
        e.position,
        e.salary,
        d.dept_name,
        d.location,
        d.manager as dept_manager
    FROM employees e
    JOIN departments d ON e.department = d.dept_name
    ORDER BY e.salary DESC
""")
result2.show()

-- 4. 使用 CTE
print("高薪員工和部門預算對比:")
result3 = spark.sql("""
    WITH high_salary_employees AS (
        SELECT department, COUNT(*) as high_earners
        FROM employees
        WHERE salary > 70000
        GROUP BY department
    ),
    dept_summary AS (
        SELECT 
            dept_name,
            budget,
            budget / 1000 as budget_k
        FROM departments
    )
    SELECT 
        ds.dept_name,
        ds.budget_k,
        COALESCE(hse.high_earners, 0) as high_earners
    FROM dept_summary ds
    LEFT JOIN high_salary_employees hse ON ds.dept_name = hse.department
""")
result3.show()

-- 5. 子查詢操作
print("薪資高於部門平均的員工:")
result4 = spark.sql("""
    SELECT 
        e1.name,
        e1.department,
        e1.salary,
        dept_avg.avg_salary
    FROM employees e1
    JOIN (
        SELECT 
            department, 
            AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
    ) dept_avg ON e1.department = dept_avg.department
    WHERE e1.salary > dept_avg.avg_salary
    ORDER BY e1.department, e1.salary DESC
""")
result4.show()
```

### 練習2解答

```sql
-- 1. 銷售排名分析
print("每日各地區銷售排名:")
rank_query = spark.sql("""
    SELECT 
        sale_date,
        region,
        salesperson,
        amount,
        ROW_NUMBER() OVER (PARTITION BY sale_date ORDER BY amount DESC) as daily_rank,
        RANK() OVER (PARTITION BY sale_date ORDER BY amount DESC) as daily_rank_with_ties,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY amount DESC) as region_rank
    FROM sales
    ORDER BY sale_date, daily_rank
""")
rank_query.show()

-- 2. 累計銷售額計算
print("累計銷售額:")
cumulative_query = spark.sql("""
    SELECT 
        sale_date,
        region,
        amount,
        SUM(amount) OVER (
            PARTITION BY region 
            ORDER BY sale_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as cumulative_amount,
        AVG(amount) OVER (
            PARTITION BY region 
            ORDER BY sale_date 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) as moving_avg_3days
    FROM sales
    ORDER BY region, sale_date
""")
cumulative_query.show()

-- 3. 同期比較分析
print("銷售額對比分析:")
comparison_query = spark.sql("""
    SELECT 
        sale_date,
        region,
        amount,
        LAG(amount, 1) OVER (PARTITION BY region ORDER BY sale_date) as prev_day_amount,
        amount - LAG(amount, 1) OVER (PARTITION BY region ORDER BY sale_date) as day_over_day_change,
        LEAD(amount, 1) OVER (PARTITION BY region ORDER BY sale_date) as next_day_amount
    FROM sales
    ORDER BY region, sale_date
""")
comparison_query.show()

-- 4. 百分位數統計
print("銷售額百分位數:")
percentile_query = spark.sql("""
    SELECT 
        region,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) as q1,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) as q3,
        PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount) as p90
    FROM sales
    GROUP BY region
""")
percentile_query.show()
```

### 練習3解答

```sql
-- 1. 銷售數據透視表
print("地區-類別銷售透視表:")
pivot_query = spark.sql("""
    SELECT 
        region,
        SUM(CASE WHEN category = 'Electronics' THEN quantity * unit_price ELSE 0 END) as Electronics_Revenue,
        SUM(CASE WHEN category = 'Books' THEN quantity * unit_price ELSE 0 END) as Books_Revenue,
        SUM(CASE WHEN category = 'Clothing' THEN quantity * unit_price ELSE 0 END) as Clothing_Revenue,
        SUM(quantity * unit_price) as Total_Revenue
    FROM orders
    GROUP BY region
    ORDER BY Total_Revenue DESC
""")
pivot_query.show()

-- 2. 客戶 RFM 分析
print("客戶 RFM 分析:")
rfm_query = spark.sql("""
    WITH customer_metrics AS (
        SELECT 
            customer_id,
            customer_name,
            DATEDIFF(CURRENT_DATE(), MAX(order_date)) as recency,
            COUNT(DISTINCT order_id) as frequency,
            SUM(quantity * unit_price) as monetary
        FROM orders
        GROUP BY customer_id, customer_name
    )
    SELECT 
        customer_id,
        customer_name,
        recency,
        frequency,
        monetary,
        CASE 
            WHEN recency <= 7 AND frequency >= 2 AND monetary >= 500 THEN 'VIP'
            WHEN recency <= 14 AND frequency >= 1 AND monetary >= 200 THEN 'Regular'
            ELSE 'At Risk'
        END as customer_segment
    FROM customer_metrics
    ORDER BY monetary DESC
""")
rfm_query.show()

-- 3. 產品績效分析
print("產品績效分析:")
product_analysis = spark.sql("""
    SELECT 
        product,
        category,
        COUNT(DISTINCT order_id) as order_count,
        SUM(quantity) as total_quantity,
        SUM(quantity * unit_price) as total_revenue,
        AVG(unit_price) as avg_price,
        COUNT(DISTINCT customer_id) as unique_customers
    FROM orders
    GROUP BY product, category
    ORDER BY total_revenue DESC
""")
product_analysis.show()

-- 4. 時間趨勢分析
print("每日銷售趨勢:")
trend_analysis = spark.sql("""
    SELECT 
        order_date,
        COUNT(DISTINCT order_id) as daily_orders,
        SUM(quantity * unit_price) as daily_revenue,
        COUNT(DISTINCT customer_id) as daily_customers,
        SUM(quantity * unit_price) / COUNT(DISTINCT order_id) as avg_order_value
    FROM orders
    GROUP BY order_date
    ORDER BY order_date
""")
trend_analysis.show()
```

### 練習4解答

```python
# 1. 實現 SCD Type 2
customers_df = spark.createDataFrame(customer_history, customer_columns)
customers_df.createOrReplaceTempView("customer_dim")

print("客戶維度表（SCD Type 2）:")
scd_query = spark.sql("""
    SELECT 
        customer_id,
        name,
        email,
        city,
        tier,
        effective_date,
        COALESCE(end_date, '9999-12-31') as end_date,
        is_current
    FROM customer_dim
    ORDER BY customer_id, effective_date
""")
scd_query.show()

# 2. 事實表分析
fact_df = spark.createDataFrame(fact_sales, fact_columns)
fact_df.createOrReplaceTempView("fact_sales")

print("事實表和維度表關聯:")
fact_dim_query = spark.sql("""
    SELECT 
        f.sale_id,
        f.sale_date,
        c.name as customer_name,
        c.tier as customer_tier,
        f.quantity,
        f.amount
    FROM fact_sales f
    JOIN customer_dim c ON f.customer_id = c.customer_id 
        AND f.sale_date BETWEEN c.effective_date AND COALESCE(c.end_date, '9999-12-31')
    ORDER BY f.sale_date
""")
fact_dim_query.show()

# 3. 數據質量檢查
print("數據質量報告:")
quality_check = spark.sql("""
    SELECT 
        'fact_sales' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN sale_id IS NULL THEN 1 END) as null_sale_id,
        COUNT(CASE WHEN amount <= 0 THEN 1 END) as invalid_amount,
        COUNT(CASE WHEN quantity <= 0 THEN 1 END) as invalid_quantity,
        MIN(sale_date) as earliest_date,
        MAX(sale_date) as latest_date
    FROM fact_sales
    
    UNION ALL
    
    SELECT 
        'customer_dim' as table_name,
        COUNT(*) as total_records,
        COUNT(CASE WHEN customer_id IS NULL THEN 1 END) as null_customer_id,
        COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as null_name,
        COUNT(CASE WHEN email IS NULL OR email = '' THEN 1 END) as null_email,
        MIN(effective_date) as earliest_date,
        MAX(effective_date) as latest_date
    FROM customer_dim
""")
quality_check.show()

# 4. 增量更新邏輯
print("增量更新示例:")
incremental_query = spark.sql("""
    WITH daily_summary AS (
        SELECT 
            sale_date,
            COUNT(*) as daily_transactions,
            SUM(amount) as daily_revenue
        FROM fact_sales
        WHERE sale_date >= '2024-01-15'  -- 增量日期條件
        GROUP BY sale_date
    )
    SELECT 
        sale_date,
        daily_transactions,
        daily_revenue,
        SUM(daily_revenue) OVER (ORDER BY sale_date) as running_total
    FROM daily_summary
    ORDER BY sale_date
""")
incremental_query.show()
```

## 練習提示

1. **SQL 最佳實踐**：
   - 使用適當的索引和分區
   - 避免不必要的子查詢
   - 合理使用 JOIN 類型

2. **窗口函數優化**：
   - 注意窗口規範的性能影響
   - 使用適當的分區和排序
   - 避免過度複雜的窗口計算

3. **數據倉庫設計**：
   - 遵循星型或雪花型架構
   - 合理設計維度表和事實表
   - 實現有效的 SCD 策略

4. **性能調優**：
   - 使用廣播 JOIN 處理小表
   - 合理設置並行度
   - 監控 SQL 執行計劃

## 進階挑戰

1. **複雜分析函數**：
   - 實現自定義聚合函數
   - 使用遞歸查詢處理層次數據
   - 實現複雜的時間序列分析

2. **大數據處理**：
   - 處理 TB 級別的數據集
   - 實現分區裁剪策略
   - 優化記憶體使用

3. **實時分析**：
   - 結合 Streaming 實現實時 SQL
   - 實現增量物化視圖
   - 建立實時儀表板

## 學習檢核

完成練習後，你應該能夠：
- [ ] 熟練使用 Spark SQL 語法
- [ ] 掌握窗口函數的各種應用
- [ ] 實現複雜的數據分析查詢
- [ ] 設計和實現數據倉庫架構
- [ ] 進行 SQL 性能調優
- [ ] 實現 ETL 流程和數據質量檢查