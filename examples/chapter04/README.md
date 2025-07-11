# 第4章：Spark SQL

## 📚 學習目標

- 掌握 Spark SQL 的基本語法
- 學會使用臨時視圖進行查詢
- 理解複雜 SQL 查詢的優化
- 熟悉內建函數和自訂函數

## 🎯 本章內容

### 核心概念
- **Spark SQL** - 結構化數據查詢
- **Temporary View** - 臨時視圖
- **Catalyst Optimizer** - 查詢優化器
- **UDF** - 使用者定義函數

### 檔案說明
- `spark_sql_basics.py` - SQL 基礎查詢操作
- `advanced_sql_functions.py` - 進階函數和優化

## 🚀 開始學習

### 執行範例

```bash
# 執行基礎 SQL 範例
poetry run python examples/chapter04/spark_sql_basics.py

# 執行進階函數範例
poetry run python examples/chapter04/advanced_sql_functions.py

# 或使用 Makefile
make run-chapter04
```

## 🔍 深入理解

### SQL 查詢類型

#### 1. 基本查詢
```sql
-- 選擇操作
SELECT name, age FROM employees;

-- 條件過濾
SELECT * FROM employees WHERE age > 30;

-- 排序
SELECT * FROM employees ORDER BY salary DESC;
```

#### 2. 聚合查詢
```sql
-- 分組統計
SELECT department, COUNT(*) as count, AVG(salary) as avg_salary
FROM employees
GROUP BY department;

-- 條件聚合
SELECT department, AVG(salary) as avg_salary
FROM employees
GROUP BY department
HAVING AVG(salary) > 70000;
```

#### 3. 連接查詢
```sql
-- 內連接
SELECT e.name, d.dept_name
FROM employees e
INNER JOIN departments d ON e.department = d.dept_code;

-- 外連接
SELECT e.name, d.dept_name
FROM employees e
LEFT JOIN departments d ON e.department = d.dept_code;
```

### 進階功能

#### 1. 視窗函數
```sql
-- 排名函數
SELECT name, salary,
       ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
FROM employees;

-- 聚合視窗函數
SELECT name, salary,
       AVG(salary) OVER (PARTITION BY department) as dept_avg
FROM employees;
```

#### 2. 子查詢
```sql
-- 標量子查詢
SELECT name, salary
FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- 相關子查詢
SELECT e1.name, e1.salary
FROM employees e1
WHERE e1.salary = (
    SELECT MAX(e2.salary)
    FROM employees e2
    WHERE e2.department = e1.department
);
```

#### 3. 公用表格運算式 (CTE)
```sql
WITH dept_stats AS (
    SELECT department, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department
)
SELECT e.name, e.salary, ds.avg_salary
FROM employees e
JOIN dept_stats ds ON e.department = ds.department;
```

## 🎛️ 內建函數

### 1. 字串函數
```sql
SELECT name,
       UPPER(name) as upper_name,
       LENGTH(name) as name_length,
       SUBSTRING(name, 1, 3) as name_prefix
FROM employees;
```

### 2. 數值函數
```sql
SELECT salary,
       ROUND(salary / 12, 2) as monthly_salary,
       CEIL(salary / 1000) as salary_k_ceil
FROM employees;
```

### 3. 日期函數
```sql
SELECT hire_date,
       YEAR(hire_date) as hire_year,
       DATEDIFF(CURRENT_DATE(), hire_date) as days_since_hire
FROM employees;
```

### 4. 陣列函數
```sql
SELECT name,
       skills,
       SIZE(skills) as skill_count,
       ARRAY_CONTAINS(skills, 'Python') as knows_python
FROM employees;
```

## 🔧 自訂函數 (UDF)

### 創建 UDF
```python
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def calculate_bonus(salary):
    return int(salary * 0.1)

# 註冊 UDF
spark.udf.register("calculate_bonus", calculate_bonus, IntegerType())

# 使用 UDF
result = spark.sql("SELECT name, salary, calculate_bonus(salary) as bonus FROM employees")
```

### UDF 最佳實踐
- 使用內建函數優於 UDF
- 避免在 UDF 中使用外部資源
- 考慮使用 Pandas UDF 提高性能

## 📊 查詢優化

### 1. 執行計劃
```python
# 查看邏輯計劃
df.explain(True)

# 查看物理計劃
df.explain("physical")
```

### 2. 廣播連接
```sql
-- 使用廣播提示
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table l
JOIN small_table s ON l.id = s.id;
```

### 3. 分區修剪
```sql
-- 利用分區列進行過濾
SELECT * FROM partitioned_table
WHERE partition_date = '2024-01-01';
```

## 📝 練習建議

### 基礎練習
1. 練習各種 SQL 查詢語法
2. 嘗試不同的連接類型
3. 使用各種內建函數

### 進階練習
1. 編寫複雜的分析查詢
2. 創建自訂函數
3. 優化查詢性能

## 🛠️ 實用技巧

### 1. 調試查詢
```python
# 查看查詢執行統計
spark.sql("SELECT * FROM employees").explain("cost")

# 啟用查詢日誌
spark.conf.set("spark.sql.queryExecutionListeners", "org.apache.spark.sql.util.QueryExecutionListener")
```

### 2. 緩存策略
```sql
-- 緩存表
CACHE TABLE employees;

-- 查看緩存狀態
SHOW TABLES;
```

### 3. 配置優化
```python
# 啟用自適應查詢執行
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## 🔧 疑難排解

### 常見問題

**Q: 查詢執行很慢怎麼辦？**
A: 檢查執行計劃，考慮添加分區或使用廣播連接。

**Q: 記憶體不足錯誤？**
A: 增加 driver 記憶體或調整 shuffle 分區數。

**Q: 臨時視圖不存在？**
A: 確保在查詢前創建視圖，視圖的生命週期綁定到 SparkSession。

## 💡 最佳實踐

1. **善用內建函數** - 避免不必要的 UDF
2. **合理使用緩存** - 對於重複查詢的表進行緩存
3. **分區策略** - 合理設計分區以提高查詢效率
4. **查詢優化** - 定期檢查和優化執行計劃
5. **資源管理** - 適當配置 Spark 資源

## 📖 相關文檔

- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [SQL Reference](https://spark.apache.org/docs/latest/sql-ref.html)
- [Built-in Functions](https://spark.apache.org/docs/latest/api/sql/index.html)

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第5章：Spark Streaming](../chapter05/README.md)
- 了解實時數據處理
- 學習結構化流處理

## 🎯 學習檢核

完成本章學習後，你應該能夠：
- [ ] 編寫基本的 SQL 查詢
- [ ] 使用複雜的 SQL 功能（子查詢、CTE、視窗函數）
- [ ] 創建和使用自訂函數
- [ ] 優化 SQL 查詢性能
- [ ] 理解 Catalyst 優化器的工作原理

## 🗂️ 章節文件總覽

### spark_sql_basics.py
- 基本 SQL 查詢語法
- 臨時視圖創建和使用
- 連接查詢和子查詢
- 視窗函數應用

### advanced_sql_functions.py
- 進階 SQL 函數
- 自訂函數 (UDF)
- 複雜數據處理
- 性能優化技巧