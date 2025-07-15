# 第3章練習：DataFrame 與 Schema 操作

## 練習目標
深入學習 DataFrame 的進階操作、Schema 定義、數據讀寫，以及資料轉換技巧。

## 練習1：自定義 Schema 和數據驗證

### 任務描述
學習如何定義複雜的 Schema，並進行數據驗證和清洗。

### 要求
1. 定義包含多種數據類型的 Schema
2. 讀取 CSV 文件並應用 Schema
3. 驗證數據完整性
4. 處理 Schema 不匹配的情況
5. 創建數據質量報告

### 程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col, when, isnan, isnull, count, sum as spark_sum

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("Schema和數據驗證練習") \
    .master("local[*]") \
    .getOrCreate()

# 定義產品數據的 Schema
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("is_available", BooleanType(), True),
    StructField("launch_date", DateType(), True),
    StructField("rating", DoubleType(), True)
])

# 完成以下任務：
# 1. 創建測試數據並應用 Schema
# 2. 讀取 CSV 文件（模擬數據質量問題）
# 3. 驗證每個欄位的數據質量
# 4. 生成數據質量報告
# 5. 清洗和修復數據

# 你的程式碼在這裡

spark.stop()
```

### 預期輸出
- Schema 定義和驗證結果
- 數據質量報告
- 清洗前後的數據對比

## 練習2：複雜數據轉換和聚合

### 任務描述
學習使用 DataFrame API 進行複雜的數據轉換和聚合操作。

### 要求
1. 創建多層次的數據聚合
2. 使用 Window 函數進行排名和累計計算
3. 實現數據透視表功能
4. 進行時間序列分析
5. 創建自定義聚合函數

### 程式碼模板

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# 銷售交易數據
sales_data = [
    ("2024-01-01", "A001", "Electronics", "Laptop", 1200.00, 2, "John", "North"),
    ("2024-01-01", "A002", "Electronics", "Mouse", 25.00, 5, "Alice", "South"),
    ("2024-01-02", "B001", "Books", "Python Guide", 45.00, 3, "Bob", "East"),
    ("2024-01-02", "A003", "Electronics", "Keyboard", 75.00, 4, "Charlie", "West"),
    ("2024-01-03", "C001", "Clothing", "T-Shirt", 20.00, 10, "Diana", "North"),
    ("2024-01-03", "A004", "Electronics", "Monitor", 300.00, 2, "Eve", "South"),
    ("2024-01-04", "B002", "Books", "Java Manual", 50.00, 2, "Frank", "East"),
    ("2024-01-04", "C002", "Clothing", "Jeans", 60.00, 3, "Grace", "West"),
    ("2024-01-05", "A005", "Electronics", "Tablet", 400.00, 1, "Henry", "North"),
    ("2024-01-05", "C003", "Clothing", "Shoes", 80.00, 2, "Ivy", "South")
]

columns = ["date", "product_id", "category", "product_name", "price", "quantity", "customer", "region"]
sales_df = spark.createDataFrame(sales_data, columns)

# 完成以下任務：
# 1. 計算每日、每週、每月的銷售統計
# 2. 使用 Window 函數計算累計銷售額
# 3. 找出每個類別的排名前3的產品
# 4. 創建銷售數據透視表（地區 vs 類別）
# 5. 計算移動平均銷售額

# 你的程式碼在這裡
```

## 練習3：多數據源整合

### 任務描述
學習如何整合來自不同數據源的數據，並處理數據格式差異。

### 要求
1. 讀取不同格式的數據文件（CSV, JSON, Parquet）
2. 統一數據格式和 Schema
3. 進行數據合併和關聯
4. 處理重複數據
5. 創建統一的數據視圖

### 程式碼模板

```python
# 模擬不同格式的數據源

# CSV 數據：客戶信息
customer_csv_data = '''customer_id,name,email,phone,city
C001,John Doe,john@email.com,123-456-7890,New York
C002,Alice Smith,alice@email.com,234-567-8901,Los Angeles
C003,Bob Johnson,bob@email.com,345-678-9012,Chicago'''

# JSON 數據：訂單信息
order_json_data = '''
[
  {"order_id": "O001", "customer_id": "C001", "product_id": "P001", "quantity": 2, "order_date": "2024-01-15"},
  {"order_id": "O002", "customer_id": "C002", "product_id": "P002", "quantity": 1, "order_date": "2024-01-16"},
  {"order_id": "O003", "customer_id": "C003", "product_id": "P001", "quantity": 3, "order_date": "2024-01-17"}
]
'''

# Parquet 數據：產品信息（模擬）
product_data = [
    ("P001", "Laptop", "Electronics", 1200.00),
    ("P002", "Smartphone", "Electronics", 800.00),
    ("P003", "Book", "Education", 25.00)
]
product_columns = ["product_id", "product_name", "category", "price"]

# 完成以下任務：
# 1. 讀取和解析各種格式的數據
# 2. 統一 Schema 格式
# 3. 進行多表關聯查詢
# 4. 識別和處理重複數據
# 5. 創建完整的訂單視圖

# 你的程式碼在這裡
```

## 練習4：自定義函數和 UDF

### 任務描述
學習創建和使用用戶定義函數（UDF）來擴展 DataFrame 的功能。

### 要求
1. 創建簡單的 UDF 函數
2. 使用 UDF 進行複雜的數據轉換
3. 創建聚合 UDF (UDAF)
4. 性能優化和最佳實踐
5. 錯誤處理和調試

### 程式碼模板

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

# 員工數據
employee_data = [
    ("E001", "John", "Doe", 5, 75000, "Engineering"),
    ("E002", "Alice", "Smith", 3, 65000, "Marketing"),
    ("E003", "Bob", "Johnson", 8, 85000, "Engineering"),
    ("E004", "Charlie", "Brown", 2, 55000, "Sales"),
    ("E005", "Diana", "Wilson", 6, 70000, "HR")
]

columns = ["emp_id", "first_name", "last_name", "years_experience", "salary", "department"]
employees_df = spark.createDataFrame(employee_data, columns)

# 完成以下任務：
# 1. 創建 UDF 來格式化員工全名
# 2. 創建 UDF 來計算薪資等級
# 3. 使用 Pandas UDF 進行批量數據處理
# 4. 創建聚合 UDF 來計算部門薪資統計
# 5. 比較 UDF 與內建函數的性能

# 你的程式碼在這裡

# 示例 UDF 定義
def format_full_name(first, last):
    return f"{last}, {first}"

# 註冊 UDF
format_name_udf = udf(format_full_name, StringType())
```

## 練習答案

### 練習1解答

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col, when, isnan, isnull, count, sum as spark_sum

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("Schema和數據驗證練習") \
    .master("local[*]") \
    .getOrCreate()

# 定義產品數據的 Schema
product_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("stock_quantity", IntegerType(), True),
    StructField("is_available", BooleanType(), True),
    StructField("launch_date", DateType(), True),
    StructField("rating", DoubleType(), True)
])

# 1. 創建測試數據
test_data = [
    ("P001", "Laptop", "Electronics", 1299.99, 50, True, "2024-01-01", 4.5),
    ("P002", "Mouse", "Electronics", 29.99, 200, True, "2024-01-15", 4.2),
    ("P003", None, "Books", 24.99, 100, True, "2024-02-01", 4.0),  # 缺失產品名
    ("P004", "Keyboard", "Electronics", -10.0, 75, True, "2024-01-20", 4.3),  # 負價格
    ("P005", "Monitor", "Electronics", 299.99, None, True, "2024-02-10", 4.7),  # 缺失庫存
]

products_df = spark.createDataFrame(test_data, product_schema)

print("原始數據:")
products_df.show()

# 2. 數據質量檢查
print("數據質量報告:")
print("=" * 50)

# 檢查空值
null_counts = products_df.select([
    count(when(col(c).isNull(), c)).alias(f"{c}_null_count") 
    for c in products_df.columns
])
null_counts.show()

# 檢查數據範圍問題
print("價格範圍檢查:")
products_df.filter(col("price") < 0).show()

print("評分範圍檢查:")
products_df.filter((col("rating") < 0) | (col("rating") > 5)).show()

# 3. 數據清洗
cleaned_df = products_df \
    .fillna({"product_name": "Unknown Product"}) \
    .fillna({"stock_quantity": 0}) \
    .withColumn("price", when(col("price") < 0, 0).otherwise(col("price"))) \
    .withColumn("rating", when(col("rating") < 0, 0)
                         .when(col("rating") > 5, 5)
                         .otherwise(col("rating")))

print("清洗後的數據:")
cleaned_df.show()

# 4. 數據質量統計
total_records = products_df.count()
clean_records = cleaned_df.filter(
    col("product_name").isNotNull() & 
    col("price") > 0 & 
    col("stock_quantity").isNotNull()
).count()

print(f"總記錄數: {total_records}")
print(f"完整記錄數: {clean_records}")
print(f"數據完整率: {clean_records/total_records*100:.2f}%")

spark.stop()
```

### 練習2解答

```python
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *

# 使用銷售數據
sales_df = spark.createDataFrame(sales_data, columns)

# 將日期字符串轉換為日期類型
sales_df = sales_df.withColumn("date", to_date(col("date")))
sales_df = sales_df.withColumn("total_amount", col("price") * col("quantity"))

print("原始銷售數據:")
sales_df.show()

# 1. 每日銷售統計
daily_sales = sales_df.groupBy("date") \
    .agg(
        sum("total_amount").alias("daily_revenue"),
        sum("quantity").alias("daily_quantity"),
        count("*").alias("daily_transactions")
    ) \
    .orderBy("date")

print("每日銷售統計:")
daily_sales.show()

# 2. 使用 Window 函數計算累計銷售額
window_spec = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
cumulative_sales = daily_sales.withColumn(
    "cumulative_revenue", 
    sum("daily_revenue").over(window_spec)
)

print("累計銷售額:")
cumulative_sales.show()

# 3. 每個類別的排名前3產品
category_window = Window.partitionBy("category").orderBy(desc("total_amount"))
top_products = sales_df.withColumn("rank", row_number().over(category_window)) \
    .filter(col("rank") <= 3) \
    .select("category", "product_name", "total_amount", "rank")

print("每個類別排名前3的產品:")
top_products.show()

# 4. 銷售數據透視表（地區 vs 類別）
pivot_table = sales_df.groupBy("region") \
    .pivot("category") \
    .agg(sum("total_amount"))

print("銷售數據透視表（地區 vs 類別）:")
pivot_table.show()

# 5. 3日移動平均
window_3_days = Window.orderBy("date").rowsBetween(-2, 0)
moving_average = daily_sales.withColumn(
    "moving_avg_3_days",
    avg("daily_revenue").over(window_3_days)
)

print("3日移動平均:")
moving_average.show()

# 6. 地區銷售排名
region_sales = sales_df.groupBy("region") \
    .agg(sum("total_amount").alias("region_revenue")) \
    .orderBy(desc("region_revenue"))

print("地區銷售排名:")
region_sales.show()
```

### 練習3解答

```python
import tempfile
import os

# 1. 準備不同格式的數據文件
# 創建臨時 CSV 文件
csv_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
csv_temp.write(customer_csv_data)
csv_temp.close()

# 創建臨時 JSON 文件
json_temp = tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False)
json_temp.write(order_json_data)
json_temp.close()

# 創建產品 DataFrame 並保存為 Parquet
products_df = spark.createDataFrame(product_data, product_columns)
parquet_path = tempfile.mkdtemp() + "/products.parquet"
products_df.write.mode("overwrite").parquet(parquet_path)

# 2. 讀取不同格式的數據
print("讀取 CSV 數據:")
customers_df = spark.read.option("header", "true").csv(csv_temp.name)
customers_df.show()
customers_df.printSchema()

print("讀取 JSON 數據:")
orders_df = spark.read.json(json_temp.name)
orders_df.show()
orders_df.printSchema()

print("讀取 Parquet 數據:")
products_df = spark.read.parquet(parquet_path)
products_df.show()
products_df.printSchema()

# 3. 統一 Schema 格式
# 確保訂單日期為日期類型
orders_df = orders_df.withColumn("order_date", to_date(col("order_date")))

# 4. 多表關聯查詢
complete_orders = orders_df \
    .join(customers_df, "customer_id") \
    .join(products_df, "product_id") \
    .select(
        "order_id", "order_date", "name", "email", "city",
        "product_name", "category", "price", "quantity",
        (col("price") * col("quantity")).alias("total_amount")
    )

print("完整的訂單視圖:")
complete_orders.show()

# 5. 重複數據檢查
print("檢查重複的客戶郵箱:")
duplicate_emails = customers_df.groupBy("email").count().filter(col("count") > 1)
duplicate_emails.show()

# 6. 創建匯總報告
summary_report = complete_orders.groupBy("city", "category") \
    .agg(
        sum("total_amount").alias("total_revenue"),
        count("*").alias("order_count"),
        avg("total_amount").alias("avg_order_value")
    ) \
    .orderBy("city", "category")

print("城市和類別匯總報告:")
summary_report.show()

# 清理臨時文件
os.unlink(csv_temp.name)
os.unlink(json_temp.name)
```

### 練習4解答

```python
from pyspark.sql.functions import udf, pandas_udf
from pyspark.sql.types import StringType, DoubleType
import pandas as pd

# 1. 創建 UDF 來格式化員工全名
def format_full_name(first, last):
    if first and last:
        return f"{last}, {first}"
    return "Unknown"

format_name_udf = udf(format_full_name, StringType())

# 2. 創建 UDF 來計算薪資等級
def calculate_salary_grade(salary, years):
    if salary >= 80000:
        return "Senior"
    elif salary >= 60000:
        return "Mid"
    else:
        return "Junior"

salary_grade_udf = udf(calculate_salary_grade, StringType())

# 3. 使用 Pandas UDF 進行批量處理
@pandas_udf(returnType=DoubleType())
def calculate_bonus(salary: pd.Series, years: pd.Series) -> pd.Series:
    return salary * 0.1 * (1 + years * 0.02)

# 應用 UDF
employees_enhanced = employees_df \
    .withColumn("full_name", format_name_udf("first_name", "last_name")) \
    .withColumn("salary_grade", salary_grade_udf("salary", "years_experience")) \
    .withColumn("annual_bonus", calculate_bonus("salary", "years_experience"))

print("增強的員工數據:")
employees_enhanced.show()

# 4. 創建聚合統計
def calculate_department_stats(df):
    return df.groupBy("department") \
        .agg(
            avg("salary").alias("avg_salary"),
            max("salary").alias("max_salary"),
            min("salary").alias("min_salary"),
            count("*").alias("employee_count"),
            avg("annual_bonus").alias("avg_bonus")
        )

dept_stats = calculate_department_stats(employees_enhanced)
print("部門統計:")
dept_stats.show()

# 5. 性能比較：UDF vs 內建函數
print("性能測試 - UDF vs 內建函數")

# 使用 UDF
start_time = time.time()
udf_result = employees_df.withColumn("full_name", format_name_udf("first_name", "last_name"))
udf_result.collect()
udf_time = time.time() - start_time

# 使用內建函數
start_time = time.time()
builtin_result = employees_df.withColumn("full_name", concat(col("last_name"), lit(", "), col("first_name")))
builtin_result.collect()
builtin_time = time.time() - start_time

print(f"UDF 執行時間: {udf_time:.4f} 秒")
print(f"內建函數執行時間: {builtin_time:.4f} 秒")
print(f"性能差異: {udf_time/builtin_time:.2f}x")
```

## 練習提示

1. **Schema 設計**：
   - 仔細考慮數據類型的選擇
   - 使用適當的可空性設置
   - 考慮數據壓縮和性能影響

2. **數據驗證**：
   - 建立完整的數據質量檢查流程
   - 使用統計函數識別異常值
   - 記錄數據清洗的步驟和原因

3. **UDF 使用**：
   - 優先使用內建函數，性能更好
   - Pandas UDF 適合向量化操作
   - 注意 UDF 的序列化開銷

4. **多數據源整合**：
   - 統一數據格式和命名規範
   - 使用適當的連接類型
   - 處理數據傾斜問題

## 進階挑戰

1. **複雜 Schema**：
   - 處理嵌套結構（Arrays, Maps, Structs）
   - 實現 Schema 演進策略
   - 處理半結構化數據

2. **數據血緣**：
   - 追蹤數據轉換過程
   - 實現數據質量監控
   - 建立數據目錄

3. **性能優化**：
   - 使用列式存儲格式
   - 優化分區策略
   - 實現增量數據處理

## 學習檢核

完成練習後，你應該能夠：
- [ ] 設計和應用複雜的 Schema
- [ ] 實現數據質量驗證和清洗
- [ ] 使用 Window 函數進行複雜分析
- [ ] 整合多種數據源
- [ ] 創建和優化 UDF
- [ ] 理解數據轉換的最佳實踐