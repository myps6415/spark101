# 第7章練習：Spark 性能調優

## 練習目標
學習 Spark 性能調優的核心技術，包括資源配置、數據分區、緩存策略、以及執行計劃優化。

## 練習1：基礎性能分析和監控

### 任務描述
學習如何分析 Spark 應用的性能瓶頸，使用 Spark UI 和監控工具。

### 要求
1. 設置性能監控
2. 分析 Spark UI
3. 識別性能瓶頸
4. 實現基本的性能測試
5. 記錄和分析性能指標

### 程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import random

# 創建性能調優的 SparkSession
spark = SparkSession.builder \
    .appName("性能調優練習") \
    .master("local[*]") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# 性能監控函數
def monitor_performance(func, *args, **kwargs):
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    
    execution_time = end_time - start_time
    print(f"執行時間: {execution_time:.2f} 秒")
    
    return result, execution_time

# 生成大量測試數據
def generate_large_dataset(num_records=1000000):
    data = []
    for i in range(num_records):
        data.append((
            i,
            f"user_{random.randint(1, 10000)}",
            random.choice(["A", "B", "C", "D"]),
            random.randint(18, 80),
            random.uniform(1000, 100000)
        ))
    
    columns = ["id", "user_name", "category", "age", "amount"]
    return spark.createDataFrame(data, columns)

# 完成以下任務：
# 1. 創建大型數據集
# 2. 分析數據分區
# 3. 測試不同操作的性能
# 4. 使用 explain() 分析執行計劃
# 5. 實現性能基準測試

# 你的程式碼在這裡

spark.stop()
```

## 練習2：數據分區優化

### 任務描述
學習如何優化數據分區策略，提高 Spark 應用的並行性和性能。

### 要求
1. 理解默認分區策略
2. 實現自定義分區
3. 優化 JOIN 操作的分區
4. 處理數據傾斜問題
5. 測試不同分區策略的性能

### 程式碼模板

```python
from pyspark.sql.functions import spark_partition_id

# 分區分析函數
def analyze_partitions(df, name="DataFrame"):
    print(f"\n{name} 分區分析:")
    print(f"分區數量: {df.rdd.getNumPartitions()}")
    
    # 每個分區的記錄數
    partition_counts = df.withColumn("partition_id", spark_partition_id()) \
                        .groupBy("partition_id") \
                        .count() \
                        .orderBy("partition_id")
    
    print("每個分區的記錄數:")
    partition_counts.show()
    
    return partition_counts

# 測試數據
def create_test_datasets():
    # 大表
    large_data = [(i, f"user_{i}", i % 100, random.randint(1, 1000)) 
                  for i in range(100000)]
    large_df = spark.createDataFrame(large_data, ["id", "name", "group_id", "value"])
    
    # 小表
    small_data = [(i, f"group_{i}", random.choice(["A", "B", "C"])) 
                  for i in range(100)]
    small_df = spark.createDataFrame(small_data, ["group_id", "group_name", "type"])
    
    return large_df, small_df

# 完成以下任務：
# 1. 分析默認分區
# 2. 測試 repartition() vs coalesce()
# 3. 實現按 key 分區
# 4. 優化 JOIN 操作
# 5. 處理數據傾斜

# 你的程式碼在這裡
```

## 練習3：緩存和持久化策略

### 任務描述
學習如何有效使用緩存和持久化來提高重複計算的性能。

### 要求
1. 比較不同存儲級別的性能
2. 實現智能緩存策略
3. 管理緩存的生命週期
4. 監控記憶體使用
5. 優化序列化性能

### 程式碼模板

```python
from pyspark import StorageLevel
import gc

# 緩存性能測試
def cache_performance_test():
    # 創建計算密集型 DataFrame
    df = generate_large_dataset(500000)
    
    # 複雜轉換
    complex_df = df.withColumn("computed_value", 
                              col("amount") * col("age") + 
                              when(col("category") == "A", 100)
                              .when(col("category") == "B", 200)
                              .otherwise(300)) \
                  .filter(col("age") > 25) \
                  .groupBy("category") \
                  .agg(avg("computed_value").alias("avg_computed"),
                       count("*").alias("count"))
    
    # 測試不同存儲級別
    storage_levels = [
        ("無緩存", None),
        ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
        ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
        ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
        ("DISK_ONLY", StorageLevel.DISK_ONLY)
    ]
    
    results = []
    
    for name, storage_level in storage_levels:
        # 清理緩存
        spark.catalog.clearCache()
        gc.collect()
        
        if storage_level:
            cached_df = complex_df.persist(storage_level)
        else:
            cached_df = complex_df
        
        # 首次執行（觸發緩存）
        start_time = time.time()
        cached_df.collect()
        first_time = time.time() - start_time
        
        # 第二次執行（使用緩存）
        start_time = time.time()
        cached_df.collect()
        second_time = time.time() - start_time
        
        results.append((name, first_time, second_time))
        print(f"{name}: 首次={first_time:.2f}s, 第二次={second_time:.2f}s")
    
    return results

# 完成以下任務：
# 1. 測試不同存儲級別
# 2. 實現動態緩存策略
# 3. 監控記憶體使用
# 4. 優化序列化
# 5. 管理緩存生命週期

# 你的程式碼在這裡
```

## 練習4：SQL 和執行計劃優化

### 任務描述
學習如何優化 Spark SQL 查詢和分析執行計劃。

### 要求
1. 分析執行計劃
2. 優化 JOIN 策略
3. 使用列式存儲
4. 實現查詢優化
5. 配置 Catalyst 優化器

### 程式碼模板

```python
# 執行計劃分析
def analyze_execution_plan(df, query_name="Query"):
    print(f"\n{query_name} 執行計劃:")
    print("="*50)
    df.explain(True)  # 顯示詳細執行計劃
    
    # 分析物理計劃
    print("\n物理計劃:")
    df.explain("formatted")

# 創建測試表
def create_test_tables():
    # 訂單表
    orders_data = [(i, f"customer_{i%1000}", f"product_{i%100}", 
                   random.randint(1, 10), random.uniform(10, 1000))
                   for i in range(100000)]
    orders_df = spark.createDataFrame(orders_data, 
                                    ["order_id", "customer_id", "product_id", "quantity", "price"])
    
    # 客戶表
    customers_data = [(f"customer_{i}", f"Customer {i}", 
                      random.choice(["A", "B", "C"]), random.randint(18, 80))
                      for i in range(1000)]
    customers_df = spark.createDataFrame(customers_data,
                                       ["customer_id", "customer_name", "segment", "age"])
    
    # 產品表
    products_data = [(f"product_{i}", f"Product {i}", 
                     random.choice(["Electronics", "Books", "Clothing"]),
                     random.uniform(10, 500))
                     for i in range(100)]
    products_df = spark.createDataFrame(products_data,
                                      ["product_id", "product_name", "category", "cost"])
    
    return orders_df, customers_df, products_df

# JOIN 優化測試
def test_join_optimizations():
    orders_df, customers_df, products_df = create_test_tables()
    
    # 創建臨時視圖
    orders_df.createOrReplaceTempView("orders")
    customers_df.createOrReplaceTempView("customers")
    products_df.createOrReplaceTempView("products")
    
    # 測試不同 JOIN 策略
    queries = {
        "普通 JOIN": """
            SELECT c.customer_name, p.product_name, o.quantity, o.price
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
            WHERE c.age > 30
        """,
        
        "廣播 JOIN": """
            SELECT /*+ BROADCAST(c, p) */ c.customer_name, p.product_name, o.quantity, o.price
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
            WHERE c.age > 30
        """
    }
    
    for name, query in queries.items():
        print(f"\n{name}:")
        result_df = spark.sql(query)
        analyze_execution_plan(result_df, name)
        
        # 測試執行時間
        start_time = time.time()
        result_df.collect()
        execution_time = time.time() - start_time
        print(f"執行時間: {execution_time:.2f} 秒")

# 完成以下任務：
# 1. 分析不同 JOIN 策略
# 2. 優化複雜查詢
# 3. 使用統計信息
# 4. 配置優化器參數
# 5. 實現自定義優化規則

# 你的程式碼在這裡
```

## 練習答案參考

由於性能調優練習需要實際的大數據環境和長時間的測試，這裡提供主要的實現思路和關鍵程式碼片段。

### 練習1解答要點

```python
# 性能基準測試
def benchmark_operations():
    df = generate_large_dataset(1000000)
    
    operations = {
        "Count": lambda: df.count(),
        "Group By": lambda: df.groupBy("category").count().collect(),
        "Filter + Count": lambda: df.filter(col("age") > 30).count(),
        "Complex Aggregation": lambda: df.groupBy("category").agg(
            avg("amount"), max("age"), min("amount")).collect()
    }
    
    results = {}
    for name, operation in operations.items():
        _, exec_time = monitor_performance(operation)
        results[name] = exec_time
    
    return results
```

### 練習2解答要點

```python
# 分區優化
def optimize_partitioning():
    large_df, small_df = create_test_datasets()
    
    # 測試不同分區策略
    strategies = {
        "Default": large_df,
        "Repartition by key": large_df.repartition("group_id"),
        "Repartition 100": large_df.repartition(100),
        "Coalesce 10": large_df.coalesce(10)
    }
    
    for name, df in strategies.items():
        print(f"\n{name}:")
        analyze_partitions(df)
        
        # 測試 JOIN 性能
        start_time = time.time()
        result = df.join(small_df, "group_id").count()
        join_time = time.time() - start_time
        print(f"JOIN 執行時間: {join_time:.2f} 秒")
```

## 練習提示

1. **性能監控**：
   - 使用 Spark UI 分析任務執行
   - 監控 CPU、記憶體、網路使用
   - 關注 GC 時間和頻率

2. **分區策略**：
   - 根據數據大小選擇分區數
   - 避免小文件問題
   - 考慮下游操作的需求

3. **緩存策略**：
   - 只緩存重複使用的數據
   - 選擇合適的存儲級別
   - 及時清理不需要的緩存

4. **SQL 優化**：
   - 使用列式存儲格式
   - 啟用自適應查詢執行
   - 收集和使用統計信息

## 學習檢核

完成練習後，你應該能夠：
- [ ] 分析和監控 Spark 應用性能
- [ ] 優化數據分區策略
- [ ] 實現有效的緩存策略
- [ ] 優化 SQL 查詢和執行計劃
- [ ] 處理性能瓶頸和數據傾斜
- [ ] 配置 Spark 集群參數