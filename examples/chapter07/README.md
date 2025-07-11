# 第7章：性能調優

## 📚 學習目標

- 理解 Spark 性能優化的核心原則
- 掌握記憶體管理和資源配置
- 學會分區策略和數據傾斜處理
- 了解監控和調試技巧

## 🎯 本章內容

### 核心概念
- **Performance Tuning** - 性能調優
- **Memory Management** - 記憶體管理
- **Data Partitioning** - 數據分區
- **Cache Strategy** - 緩存策略

### 檔案說明
- `performance_tuning.py` - 性能調優實戰示例

## 🚀 開始學習

### 執行範例

```bash
# 執行性能調優範例
poetry run python examples/chapter07/performance_tuning.py

# 或使用 Makefile
make run-chapter07
```

## 🔍 深入理解

### 性能調優框架

```
應用分析 → 瓶頸識別 → 優化策略 → 效果驗證
    ↓         ↓         ↓         ↓
  Profile   Bottleneck  Optimize  Measure
```

### 主要優化領域

#### 1. 資源配置
```python
# Spark 配置優化
spark = SparkSession.builder \
    .appName("Performance Tuning") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.instances", "10") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()
```

#### 2. 記憶體管理
```python
# 緩存策略
from pyspark import StorageLevel

# 記憶體緩存
df.cache()

# 記憶體+磁碟緩存
df.persist(StorageLevel.MEMORY_AND_DISK)

# 序列化緩存
df.persist(StorageLevel.MEMORY_ONLY_SER)
```

#### 3. 分區優化
```python
# 重新分區
df_repartitioned = df.repartition(4)

# 按列分區
df_partitioned = df.repartition(col("category"))

# 合併分區
df_coalesced = df.coalesce(2)
```

## 🛠️ 關鍵優化技術

### 1. 廣播變數
```python
from pyspark.sql.functions import broadcast

# 廣播小表
result = large_df.join(broadcast(small_df), "key")
```

### 2. 自適應查詢執行 (AQE)
```python
# 啟用 AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### 3. 數據傾斜處理
```python
# 加鹽技術
salted_df = df.withColumn("salted_key", 
                         concat(col("key"), lit("_"), (rand() * 10).cast("int")))

# 預聚合
pre_aggregated = df.groupBy("key").agg(count("*").alias("count"))
```

## 📊 監控和診斷

### 1. Spark UI 監控
- **Jobs** - 作業執行狀態
- **Stages** - 階段執行詳情
- **Storage** - 緩存使用情況
- **Executors** - 執行器資源使用

### 2. 執行計劃分析
```python
# 查看執行計劃
df.explain()

# 詳細執行計劃
df.explain(True)

# 查看成本
df.explain("cost")
```

### 3. 性能指標
```python
# 測量執行時間
import time

start_time = time.time()
result = df.count()
execution_time = time.time() - start_time
print(f"執行時間: {execution_time:.2f} 秒")
```

## 🎯 優化策略

### 1. 數據格式優化
```python
# 使用 Parquet 格式
df.write.parquet("optimized_data.parquet")

# 啟用壓縮
df.write.option("compression", "snappy").parquet("compressed_data.parquet")
```

### 2. 查詢優化
```python
# 謂詞下推
df.filter(col("date") >= "2024-01-01").select("id", "name")

# 列修剪
df.select("id", "name").filter(col("status") == "active")
```

### 3. 連接優化
```python
# 選擇合適的連接類型
# 廣播連接 - 小表
df1.join(broadcast(df2), "key")

# 排序合併連接 - 大表
df1.join(df2, "key")
```

## 📈 記憶體調優

### 1. 記憶體配置
```python
# 執行器記憶體
spark.conf.set("spark.executor.memory", "4g")

# 驅動程式記憶體
spark.conf.set("spark.driver.memory", "2g")

# 記憶體分數
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
```

### 2. 垃圾回收優化
```python
# G1 垃圾回收器
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

# 堆外記憶體
spark.conf.set("spark.executor.memoryOffHeap.enabled", "true")
spark.conf.set("spark.executor.memoryOffHeap.size", "1g")
```

## 🔧 網路和 I/O 優化

### 1. 序列化優化
```python
# 使用 Kryo 序列化
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

### 2. Shuffle 優化
```python
# Shuffle 分區數
spark.conf.set("spark.sql.shuffle.partitions", "400")

# Shuffle 壓縮
spark.conf.set("spark.sql.adaptive.shuffle.targetPostShuffleInputSize", "67108864")
```

## 📝 性能測試

### 1. 基準測試
```python
def benchmark_operation(operation, description):
    start_time = time.time()
    result = operation()
    end_time = time.time()
    print(f"{description}: {end_time - start_time:.2f} 秒")
    return result

# 測試不同的緩存策略
benchmark_operation(lambda: df.count(), "不使用緩存")
df.cache()
benchmark_operation(lambda: df.count(), "使用緩存")
```

### 2. 資源使用監控
```python
# 獲取執行器信息
executor_infos = spark.sparkContext.statusTracker().getExecutorInfos()
for executor in executor_infos:
    print(f"執行器 {executor.executorId}: {executor.memoryUsed}/{executor.maxMemory}")
```

## 💡 最佳實踐

### 1. 數據格式和壓縮
- 使用 Parquet 格式
- 啟用適當的壓縮
- 避免小文件問題

### 2. 分區策略
- 合理設置分區數
- 使用數據分區
- 避免數據傾斜

### 3. 緩存策略
- 緩存重複使用的數據
- 選擇適當的存儲級別
- 及時清理不需要的緩存

### 4. 資源配置
- 根據集群資源調整配置
- 平衡執行器數量和資源
- 監控資源使用情況

## 🔧 疑難排解

### 常見問題

**Q: 作業執行緩慢？**
A: 檢查分區策略、數據傾斜、資源配置。

**Q: 記憶體不足錯誤？**
A: 增加執行器記憶體、調整分區數、優化數據結構。

**Q: Shuffle 操作很慢？**
A: 減少 Shuffle 操作、使用廣播連接、優化分區策略。

## 📊 性能調優檢核表

### 配置檢查
- [ ] 執行器記憶體和核心數配置合理
- [ ] 啟用自適應查詢執行
- [ ] 序列化器設置為 Kryo
- [ ] 合適的 Shuffle 分區數

### 程式碼檢查
- [ ] 使用 Parquet 格式
- [ ] 合理的緩存策略
- [ ] 避免不必要的 Shuffle
- [ ] 廣播小表連接

### 監控檢查
- [ ] 定期查看 Spark UI
- [ ] 監控資源使用情況
- [ ] 檢查執行計劃
- [ ] 測量關鍵操作性能

## 📖 相關文檔

- [Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)
- [Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Monitoring](https://spark.apache.org/docs/latest/monitoring.html)

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第8章：實戰項目](../chapter08/README.md)
- 應用所學知識到實際項目
- 建立完整的生產級應用

## 🎯 學習檢核

完成本章學習後，你應該能夠：
- [ ] 識別 Spark 應用的性能瓶頸
- [ ] 配置適當的資源和記憶體設置
- [ ] 實施有效的分區和緩存策略
- [ ] 處理數據傾斜問題
- [ ] 監控和調試 Spark 應用

## 🗂️ 章節文件總覽

### performance_tuning.py
- 分區策略優化
- 緩存策略比較
- 廣播變數應用
- 數據傾斜處理
- 執行計劃分析
- 性能監控技巧
- 資源配置建議