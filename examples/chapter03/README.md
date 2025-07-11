# 第3章：DataFrame 和 Dataset API

## 📚 學習目標

- 理解 DataFrame 的概念和優勢
- 掌握 Schema 定義和操作
- 學會各種數據讀寫操作
- 熟悉 DataFrame 的常用操作

## 🎯 本章內容

### 核心概念
- **DataFrame** - 結構化數據抽象
- **Schema** - 數據結構定義
- **Column** - 列操作
- **Row** - 行數據

### 檔案說明
- `dataframe_basics.py` - DataFrame 基礎操作
- `schema_operations.py` - Schema 定義和類型轉換
- `data_io_operations.py` - 數據讀寫操作

## 🚀 開始學習

### 執行範例

```bash
# 執行所有範例
poetry run python examples/chapter03/dataframe_basics.py
poetry run python examples/chapter03/schema_operations.py
poetry run python examples/chapter03/data_io_operations.py

# 或使用 Makefile
make run-chapter03
```

## 🔍 深入理解

### DataFrame vs RDD

| 特性 | DataFrame | RDD |
|------|-----------|-----|
| 結構 | 結構化數據 | 非結構化數據 |
| 優化 | Catalyst 優化器 | 手動優化 |
| API | 高級 API | 低級 API |
| 性能 | 更高 | 相對較低 |
| 易用性 | 更易用 | 更靈活 |

### 主要操作類型

#### 1. 數據創建
```python
# 從列表創建
df = spark.createDataFrame(data, schema)

# 從文件創建
df = spark.read.json("path/to/file.json")
```

#### 2. 數據查詢
```python
# 選擇列
df.select("name", "age")

# 過濾行
df.filter(df.age > 25)

# 分組聚合
df.groupBy("department").avg("salary")
```

#### 3. 數據轉換
```python
# 添加列
df.withColumn("new_col", df.col1 + df.col2)

# 重命名列
df.withColumnRenamed("old_name", "new_name")

# 類型轉換
df.withColumn("age", df.age.cast("integer"))
```

## 📊 Schema 操作

### 定義 Schema
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True)
])
```

### Schema 驗證
```python
# 檢查 Schema
df.printSchema()

# 驗證數據類型
df.dtypes
```

## 💾 數據讀寫

### 支持的格式
- **JSON** - 半結構化數據
- **CSV** - 逗號分隔值
- **Parquet** - 列式存儲（推薦）
- **Avro** - 序列化格式
- **ORC** - 優化的行列存儲

### 讀取範例
```python
# JSON
df = spark.read.json("data.json")

# CSV
df = spark.read.option("header", "true").csv("data.csv")

# Parquet
df = spark.read.parquet("data.parquet")
```

### 寫入範例
```python
# 寫入 Parquet
df.write.mode("overwrite").parquet("output.parquet")

# 寫入 CSV
df.write.option("header", "true").csv("output.csv")

# 分區寫入
df.write.partitionBy("year").parquet("partitioned_data")
```

## 📝 練習建議

### 基礎練習
1. 創建不同類型的 DataFrame
2. 練習各種查詢操作
3. 嘗試不同的數據格式讀寫

### 進階練習
1. 設計複雜的 Schema
2. 處理嵌套數據結構
3. 實施數據清洗流程

## 🛠️ 實用技巧

### 1. 性能優化
```python
# 查看執行計劃
df.explain()

# 緩存 DataFrame
df.cache()

# 重分區
df.repartition(4)
```

### 2. 數據品質
```python
# 檢查空值
df.filter(df.column.isNull()).count()

# 去重
df.distinct()

# 統計描述
df.describe()
```

### 3. 複雜操作
```python
# 窗口函數
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window = Window.partitionBy("department").orderBy("salary")
df.withColumn("rank", row_number().over(window))
```

## 🔧 疑難排解

### 常見問題

**Q: 如何處理 Schema 不一致的問題？**
A: 使用 `spark.read.option("multiline", "true")` 或預定義 Schema。

**Q: 讀取大文件時記憶體不足？**
A: 調整分區數或使用流式讀取。

**Q: 如何處理特殊字符？**
A: 使用適當的編碼設置和轉義字符。

## 💡 最佳實踐

1. **使用 Parquet 格式** - 最佳的性能和壓縮比
2. **合理設計 Schema** - 避免過度複雜的嵌套結構
3. **利用分區** - 提高查詢性能
4. **數據驗證** - 確保數據品質
5. **適當緩存** - 對於重複使用的 DataFrame

## 📖 相關文檔

- [DataFrame API Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Data Sources](https://spark.apache.org/docs/latest/sql-data-sources.html)
- [PySpark SQL Functions](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html)

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第4章：Spark SQL](../chapter04/README.md)
- 深入學習 SQL 查詢語法
- 了解 Catalyst 優化器

## 🎯 學習檢核

完成本章學習後，你應該能夠：
- [ ] 創建和操作 DataFrame
- [ ] 定義和使用 Schema
- [ ] 進行各種數據讀寫操作
- [ ] 處理複雜的數據轉換
- [ ] 優化 DataFrame 性能

## 🗂️ 章節文件總覽

### dataframe_basics.py
- DataFrame 創建方法
- 基本查詢操作
- 數據轉換和清洗
- 聚合和分組操作

### schema_operations.py
- Schema 定義和驗證
- 類型轉換操作
- 複雜數據結構處理
- Schema 演化處理

### data_io_operations.py
- 多格式數據讀寫
- 分區策略
- 性能優化配置
- 錯誤處理機制