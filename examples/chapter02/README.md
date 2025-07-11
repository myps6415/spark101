# 第2章：Spark Core 基本操作

## 📚 學習目標

- 深入理解 RDD (Resilient Distributed Dataset) 概念
- 掌握 Transformation 和 Action 操作
- 學會 RDD 的創建和基本操作
- 理解 Spark 的分區機制

## 🎯 本章內容

### 核心概念
- **RDD** - Spark 的基本數據抽象
- **Transformation** - 轉換操作（延遲執行）
- **Action** - 行動操作（觸發計算）
- **分區** - 數據的分散式存儲單元

### 檔案說明
- `rdd_basics.py` - RDD 基礎操作示例

## 🚀 開始學習

### 執行範例

```bash
# 使用 Poetry（推薦）
poetry run python examples/chapter02/rdd_basics.py

# 或使用 Makefile
make run-chapter02
```

### 預期輸出
程式將展示：
- RDD 創建方法
- 基本 Transformation 操作
- 常用 Action 操作
- 鍵值對 RDD 操作

## 🔍 深入理解

### RDD 操作分類

#### Transformation 操作（延遲執行）
- `map()` - 元素級轉換
- `filter()` - 過濾操作
- `flatMap()` - 扁平化映射
- `groupByKey()` - 按鍵分組
- `reduceByKey()` - 按鍵聚合

#### Action 操作（觸發計算）
- `collect()` - 收集所有元素
- `count()` - 統計元素數量
- `take()` - 取前 N 個元素
- `reduce()` - 聚合操作
- `foreach()` - 遍歷操作

### 程式碼解析

```python
# 創建 RDD
numbers_rdd = sc.parallelize([1, 2, 3, 4, 5])

# Transformation（延遲執行）
even_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)

# Action（觸發計算）
result = even_rdd.collect()
```

## 📝 RDD 操作詳解

### 1. 創建 RDD
```python
# 從集合創建
rdd1 = sc.parallelize([1, 2, 3, 4, 5])

# 從文件創建
rdd2 = sc.textFile("path/to/file.txt")
```

### 2. 基本 Transformation
```python
# 映射操作
squared = numbers.map(lambda x: x ** 2)

# 過濾操作
evens = numbers.filter(lambda x: x % 2 == 0)

# 扁平化映射
words = lines.flatMap(lambda line: line.split())
```

### 3. 鍵值對操作
```python
# 創建鍵值對
pairs = numbers.map(lambda x: (x % 3, x))

# 按鍵分組
grouped = pairs.groupByKey()

# 按鍵聚合
summed = pairs.reduceByKey(lambda a, b: a + b)
```

## 📊 性能考慮

### 分區策略
- RDD 會自動分區
- 分區數影響並行度
- 可以手動控制分區

```python
# 查看分區數
print(f"分區數: {rdd.getNumPartitions()}")

# 重新分區
rdd_repartitioned = rdd.repartition(4)
```

### 延遲計算
- Transformation 操作不會立即執行
- 只有遇到 Action 操作才會觸發計算
- 這允許 Spark 優化執行計劃

## 📝 練習建議

### 基礎練習
1. 嘗試不同的 Transformation 操作
2. 實驗各種 Action 操作
3. 觀察延遲計算的效果

### 進階練習
1. 處理大型數據集
2. 實現自定義聚合操作
3. 優化分區策略

## 🛠️ 實用技巧

### 1. 性能監控
```python
# 查看 RDD 的執行計劃
rdd.toDebugString()

# 緩存頻繁使用的 RDD
rdd.cache()
```

### 2. 錯誤處理
```python
# 處理可能的異常
try:
    result = rdd.map(some_function).collect()
except Exception as e:
    print(f"處理錯誤: {e}")
```

## 🔧 疑難排解

### 常見問題

**Q: 為什麼 Transformation 操作這麼快？**
A: 因為 Transformation 是延遲執行的，只有在 Action 操作時才真正計算。

**Q: 如何查看 RDD 的內容？**
A: 使用 `collect()` 或 `take(n)` 操作，但要小心大數據集可能導致記憶體溢出。

**Q: 分區數如何影響性能？**
A: 分區數太少會降低並行度，太多會增加管理開銷。一般建議每個分區 100-200MB。

## 💡 最佳實踐

1. **合理使用緩存** - 對於多次使用的 RDD 進行緩存
2. **避免 shuffle** - 儘量減少需要 shuffle 的操作
3. **選擇合適的分區數** - 根據數據大小和集群資源調整
4. **使用 reduceByKey 而非 groupByKey** - 前者在 shuffle 前進行預聚合

## 📖 相關文檔

- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [PySpark RDD API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.html#rdd-apis)

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第3章：DataFrame 和 Dataset API](../chapter03/README.md)
- 了解 DataFrame 相比 RDD 的優勢
- 學習結構化數據處理

## 🎯 學習檢核

完成本章學習後，你應該能夠：
- [ ] 解釋 RDD 的基本概念
- [ ] 區分 Transformation 和 Action 操作
- [ ] 使用各種 RDD 操作處理數據
- [ ] 理解 Spark 的延遲計算機制
- [ ] 處理鍵值對 RDD