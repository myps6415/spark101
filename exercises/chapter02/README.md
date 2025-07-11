# 第2章練習：RDD 基本操作

## 練習目標
通過實際操作練習，深入理解 RDD 的概念、Transformation 和 Action 操作。

## 練習1：基本 RDD 操作

### 任務描述
使用 RDD 操作處理數字序列。

### 要求
1. 創建一個包含1到100的數字 RDD
2. 找出所有偶數
3. 計算偶數的平方
4. 統計偶數平方的總和
5. 找出最大的3個偶數平方值

### 程式碼模板

```python
from pyspark import SparkContext, SparkConf

# 創建 SparkContext
conf = SparkConf().setAppName("RDD基本操作練習").setMaster("local[*]")
sc = SparkContext(conf=conf)

# 創建數字 RDD
numbers = list(range(1, 101))
numbers_rdd = sc.parallelize(numbers)

# 完成以下任務：
# 1. 找出所有偶數
# 2. 計算偶數的平方
# 3. 計算總和
# 4. 找出最大的3個值

# 你的程式碼在這裡

# 停止 SparkContext
sc.stop()
```

### 預期輸出
- 偶數總數：50
- 偶數平方總和：171700
- 最大的3個偶數平方值：[10000, 9604, 9216]

## 練習2：文本處理

### 任務描述
使用 RDD 處理文本數據，實現詞頻統計。

### 要求
1. 創建包含多行文本的 RDD
2. 將文本分割成單詞
3. 過濾掉長度小於3的單詞
4. 統計每個單詞的出現次數
5. 找出出現次數最多的前5個單詞

### 程式碼模板

```python
# 文本數據
texts = [
    "Apache Spark is a unified analytics engine for large-scale data processing",
    "Spark provides high-level APIs in Java, Scala, Python and R",
    "Spark runs on Hadoop, Apache Mesos, Kubernetes, standalone, or in the cloud",
    "Spark is built on the concept of resilient distributed datasets (RDDs)",
    "RDDs are fault-tolerant collections of elements that can be operated on in parallel",
    "Spark SQL is a Spark module for structured data processing",
    "Spark Streaming enables scalable, high-throughput, fault-tolerant stream processing",
    "MLlib is Apache Spark's machine learning library",
    "GraphX is Apache Spark's API for graphs and graph-parallel computation"
]

# 創建文本 RDD
text_rdd = sc.parallelize(texts)

# 完成以下任務：
# 1. 分割成單詞
# 2. 轉換為小寫
# 3. 過濾短單詞
# 4. 統計詞頻
# 5. 找出高頻詞

# 你的程式碼在這裡
```

### 預期輸出
- 總單詞數（去重後）
- 詞頻統計前5名
- 最長的單詞

## 練習3：鍵值對 RDD 操作

### 任務描述
使用鍵值對 RDD 處理銷售數據。

### 要求
1. 創建銷售記錄 RDD
2. 計算每個產品的總銷售額
3. 找出銷售額最高的產品
4. 計算每個地區的銷售統計
5. 找出每個地區最受歡迎的產品

### 程式碼模板

```python
# 銷售數據 (產品, 地區, 銷售額)
sales_data = [
    ("iPhone", "North", 25000),
    ("iPhone", "South", 30000),
    ("iPhone", "East", 22000),
    ("iPhone", "West", 28000),
    ("Samsung", "North", 18000),
    ("Samsung", "South", 22000),
    ("Samsung", "East", 20000),
    ("Samsung", "West", 24000),
    ("Huawei", "North", 15000),
    ("Huawei", "South", 18000),
    ("Huawei", "East", 16000),
    ("Huawei", "West", 20000),
    ("Xiaomi", "North", 12000),
    ("Xiaomi", "South", 15000),
    ("Xiaomi", "East", 13000),
    ("Xiaomi", "West", 17000)
]

# 創建銷售 RDD
sales_rdd = sc.parallelize(sales_data)

# 完成以下任務：
# 1. 計算每個產品的總銷售額
# 2. 計算每個地區的總銷售額
# 3. 找出銷售額最高的產品
# 4. 找出每個地區最受歡迎的產品

# 你的程式碼在這裡
```

## 練習4：日誌分析

### 任務描述
使用 RDD 分析網站訪問日誌。

### 要求
1. 解析日誌格式
2. 統計不同狀態碼的請求數
3. 找出最常訪問的頁面
4. 統計每小時的訪問量
5. 識別可能的異常訪問

### 程式碼模板

```python
# 模擬日誌數據
log_data = [
    "192.168.1.100 - - [01/Jan/2024:10:00:00 +0000] \"GET /index.html HTTP/1.1\" 200 2048",
    "192.168.1.101 - - [01/Jan/2024:10:00:01 +0000] \"GET /about.html HTTP/1.1\" 200 1024",
    "192.168.1.102 - - [01/Jan/2024:10:00:02 +0000] \"POST /login HTTP/1.1\" 200 512",
    "192.168.1.103 - - [01/Jan/2024:10:00:03 +0000] \"GET /products.html HTTP/1.1\" 200 4096",
    "192.168.1.104 - - [01/Jan/2024:10:00:04 +0000] \"GET /contact.html HTTP/1.1\" 404 256",
    "192.168.1.105 - - [01/Jan/2024:11:00:00 +0000] \"GET /api/users HTTP/1.1\" 500 128",
    "192.168.1.106 - - [01/Jan/2024:11:00:01 +0000] \"PUT /api/products/1 HTTP/1.1\" 201 1024",
    "192.168.1.107 - - [01/Jan/2024:11:00:02 +0000] \"DELETE /api/products/2 HTTP/1.1\" 204 0",
    "192.168.1.108 - - [01/Jan/2024:11:00:03 +0000] \"GET /search?q=laptop HTTP/1.1\" 200 8192",
    "192.168.1.109 - - [01/Jan/2024:12:00:00 +0000] \"GET /admin/dashboard HTTP/1.1\" 403 256"
]

# 創建日誌 RDD
log_rdd = sc.parallelize(log_data)

# 完成以下任務：
# 1. 解析日誌格式
# 2. 統計狀態碼
# 3. 找出熱門頁面
# 4. 統計每小時訪問量
# 5. 識別異常訪問

# 你的程式碼在這裡
```

## 練習答案

### 練習1解答

```python
from pyspark import SparkContext, SparkConf

# 創建 SparkContext
conf = SparkConf().setAppName("RDD基本操作練習").setMaster("local[*]")
sc = SparkContext(conf=conf)

# 創建數字 RDD
numbers = list(range(1, 101))
numbers_rdd = sc.parallelize(numbers)

# 1. 找出所有偶數
even_numbers = numbers_rdd.filter(lambda x: x % 2 == 0)
print(f"偶數總數: {even_numbers.count()}")

# 2. 計算偶數的平方
even_squares = even_numbers.map(lambda x: x ** 2)
print(f"偶數平方: {even_squares.take(10)}")

# 3. 計算總和
total_sum = even_squares.reduce(lambda x, y: x + y)
print(f"偶數平方總和: {total_sum}")

# 4. 找出最大的3個值
top_three = even_squares.top(3)
print(f"最大的3個偶數平方值: {top_three}")

# 停止 SparkContext
sc.stop()
```

### 練習2解答

```python
# 創建文本 RDD
text_rdd = sc.parallelize(texts)

# 1. 分割成單詞並轉換為小寫
words_rdd = text_rdd.flatMap(lambda line: line.lower().split())

# 2. 過濾短單詞和標點符號
import re
filtered_words = words_rdd.filter(lambda word: len(word) >= 3 and re.match(r'^[a-z]+$', word))

# 3. 統計詞頻
word_counts = filtered_words.map(lambda word: (word, 1)) \
                          .reduceByKey(lambda a, b: a + b)

# 4. 找出高頻詞
top_words = word_counts.takeOrdered(5, key=lambda x: -x[1])
print("詞頻統計前5名:")
for word, count in top_words:
    print(f"  {word}: {count}")

# 5. 找出最長的單詞
longest_word = filtered_words.reduce(lambda a, b: a if len(a) > len(b) else b)
print(f"最長的單詞: {longest_word}")

# 總單詞數（去重後）
unique_words = filtered_words.distinct().count()
print(f"總單詞數（去重後）: {unique_words}")
```

### 練習3解答

```python
# 創建銷售 RDD
sales_rdd = sc.parallelize(sales_data)

# 1. 計算每個產品的總銷售額
product_sales = sales_rdd.map(lambda x: (x[0], x[2])) \
                        .reduceByKey(lambda a, b: a + b)

print("每個產品的總銷售額:")
for product, sales in product_sales.collect():
    print(f"  {product}: ${sales}")

# 2. 計算每個地區的總銷售額
region_sales = sales_rdd.map(lambda x: (x[1], x[2])) \
                       .reduceByKey(lambda a, b: a + b)

print("\n每個地區的總銷售額:")
for region, sales in region_sales.collect():
    print(f"  {region}: ${sales}")

# 3. 找出銷售額最高的產品
top_product = product_sales.reduce(lambda a, b: a if a[1] > b[1] else b)
print(f"\n銷售額最高的產品: {top_product[0]} (${top_product[1]})")

# 4. 找出每個地區最受歡迎的產品
region_product_sales = sales_rdd.map(lambda x: ((x[1], x[0]), x[2])) \
                               .reduceByKey(lambda a, b: a + b)

region_top_products = region_product_sales.map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                                         .groupByKey() \
                                         .mapValues(lambda x: max(x, key=lambda y: y[1]))

print("\n每個地區最受歡迎的產品:")
for region, (product, sales) in region_top_products.collect():
    print(f"  {region}: {product} (${sales})")
```

### 練習4解答

```python
import re

# 創建日誌 RDD
log_rdd = sc.parallelize(log_data)

# 1. 解析日誌格式
def parse_log_line(line):
    # 使用正則表達式解析日誌
    pattern = r'(\S+) - - \[(.*?)\] "(\S+) (\S+) \S+" (\d+) (\d+)'
    match = re.match(pattern, line)
    if match:
        ip, timestamp, method, url, status, size = match.groups()
        return (ip, timestamp, method, url, int(status), int(size))
    return None

parsed_logs = log_rdd.map(parse_log_line).filter(lambda x: x is not None)

# 2. 統計狀態碼
status_counts = parsed_logs.map(lambda x: (x[4], 1)) \
                          .reduceByKey(lambda a, b: a + b)

print("狀態碼統計:")
for status, count in status_counts.collect():
    print(f"  {status}: {count}")

# 3. 找出熱門頁面
page_counts = parsed_logs.map(lambda x: (x[3], 1)) \
                        .reduceByKey(lambda a, b: a + b)

top_pages = page_counts.takeOrdered(3, key=lambda x: -x[1])
print("\n熱門頁面:")
for page, count in top_pages:
    print(f"  {page}: {count}")

# 4. 統計每小時訪問量
def extract_hour(timestamp):
    # 從時間戳中提取小時
    return timestamp.split(':')[1]

hour_counts = parsed_logs.map(lambda x: (extract_hour(x[1]), 1)) \
                        .reduceByKey(lambda a, b: a + b)

print("\n每小時訪問量:")
for hour, count in sorted(hour_counts.collect()):
    print(f"  {hour}:00: {count}")

# 5. 識別異常訪問（錯誤狀態碼）
error_logs = parsed_logs.filter(lambda x: x[4] >= 400)
error_ips = error_logs.map(lambda x: (x[0], 1)) \
                     .reduceByKey(lambda a, b: a + b)

print("\n異常訪問 IP:")
for ip, count in error_ips.collect():
    print(f"  {ip}: {count} 次錯誤")
```

## 練習提示

1. **理解延遲執行**：Transformation 操作不會立即執行，直到遇到 Action 操作
2. **合理使用 collect()**：對於大數據集，避免使用 collect()，改用 take() 或 sample()
3. **善用 cache()**：對於重複使用的 RDD，使用 cache() 可以提高性能
4. **注意分區**：了解 RDD 的分區對性能的影響
5. **錯誤處理**：在實際應用中，要處理可能的解析錯誤

## 進階挑戰

1. **性能優化**：
   - 比較不同分區數對性能的影響
   - 使用 `repartition()` 和 `coalesce()` 優化分區

2. **複雜數據處理**：
   - 處理嵌套數據結構
   - 實現自定義的聚合函數

3. **容錯測試**：
   - 模擬節點故障
   - 測試 RDD 的容錯機制

4. **大數據集**：
   - 使用更大的數據集測試性能
   - 學習如何處理記憶體限制

## 學習檢核

完成練習後，你應該能夠：
- [ ] 理解 RDD 的基本概念
- [ ] 區分 Transformation 和 Action 操作
- [ ] 熟練使用 map, filter, reduce 等操作
- [ ] 處理鍵值對 RDD
- [ ] 實現詞頻統計等經典算法
- [ ] 分析和處理日誌數據
- [ ] 理解 RDD 的分區和緩存機制