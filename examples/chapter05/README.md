# 第5章：Spark Streaming

## 📚 學習目標

- 理解流式處理的基本概念
- 掌握結構化流處理 (Structured Streaming)
- 學會與 Kafka 等消息系統的整合
- 了解實時數據處理的最佳實踐

## 🎯 本章內容

### 核心概念
- **Structured Streaming** - 結構化流處理
- **Watermark** - 水印機制
- **Trigger** - 觸發器
- **Output Mode** - 輸出模式

### 檔案說明
- `streaming_basics.py` - 流式處理基礎
- `kafka_streaming.py` - Kafka 整合示例

## 🚀 開始學習

### 執行範例

```bash
# 執行基礎流處理
poetry run python examples/chapter05/streaming_basics.py

# 執行 Kafka 整合範例
poetry run python examples/chapter05/kafka_streaming.py

# 或使用 Makefile
make run-chapter05
```

## 🔍 深入理解

### 流處理架構

```
數據源 → 流處理引擎 → 輸出接收器
  ↓         ↓           ↓
File     Structured   Console
Kafka    Streaming    File
Socket     Engine     Memory
```

### 基本流處理模式

#### 1. 數據攝取
```python
# 從文件讀取流
streaming_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("path", input_path) \
    .load()

# 從 Kafka 讀取流
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "topic_name") \
    .load()
```

#### 2. 流處理
```python
# 基本轉換
processed_df = streaming_df \
    .filter(col("status") == "active") \
    .withColumn("processed_time", current_timestamp())

# 聚合操作
aggregated_df = streaming_df \
    .groupBy("category") \
    .agg(count("*").alias("count"))
```

#### 3. 輸出
```python
# 輸出到控制台
query = processed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

# 輸出到文件
query = processed_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_path) \
    .start()
```

### 時間視窗操作

#### 1. 滾動視窗
```python
# 每5分鐘統計一次
windowed_df = streaming_df \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .agg(count("*").alias("count"))
```

#### 2. 滑動視窗
```python
# 每2分鐘統計過去10分鐘的數據
windowed_df = streaming_df \
    .groupBy(window(col("timestamp"), "10 minutes", "2 minutes")) \
    .agg(count("*").alias("count"))
```

#### 3. 水印處理
```python
# 處理延遲數據
watermarked_df = streaming_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .agg(count("*").alias("count"))
```

## 🎛️ 輸出模式

### 1. Append Mode
- 只輸出新增的行
- 適用於不可變數據

### 2. Update Mode
- 輸出更新的行
- 適用於聚合操作

### 3. Complete Mode
- 輸出完整結果
- 適用於小型聚合結果

## 🔧 觸發器配置

### 1. 處理時間觸發器
```python
# 每10秒觸發一次
.trigger(processingTime='10 seconds')
```

### 2. 一次性觸發器
```python
# 只執行一次
.trigger(once=True)
```

### 3. 連續觸發器
```python
# 連續處理
.trigger(continuous='1 second')
```

## 📊 Kafka 整合

### 1. 從 Kafka 讀取
```python
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "input_topic") \
    .option("startingOffsets", "latest") \
    .load()

# 解析 JSON 數據
parsed_df = kafka_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")
```

### 2. 寫入 Kafka
```python
query = processed_df.select(
    to_json(struct(*df.columns)).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "output_topic") \
    .start()
```

## 🛠️ 狀態管理

### 1. 有狀態操作
```python
# 累積統計
cumulative_df = streaming_df \
    .groupBy("user_id") \
    .agg(
        count("*").alias("total_events"),
        sum("amount").alias("total_amount")
    )
```

### 2. 去重操作
```python
# 基於鍵去重
deduped_df = streaming_df \
    .withWatermark("timestamp", "1 hour") \
    .dropDuplicates(["user_id", "event_id"])
```

## 📝 練習建議

### 基礎練習
1. 創建簡單的流處理應用
2. 嘗試不同的輸出模式
3. 實驗時間視窗操作

### 進階練習
1. 整合 Kafka 進行端到端流處理
2. 實現複雜的狀態管理
3. 處理延遲數據和水印

## 🛠️ 監控和調試

### 1. 查詢狀態
```python
# 查看查詢狀態
query.status

# 查看處理進度
query.lastProgress
```

### 2. 性能監控
```python
# 監控處理速度
query.lastProgress['inputRowsPerSecond']
query.lastProgress['processedRowsPerSecond']
```

### 3. 錯誤處理
```python
# 設置查詢監聽器
def on_query_progress(event):
    print(f"Batch: {event.progress.batchId}")
    print(f"Records: {event.progress.numInputRows}")

query.awaitTermination()
```

## 🔧 疑難排解

### 常見問題

**Q: 流處理查詢停止工作？**
A: 檢查檢查點目錄是否有權限問題，確保錯誤處理機制正常。

**Q: 處理延遲過高？**
A: 調整觸發器間隔，優化查詢邏輯，增加並行度。

**Q: 記憶體使用過高？**
A: 配置適當的水印，避免狀態無限增長。

## 💡 最佳實踐

1. **設置適當的水印** - 處理延遲數據
2. **選擇合適的觸發器** - 平衡延遲和吞吐量
3. **監控查詢性能** - 定期檢查處理指標
4. **錯誤恢復機制** - 實現容錯處理
5. **資源管理** - 適當配置執行器資源

## 📖 相關文檔

- [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Streaming Query Management](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/streaming.html)

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第6章：MLlib 機器學習](../chapter06/README.md)
- 了解機器學習在 Spark 中的應用
- 學習特徵工程和模型訓練

## 🎯 學習檢核

完成本章學習後，你應該能夠：
- [ ] 理解流式處理的基本概念
- [ ] 創建和管理流處理查詢
- [ ] 使用時間視窗進行聚合
- [ ] 整合 Kafka 進行實時數據處理
- [ ] 處理延遲數據和狀態管理

## 🗂️ 章節文件總覽

### streaming_basics.py
- 結構化流處理基礎
- 時間視窗操作
- 不同輸出模式
- 狀態管理示例

### kafka_streaming.py
- Kafka 整合配置
- 實時事件處理
- 端到端流處理管道
- 監控和警報系統

## 📋 環境準備

### Kafka 設置（可選）
```bash
# 下載 Kafka
wget https://downloads.apache.org/kafka/2.8.0/kafka_2.13-2.8.0.tgz

# 啟動 Kafka
bin/kafka-server-start.sh config/server.properties

# 創建主題
bin/kafka-topics.sh --create --topic input_topic --bootstrap-server localhost:9092
```

### 依賴包
```bash
# 安裝 Kafka 客戶端
pip install kafka-python

# 或使用 Poetry
poetry add kafka-python
```