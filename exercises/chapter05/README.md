# 第5章練習：Spark Streaming 實時數據處理

## 練習目標
學習 Spark Structured Streaming 的核心概念，實現實時數據處理管道，掌握流式數據的窗口操作和狀態管理。

## 練習1：基本流處理操作

### 任務描述
建立基本的 Structured Streaming 應用，處理模擬的實時數據流。

### 要求
1. 創建基本的流式 DataFrame
2. 實現簡單的數據轉換
3. 使用不同的輸出模式
4. 實現流式數據的監控
5. 處理流式數據的錯誤和重啟

### 程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import time
import json
import random
from threading import Thread
import tempfile
import os

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("Spark Streaming基礎練習") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# 設置日誌級別
spark.sparkContext.setLogLevel("WARN")

# 定義用戶活動數據結構
activity_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("activity_type", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("page_url", StringType(), True),
    StructField("session_id", StringType(), True)
])

# 模擬數據生成器
def generate_activity_data():
    activities = ["login", "view_page", "click_button", "purchase", "logout"]
    pages = ["/home", "/products", "/cart", "/checkout", "/profile"]
    
    return {
        "user_id": f"user_{random.randint(1, 100)}",
        "activity_type": random.choice(activities),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "page_url": random.choice(pages),
        "session_id": f"session_{random.randint(1, 50)}"
    }

# 完成以下任務：
# 1. 創建文件流輸入源
# 2. 實現基本的流式轉換
# 3. 設置不同的輸出模式
# 4. 實現流式查詢監控
# 5. 處理流式數據的容錯

# 你的程式碼在這裡

# 示例：創建基本的流處理
def create_basic_stream():
    # 創建輸入流
    input_stream = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .load()
    
    # 基本轉換
    processed_stream = input_stream \
        .select(
            col("timestamp"),
            col("value"),
            (col("value") % 10).alias("category")
        ) \
        .where(col("value") % 2 == 0)
    
    # 輸出到控制台
    query = processed_stream \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    return query

# 停止 Spark
# spark.stop()
```

### 預期輸出
- 流式數據的實時處理結果
- 不同輸出模式的差異展示
- 流處理監控信息

## 練習2：窗口操作和時間處理

### 任務描述
學習在流式數據中使用時間窗口進行聚合計算。

### 要求
1. 實現滑動窗口和滾動窗口
2. 處理事件時間和處理時間
3. 實現延遲數據處理
4. 使用 Watermark 處理亂序數據
5. 創建複雜的時間窗口分析

### 程式碼模板

```python
from pyspark.sql.functions import window, col, count, sum as spark_sum, avg, max as spark_max

# 網站點擊事件數據
def create_clickstream_data():
    # 模擬點擊流數據
    click_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("page_id", StringType(), True),
        StructField("event_time", TimestampType(), True),
        StructField("session_id", StringType(), True),
        StructField("ip_address", StringType(), True)
    ])
    
    # 創建記憶體流（用於測試）
    clicks_df = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 10) \
        .load() \
        .select(
            concat(lit("user_"), (col("value") % 20).cast("string")).alias("user_id"),
            concat(lit("page_"), (col("value") % 5).cast("string")).alias("page_id"),
            col("timestamp").alias("event_time"),
            concat(lit("session_"), (col("value") % 10).cast("string")).alias("session_id"),
            concat(lit("192.168.1."), (col("value") % 255).cast("string")).alias("ip_address")
        )
    
    return clicks_df

# 完成以下任務：
# 1. 實現5分鐘滾動窗口統計
# 2. 創建10分鐘滑動窗口（每5分鐘更新）
# 3. 處理延遲到達的數據
# 4. 實現會話窗口分析
# 5. 多層次時間聚合

# 你的程式碼在這裡

def window_analysis_example():
    clicks_stream = create_clickstream_data()
    
    # 1. 滾動窗口統計
    rolling_window_stats = clicks_stream \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("page_id")
        ) \
        .agg(
            count("*").alias("click_count"),
            countDistinct("user_id").alias("unique_users")
        )
    
    # 2. 滑動窗口統計
    sliding_window_stats = clicks_stream \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "10 minutes", "5 minutes"),
            col("page_id")
        ) \
        .agg(
            count("*").alias("click_count"),
            countDistinct("user_id").alias("unique_users")
        )
    
    return rolling_window_stats, sliding_window_stats
```

## 練習3：狀態管理和複雜事件處理

### 任務描述
學習在流處理中管理狀態，實現複雜的事件處理邏輯。

### 要求
1. 使用 mapGroupsWithState 進行有狀態處理
2. 實現用戶會話分析
3. 檢測異常事件模式
4. 實現事件去重
5. 創建實時推薦系統

### 程式碼模板

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from typing import Iterator, Tuple

# 用戶會話狀態
class UserSession:
    def __init__(self):
        self.session_start = None
        self.last_activity = None
        self.page_views = []
        self.total_duration = 0
        
    def update(self, activity):
        if self.session_start is None:
            self.session_start = activity['timestamp']
        self.last_activity = activity['timestamp']
        self.page_views.append(activity['page_url'])
        
    def is_expired(self, current_time, timeout_minutes=30):
        if self.last_activity is None:
            return False
        return (current_time - self.last_activity).total_seconds() > timeout_minutes * 60

# 完成以下任務：
# 1. 實現用戶會話追蹤
# 2. 檢測異常登錄行為
# 3. 實現實時去重
# 4. 創建事件序列檢測
# 5. 實現流式機器學習預測

# 你的程式碼在這裡

def session_analysis():
    # 用戶活動數據流
    activities = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 2) \
        .load() \
        .select(
            concat(lit("user_"), (col("value") % 10).cast("string")).alias("user_id"),
            col("timestamp").alias("activity_time"),
            array(lit("/home"), lit("/products"), lit("/cart")).getItem(
                (col("value") % 3).cast("int")
            ).alias("page")
        )
    
    # 狀態管理函數
    def update_user_session(user_id: str, activities: Iterator, state: GroupState) -> Iterator:
        # 實現會話更新邏輯
        if state.exists:
            session = state.get
        else:
            session = {"start_time": None, "page_count": 0, "last_page": None}
        
        activity_list = list(activities)
        for activity in activity_list:
            if session["start_time"] is None:
                session["start_time"] = activity["activity_time"]
            session["page_count"] += 1
            session["last_page"] = activity["page"]
        
        state.update(session)
        
        # 返回會話摘要
        yield {
            "user_id": user_id,
            "session_pages": session["page_count"],
            "last_activity": session["last_page"]
        }
    
    return activities
```

## 練習4：實時數據管道和 Kafka 整合

### 任務描述
建立完整的實時數據管道，整合 Kafka 作為消息隊列。

### 要求
1. 設置 Kafka 生產者和消費者
2. 實現多 Topic 數據處理
3. 創建實時 ETL 管道
4. 實現流式數據的 Join 操作
5. 建立實時監控儀表板

### 程式碼模板

```python
# 注意：此練習需要 Kafka 環境，可以使用 Docker 快速設置

# Kafka 配置
kafka_bootstrap_servers = "localhost:9092"
kafka_topics = {
    "user_events": "user_activity_topic",
    "transactions": "transaction_topic",
    "inventory": "inventory_topic"
}

# 完成以下任務：
# 1. 從 Kafka 讀取多個 Topic
# 2. 實現實時數據清洗和轉換
# 3. 執行流式 Join 操作
# 4. 輸出到不同的下游系統
# 5. 實現容錯和恢復機制

def kafka_streaming_pipeline():
    # 用戶事件流
    user_events = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topics["user_events"]) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), user_event_schema).alias("data")
        ) \
        .select("key", "data.*")
    
    # 交易數據流
    transactions = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topics["transactions"]) \
        .option("startingOffsets", "latest") \
        .load() \
        .select(
            col("key").cast("string"),
            from_json(col("value").cast("string"), transaction_schema).alias("data")
        ) \
        .select("key", "data.*")
    
    # 實時 Join 操作
    enriched_data = user_events \
        .withWatermark("event_time", "10 minutes") \
        .join(
            transactions.withWatermark("transaction_time", "10 minutes"),
            expr("""
                user_id = customer_id AND
                event_time BETWEEN transaction_time - INTERVAL 5 MINUTES
                                AND transaction_time + INTERVAL 5 MINUTES
            """)
        )
    
    return enriched_data

# 示例 Schema 定義
user_event_schema = StructType([
    StructField("user_id", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", TimestampType()),
    StructField("page_url", StringType())
])

transaction_schema = StructType([
    StructField("transaction_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("transaction_time", TimestampType()),
    StructField("amount", DoubleType()),
    StructField("product_id", StringType())
])

# 你的程式碼在這裡
```

## 練習答案

### 練習1解答

```python
import tempfile
import shutil
import json
import time
from threading import Thread

# 創建臨時目錄用於文件流
temp_dir = tempfile.mkdtemp()
input_path = f"{temp_dir}/input"
checkpoint_path = f"{temp_dir}/checkpoint"
os.makedirs(input_path, exist_ok=True)

def generate_data_files():
    """生成測試數據文件"""
    for i in range(10):
        data = []
        for j in range(5):
            data.append(generate_activity_data())
        
        filename = f"{input_path}/data_{i:03d}.json"
        with open(filename, 'w') as f:
            for record in data:
                f.write(json.dumps(record) + '\n')
        
        time.sleep(2)  # 每2秒生成一個文件

# 啟動數據生成線程
data_thread = Thread(target=generate_data_files)
data_thread.daemon = True
data_thread.start()

# 1. 創建文件流
file_stream = spark \
    .readStream \
    .schema(activity_schema) \
    .option("maxFilesPerTrigger", 1) \
    .json(input_path)

print("開始處理文件流:")

# 2. 基本轉換
processed_stream = file_stream \
    .withColumn("hour", hour(col("timestamp"))) \
    .withColumn("is_purchase", col("activity_type") == "purchase") \
    .filter(col("activity_type").isin(["login", "purchase", "view_page"]))

# 3. 輸出模式示例
# Append 模式
append_query = processed_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="5 seconds") \
    .start()

print("文件流處理已啟動，運行10秒...")
time.sleep(10)
append_query.stop()

# 4. 聚合查詢（Complete 模式）
aggregated_stream = file_stream \
    .groupBy("activity_type") \
    .agg(
        count("*").alias("count"),
        countDistinct("user_id").alias("unique_users")
    )

complete_query = aggregated_stream \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .trigger(processingTime="5 seconds") \
    .start()

print("聚合查詢已啟動，運行10秒...")
time.sleep(10)
complete_query.stop()

# 清理資源
shutil.rmtree(temp_dir)
```

### 練習2解答

```python
# 創建點擊流數據
clicks_stream = create_clickstream_data()

# 1. 滾動窗口統計（每5分鐘）
print("滾動窗口統計:")
rolling_stats = clicks_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "5 minutes"),
        col("page_id")
    ) \
    .agg(
        count("*").alias("clicks"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("unique_sessions")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("page_id"),
        col("clicks"),
        col("unique_users"),
        col("unique_sessions")
    )

rolling_query = rolling_stats \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="30 seconds") \
    .start()

# 2. 滑動窗口統計（10分鐘窗口，每5分鐘滑動）
print("滑動窗口統計:")
sliding_stats = clicks_stream \
    .withWatermark("event_time", "15 minutes") \
    .groupBy(
        window(col("event_time"), "10 minutes", "5 minutes"),
        col("page_id")
    ) \
    .agg(
        count("*").alias("clicks"),
        countDistinct("user_id").alias("unique_users")
    ) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("page_id"),
        col("clicks"),
        col("unique_users"),
        (col("clicks").cast("double") / col("unique_users")).alias("clicks_per_user")
    )

sliding_query = sliding_stats \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="30 seconds") \
    .start()

# 3. 多層次時間聚合
multi_level_stats = clicks_stream \
    .withWatermark("event_time", "10 minutes") \
    .groupBy(
        window(col("event_time"), "1 minute").alias("minute_window"),
        window(col("event_time"), "5 minutes").alias("five_minute_window"),
        col("page_id")
    ) \
    .agg(count("*").alias("clicks"))

# 運行查詢
print("窗口查詢運行中...")
time.sleep(30)

rolling_query.stop()
sliding_query.stop()
```

### 練習3解答

```python
from pyspark.sql.streaming import GroupState, GroupStateTimeout

# 定義會話狀態
session_timeout_minutes = 30

def update_session_state(user_id, activities, state: GroupState):
    """更新用戶會話狀態"""
    
    # 獲取或初始化狀態
    if state.exists:
        session_data = state.get
    else:
        session_data = {
            "session_start": None,
            "last_activity": None,
            "page_views": [],
            "activity_count": 0
        }
    
    # 處理新活動
    activities_list = list(activities)
    for activity in activities_list:
        if session_data["session_start"] is None:
            session_data["session_start"] = activity["activity_time"]
        
        session_data["last_activity"] = activity["activity_time"]
        session_data["page_views"].append(activity["page"])
        session_data["activity_count"] += 1
    
    # 更新狀態
    state.update(session_data)
    
    # 設置超時
    state.setTimeoutDuration(f"{session_timeout_minutes} minutes")
    
    # 返回會話摘要
    if activities_list:
        yield Row(
            user_id=user_id,
            session_start=session_data["session_start"],
            last_activity=session_data["last_activity"],
            total_pages=len(set(session_data["page_views"])),
            activity_count=session_data["activity_count"]
        )

# 創建狀態化的流處理
activities_with_state = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 3) \
    .load() \
    .select(
        concat(lit("user_"), (col("value") % 5).cast("string")).alias("user_id"),
        col("timestamp").alias("activity_time"),
        array(lit("/home"), lit("/products"), lit("/cart")).getItem(
            (col("value") % 3).cast("int")
        ).alias("page")
    )

# 應用狀態管理
session_summaries = activities_with_state \
    .groupByKey(lambda x: x.user_id) \
    .mapGroupsWithState(
        update_session_state,
        GroupStateTimeout.ProcessingTimeTimeout
    )

# 異常檢測示例
def detect_anomalies(df):
    return df \
        .withColumn("is_rapid_clicks", col("activity_count") > 10) \
        .withColumn("session_duration_minutes", 
                   (col("last_activity").cast("long") - col("session_start").cast("long")) / 60) \
        .withColumn("is_long_session", col("session_duration_minutes") > 60)

anomaly_detection = session_summaries \
    .transform(detect_anomalies) \
    .filter(col("is_rapid_clicks") | col("is_long_session"))

print("會話分析和異常檢測:")
state_query = anomaly_detection \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

time.sleep(60)
state_query.stop()
```

### 練習4解答

```python
# 模擬 Kafka 流（使用 rate source）
def simulate_kafka_streams():
    # 模擬用戶事件流
    user_events_sim = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .load() \
        .select(
            concat(lit("user_"), (col("value") % 20).cast("string")).alias("user_id"),
            array(lit("login"), lit("view"), lit("click")).getItem(
                (col("value") % 3).cast("int")
            ).alias("event_type"),
            col("timestamp").alias("event_time"),
            concat(lit("/page_"), (col("value") % 10).cast("string")).alias("page_url")
        )
    
    # 模擬交易流
    transactions_sim = spark \
        .readStream \
        .format("rate") \
        .option("rowsPerSecond", 2) \
        .load() \
        .select(
            concat(lit("txn_"), col("value").cast("string")).alias("transaction_id"),
            concat(lit("user_"), (col("value") % 20).cast("string")).alias("customer_id"),
            col("timestamp").alias("transaction_time"),
            (rand() * 1000 + 10).alias("amount"),
            concat(lit("prod_"), (col("value") % 50).cast("string")).alias("product_id")
        )
    
    return user_events_sim, transactions_sim

# 創建模擬流
user_events, transactions = simulate_kafka_streams()

# 實時 ETL 管道
def create_real_time_etl():
    # 1. 數據清洗和豐富化
    cleaned_events = user_events \
        .filter(col("user_id").isNotNull()) \
        .withColumn("event_hour", hour(col("event_time"))) \
        .withColumn("is_peak_hour", col("event_hour").between(9, 17))
    
    cleaned_transactions = transactions \
        .filter(col("amount") > 0) \
        .withColumn("amount_category",
                   when(col("amount") < 50, "Low")
                   .when(col("amount") < 200, "Medium")
                   .otherwise("High"))
    
    # 2. 流式 Join
    enriched_data = cleaned_events \
        .withWatermark("event_time", "5 minutes") \
        .join(
            cleaned_transactions.withWatermark("transaction_time", "5 minutes"),
            expr("""
                user_id = customer_id AND
                event_time BETWEEN transaction_time - INTERVAL 2 MINUTES
                                AND transaction_time + INTERVAL 2 MINUTES
            """),
            "inner"
        ) \
        .select(
            col("user_id"),
            col("event_type"),
            col("event_time"),
            col("transaction_id"),
            col("amount"),
            col("amount_category"),
            col("product_id")
        )
    
    # 3. 實時聚合
    real_time_metrics = enriched_data \
        .withWatermark("event_time", "10 minutes") \
        .groupBy(
            window(col("event_time"), "5 minutes"),
            col("amount_category")
        ) \
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_transaction_value"),
            countDistinct("user_id").alias("unique_customers")
        )
    
    return enriched_data, real_time_metrics

# 執行實時 ETL
enriched_stream, metrics_stream = create_real_time_etl()

print("實時數據關聯:")
join_query = enriched_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="10 seconds") \
    .start()

print("實時指標計算:")
metrics_query = metrics_stream \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime="15 seconds") \
    .start()

# 運行管道
time.sleep(60)

join_query.stop()
metrics_query.stop()
```

## 練習提示

1. **流處理基礎**：
   - 理解事件時間 vs 處理時間
   - 正確設置 Watermark 處理延遲數據
   - 選擇合適的觸發間隔

2. **窗口操作**：
   - 滾動窗口用於不重疊的時間段統計
   - 滑動窗口用於重疊的趨勢分析
   - 會話窗口用於用戶行為分析

3. **狀態管理**：
   - 謹慎使用有狀態操作，注意記憶體消耗
   - 設置合理的狀態超時時間
   - 實現狀態的檢查點和恢復

4. **性能優化**：
   - 合理設置並行度
   - 使用適當的序列化格式
   - 監控流處理的延遲和吞吐量

## 進階挑戰

1. **複雜事件處理**：
   - 實現複雜事件模式檢測
   - 構建實時推薦系統
   - 實現動態閾值告警

2. **多流處理**：
   - 處理多個數據源的流
   - 實現流間的複雜 Join
   - 處理不同速率的數據流

3. **生產環境部署**：
   - 設計容錯和故障恢復
   - 實現動態擴縮容
   - 建立監控和告警系統

## 學習檢核

完成練習後，你應該能夠：
- [ ] 創建和管理 Structured Streaming 應用
- [ ] 使用各種窗口函數進行時間分析
- [ ] 實現有狀態的流處理
- [ ] 處理延遲和亂序數據
- [ ] 整合 Kafka 等外部系統
- [ ] 設計端到端的實時數據管道