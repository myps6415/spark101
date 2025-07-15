# 第8章練習：實戰項目

## 練習目標
通過完整的實戰項目，綜合運用 Spark 的各項技術，建立端到端的大數據解決方案。

## 項目1：實時電商數據分析平台

### 項目描述
建立一個實時電商數據分析平台，處理用戶行為、訂單交易、庫存管理等多個數據流。

### 項目要求
1. 實時數據收集和處理
2. 多維度數據分析
3. 實時推薦系統
4. 異常檢測和告警
5. 可視化儀表板

### 技術棧
- Spark Streaming (結構化流)
- Spark SQL (複雜查詢)
- MLlib (機器學習)
- Kafka (消息隊列)
- Redis (快取)

### 項目結構

```
ecommerce_analytics/
├── data_generator/           # 數據生成器
│   ├── user_behavior.py     # 用戶行為數據
│   ├── transaction.py       # 交易數據
│   └── inventory.py         # 庫存數據
├── streaming/               # 流處理
│   ├── behavior_stream.py   # 行為分析
│   ├── sales_stream.py      # 銷售分析
│   └── recommendation.py    # 實時推薦
├── batch/                   # 批處理
│   ├── daily_report.py      # 日報生成
│   └── model_training.py    # 模型訓練
├── models/                  # 機器學習模型
├── config/                  # 配置文件
└── utils/                   # 工具函數
```

### 核心程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import *
from pyspark.ml.classification import *
import json
import time
from datetime import datetime, timedelta

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("電商實時分析平台") \
    .master("local[*]") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# 數據 Schema 定義
user_behavior_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_type", StringType(), True),  # view, click, cart, purchase
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("page_url", StringType(), True),
    StructField("user_agent", StringType(), True)
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("discount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# 1. 實時用戶行為分析
class UserBehaviorAnalyzer:
    def __init__(self, spark):
        self.spark = spark
    
    def create_behavior_stream(self, kafka_servers="localhost:9092"):
        # 從 Kafka 讀取用戶行為數據
        behavior_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "user_behavior") \
            .option("startingOffsets", "latest") \
            .load() \
            .select(
                col("key").cast("string"),
                from_json(col("value").cast("string"), user_behavior_schema).alias("data")
            ) \
            .select("data.*")
        
        return behavior_stream
    
    def real_time_metrics(self, behavior_stream):
        # 實時指標計算
        metrics = behavior_stream \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("event_type"),
                col("category_id")
            ) \
            .agg(
                count("*").alias("event_count"),
                countDistinct("user_id").alias("unique_users"),
                countDistinct("session_id").alias("unique_sessions")
            )
        
        return metrics
    
    def user_session_analysis(self, behavior_stream):
        # 用戶會話分析
        session_metrics = behavior_stream \
            .withWatermark("timestamp", "30 minutes") \
            .groupBy(
                window(col("timestamp"), "10 minutes"),
                col("user_id"),
                col("session_id")
            ) \
            .agg(
                count("*").alias("page_views"),
                countDistinct("product_id").alias("products_viewed"),
                max("timestamp").alias("last_activity"),
                min("timestamp").alias("first_activity"),
                collect_list("event_type").alias("event_sequence")
            ) \
            .withColumn("session_duration", 
                       (col("last_activity").cast("long") - col("first_activity").cast("long")) / 60)
        
        return session_metrics

# 2. 實時銷售分析
class SalesAnalyzer:
    def __init__(self, spark):
        self.spark = spark
    
    def create_transaction_stream(self, kafka_servers="localhost:9092"):
        transaction_stream = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_servers) \
            .option("subscribe", "transactions") \
            .load() \
            .select(
                from_json(col("value").cast("string"), transaction_schema).alias("data")
            ) \
            .select("data.*")
        
        return transaction_stream
    
    def sales_metrics(self, transaction_stream):
        # 銷售指標
        sales_metrics = transaction_stream \
            .withColumn("revenue", col("quantity") * col("price") * (1 - col("discount"))) \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "5 minutes"),
                col("product_id")
            ) \
            .agg(
                sum("revenue").alias("total_revenue"),
                sum("quantity").alias("total_quantity"),
                count("*").alias("transaction_count"),
                avg("price").alias("avg_price")
            )
        
        return sales_metrics
    
    def anomaly_detection(self, sales_metrics):
        # 銷售異常檢測
        # 計算移動平均和標準差
        window_spec = Window.orderBy("window").rowsBetween(-5, -1)
        
        anomaly_detection = sales_metrics \
            .withColumn("avg_revenue_5min", avg("total_revenue").over(window_spec)) \
            .withColumn("stddev_revenue_5min", stddev("total_revenue").over(window_spec)) \
            .withColumn("z_score", 
                       (col("total_revenue") - col("avg_revenue_5min")) / col("stddev_revenue_5min")) \
            .withColumn("is_anomaly", abs(col("z_score")) > 2.0)
        
        return anomaly_detection

# 3. 實時推薦系統
class RecommendationEngine:
    def __init__(self, spark):
        self.spark = spark
        self.model = None
    
    def train_als_model(self, ratings_df):
        # 訓練 ALS 推薦模型
        als = ALS(
            maxIter=10,
            regParam=0.1,
            userCol="user_id_numeric",
            itemCol="product_id_numeric",
            ratingCol="rating",
            coldStartStrategy="drop"
        )
        
        self.model = als.fit(ratings_df)
        return self.model
    
    def real_time_recommendations(self, behavior_stream):
        # 實時推薦
        # 基於用戶最近行為生成推薦
        recent_views = behavior_stream \
            .filter(col("event_type") == "view") \
            .withWatermark("timestamp", "1 hour") \
            .groupBy("user_id") \
            .agg(collect_list("product_id").alias("recent_products"))
        
        # 這裡可以結合訓練好的模型進行推薦
        return recent_views

# 4. 數據管道協調器
class DataPipelineOrchestrator:
    def __init__(self, spark):
        self.spark = spark
        self.behavior_analyzer = UserBehaviorAnalyzer(spark)
        self.sales_analyzer = SalesAnalyzer(spark)
        self.recommendation_engine = RecommendationEngine(spark)
    
    def start_pipeline(self):
        # 啟動所有數據流
        behavior_stream = self.behavior_analyzer.create_behavior_stream()
        transaction_stream = self.sales_analyzer.create_transaction_stream()
        
        # 行為分析流
        behavior_metrics = self.behavior_analyzer.real_time_metrics(behavior_stream)
        behavior_query = behavior_metrics \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        # 銷售分析流
        sales_metrics = self.sales_analyzer.sales_metrics(transaction_stream)
        sales_query = sales_metrics \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        # 異常檢測流
        anomaly_stream = self.sales_analyzer.anomaly_detection(sales_metrics)
        anomaly_query = anomaly_stream \
            .filter(col("is_anomaly") == True) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .trigger(processingTime="60 seconds") \
            .start()
        
        return [behavior_query, sales_query, anomaly_query]

# 完成以下任務：
# 1. 實現完整的數據生成器
# 2. 建立實時分析管道
# 3. 實現推薦系統
# 4. 添加異常檢測和告警
# 5. 創建性能監控

# 你的程式碼在這裡
```

## 項目2：日誌分析和運維監控平台

### 項目描述
建立一個大規模日誌分析平台，處理應用程式日誌、系統日誌、網路日誌等。

### 項目要求
1. 多格式日誌解析
2. 實時異常檢測
3. 性能指標監控
4. 安全事件分析
5. 自動告警系統

### 核心程式碼模板

```python
# 日誌分析器
class LogAnalyzer:
    def __init__(self, spark):
        self.spark = spark
    
    def parse_apache_logs(self, log_stream):
        # Apache 日誌解析
        log_pattern = r'^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\d+|-)'
        
        parsed_logs = log_stream \
            .select(
                regexp_extract(col("value"), log_pattern, 1).alias("ip"),
                regexp_extract(col("value"), log_pattern, 2).alias("timestamp"),
                regexp_extract(col("value"), log_pattern, 3).alias("method"),
                regexp_extract(col("value"), log_pattern, 4).alias("url"),
                regexp_extract(col("value"), log_pattern, 6).alias("status"),
                regexp_extract(col("value"), log_pattern, 7).alias("size")
            ) \
            .filter(col("ip") != "")
        
        return parsed_logs
    
    def detect_anomalies(self, parsed_logs):
        # 異常檢測
        anomalies = parsed_logs \
            .withWatermark("timestamp", "10 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute"),
                col("ip")
            ) \
            .agg(
                count("*").alias("request_count"),
                countDistinct("url").alias("unique_urls"),
                sum(when(col("status").startswith("4"), 1).otherwise(0)).alias("error_4xx"),
                sum(when(col("status").startswith("5"), 1).otherwise(0)).alias("error_5xx")
            ) \
            .withColumn("is_suspicious", 
                       (col("request_count") > 100) | 
                       (col("error_4xx") > 10) | 
                       (col("error_5xx") > 5))
        
        return anomalies

# 完成日誌分析項目
```

## 項目3：金融風險管理系統

### 項目描述
建立金融風險實時監控系統，檢測異常交易、欺詐行為、市場風險等。

### 項目要求
1. 實時交易監控
2. 欺詐檢測模型
3. 風險指標計算
4. 合規檢查
5. 壓力測試

### 核心程式碼模板

```python
# 風險管理系統
class RiskManagementSystem:
    def __init__(self, spark):
        self.spark = spark
    
    def fraud_detection(self, transaction_stream):
        # 欺詐檢測
        # 規則基礎檢測
        fraud_rules = transaction_stream \
            .withColumn("is_high_amount", col("amount") > 10000) \
            .withColumn("is_unusual_time", 
                       hour(col("timestamp")).between(23, 5)) \
            .withColumn("risk_score",
                       when(col("is_high_amount"), 3).otherwise(0) +
                       when(col("is_unusual_time"), 2).otherwise(0))
        
        return fraud_rules
    
    def calculate_var(self, portfolio_stream):
        # 計算 VaR (Value at Risk)
        var_calculation = portfolio_stream \
            .withWatermark("timestamp", "1 hour") \
            .groupBy(
                window(col("timestamp"), "1 day"),
                col("portfolio_id")
            ) \
            .agg(
                collect_list("daily_return").alias("returns")
            ) \
            .withColumn("var_95", 
                       expr("percentile_approx(returns, 0.05)"))
        
        return var_calculation

# 完成風險管理項目
```

## 項目實施指南

### 1. 環境準備
```bash
# 啟動 Kafka
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  confluentinc/cp-kafka

# 啟動 Redis
docker run -d --name redis -p 6379:6379 redis

# 安裝 Python 依賴
pip install pyspark kafka-python redis
```

### 2. 數據生成器實現
```python
# data_generator/user_behavior.py
import json
import time
import random
from kafka import KafkaProducer

class UserBehaviorGenerator:
    def __init__(self, kafka_servers='localhost:9092'):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    def generate_behavior_event(self):
        event = {
            "user_id": f"user_{random.randint(1, 1000)}",
            "session_id": f"session_{random.randint(1, 100)}",
            "event_type": random.choice(["view", "click", "cart", "purchase"]),
            "product_id": f"product_{random.randint(1, 500)}",
            "category_id": f"category_{random.randint(1, 50)}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "page_url": f"/product/{random.randint(1, 500)}",
            "user_agent": "Mozilla/5.0..."
        }
        return event
    
    def start_generating(self, events_per_second=10):
        while True:
            for _ in range(events_per_second):
                event = self.generate_behavior_event()
                self.producer.send('user_behavior', event)
            time.sleep(1)

# 啟動數據生成器
if __name__ == "__main__":
    generator = UserBehaviorGenerator()
    generator.start_generating()
```

### 3. 部署和監控
```python
# 性能監控
class PerformanceMonitor:
    def __init__(self, spark):
        self.spark = spark
    
    def monitor_streaming_metrics(self, query):
        # 監控流處理指標
        progress = query.lastProgress
        if progress:
            print(f"批次 ID: {progress['batchId']}")
            print(f"處理時間: {progress['durationMs']['totalMs']} ms")
            print(f"輸入行數: {progress['inputRowsPerSecond']}")
            print(f"處理行數: {progress['processedRowsPerSecond']}")
    
    def alert_on_lag(self, query, threshold_ms=30000):
        # 延遲告警
        progress = query.lastProgress
        if progress and progress['durationMs']['totalMs'] > threshold_ms:
            print(f"警告：處理延遲過高 {progress['durationMs']['totalMs']} ms")
```

## 項目評估標準

### 功能完整性 (40%)
- [ ] 實時數據處理管道
- [ ] 批處理分析任務
- [ ] 機器學習模型整合
- [ ] 異常檢測和告警
- [ ] 數據可視化

### 性能優化 (30%)
- [ ] 適當的分區策略
- [ ] 有效的緩存使用
- [ ] SQL 查詢優化
- [ ] 資源配置調優
- [ ] 延遲和吞吐量優化

### 代碼質量 (20%)
- [ ] 模組化設計
- [ ] 錯誤處理
- [ ] 日誌記錄
- [ ] 測試覆蓋
- [ ] 文檔完整性

### 創新性 (10%)
- [ ] 獨特的業務洞察
- [ ] 創新的技術應用
- [ ] 用戶體驗優化
- [ ] 可擴展性設計

## 學習檢核

完成實戰項目後，你應該能夠：
- [ ] 設計端到端的大數據解決方案
- [ ] 整合 Spark 的多個組件
- [ ] 處理實際的業務場景
- [ ] 實現生產級別的系統
- [ ] 進行系統性能調優
- [ ] 建立監控和告警機制