#!/usr/bin/env python3
"""
第5章：Spark Streaming - Kafka 整合
學習如何與 Kafka 整合進行實時流處理
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import (
    col, from_json, to_json, struct, current_timestamp, 
    window, count, sum as spark_sum, avg, max as spark_max, min as spark_min
)
import json
import time
import threading
from datetime import datetime, timedelta

def create_sample_kafka_data():
    """創建示例 Kafka 消息數據"""
    import random
    
    # 模擬不同類型的事件
    event_types = ["user_login", "page_view", "purchase", "logout"]
    users = ["user_001", "user_002", "user_003", "user_004", "user_005"]
    pages = ["home", "product", "cart", "checkout", "profile"]
    products = ["laptop", "phone", "tablet", "watch", "headphones"]
    
    events = []
    
    for i in range(100):
        event_type = random.choice(event_types)
        
        base_event = {
            "event_id": f"evt_{i:05d}",
            "event_type": event_type,
            "user_id": random.choice(users),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 60))).isoformat(),
            "session_id": f"sess_{random.randint(1000, 9999)}"
        }
        
        # 根據事件類型添加特定字段
        if event_type == "page_view":
            base_event.update({
                "page": random.choice(pages),
                "duration": random.randint(10, 300)
            })
        elif event_type == "purchase":
            base_event.update({
                "product": random.choice(products),
                "quantity": random.randint(1, 5),
                "price": round(random.uniform(10, 1000), 2)
            })
        elif event_type == "user_login":
            base_event.update({
                "device": random.choice(["desktop", "mobile", "tablet"]),
                "location": random.choice(["US", "UK", "CA", "DE", "JP"])
            })
        
        events.append(base_event)
    
    return events

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Kafka Streaming") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/kafka_checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("🌊 Kafka Streaming 整合示範")
    print("=" * 40)
    
    # 注意：這個示例展示了如何使用 Kafka，但需要實際的 Kafka 服務器
    # 在實際使用中，您需要：
    # 1. 啟動 Kafka 服務器
    # 2. 創建主題
    # 3. 配置正確的 Kafka 伺服器地址
    
    print("⚠️  注意：此示例需要運行中的 Kafka 服務器")
    print("如果沒有 Kafka 服務器，請參考文檔設置本地 Kafka 環境")
    
    # 1. 定義消息結構
    print("\n1️⃣ 定義消息結構")
    
    # 基本事件結構
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("page", StringType(), True),
        StructField("duration", IntegerType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("device", StringType(), True),
        StructField("location", StringType(), True)
    ])
    
    print("事件 Schema:")
    print(event_schema)
    
    # 2. 模擬 Kafka 流數據（使用 rate source）
    print("\n2️⃣ 模擬流數據")
    
    # 由於沒有實際的 Kafka，我們使用 rate source 模擬
    # 在實際環境中，這裡會是從 Kafka 讀取
    
    # 創建示例數據
    sample_events = create_sample_kafka_data()
    
    # 創建 DataFrame
    events_df = spark.createDataFrame(
        [(json.dumps(event),) for event in sample_events], 
        ["value"]
    )
    
    print("示例事件數據:")
    events_df.show(5, truncate=False)
    
    # 3. 解析 JSON 消息
    print("\n3️⃣ 解析 JSON 消息")
    
    # 解析 JSON 格式的消息
    parsed_df = events_df.select(
        from_json(col("value"), event_schema).alias("data")
    ).select("data.*")
    
    print("解析後的數據:")
    parsed_df.show(5, truncate=False)
    
    # 4. 實時事件統計
    print("\n4️⃣ 實時事件統計")
    
    # 按事件類型統計
    event_stats = parsed_df.groupBy("event_type") \
        .agg(
            count("*").alias("event_count"),
            spark_sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price")
        )
    
    print("事件統計:")
    event_stats.show()
    
    # 5. 用戶行為分析
    print("\n5️⃣ 用戶行為分析")
    
    # 用戶會話統計
    user_sessions = parsed_df.groupBy("user_id", "session_id") \
        .agg(
            count("*").alias("events_in_session"),
            spark_sum("duration").alias("total_duration"),
            spark_sum("quantity").alias("items_purchased"),
            spark_sum("price").alias("total_spent")
        )
    
    print("用戶會話統計:")
    user_sessions.show()
    
    # 6. 時間窗口分析
    print("\n6️⃣ 時間窗口分析")
    
    # 轉換時間戳
    from pyspark.sql.functions import to_timestamp
    
    windowed_df = parsed_df.withColumn(
        "event_time", 
        to_timestamp(col("timestamp"))
    )
    
    # 10分鐘窗口的事件統計
    windowed_stats = windowed_df \
        .groupBy(
            window(col("event_time"), "10 minutes"),
            col("event_type")
        ) \
        .agg(
            count("*").alias("event_count"),
            spark_sum("quantity").alias("total_quantity")
        )
    
    print("時間窗口統計:")
    windowed_stats.show(truncate=False)
    
    # 7. 實時異常檢測
    print("\n7️⃣ 實時異常檢測")
    
    # 檢測異常購買行為
    anomalies = parsed_df.filter(
        (col("event_type") == "purchase") &
        ((col("quantity") > 10) | (col("price") > 500))
    )
    
    print("異常購買行為:")
    anomalies.show()
    
    # 8. 實時推薦系統數據
    print("\n8️⃣ 實時推薦系統數據")
    
    # 產品瀏覽統計
    product_views = parsed_df.filter(col("event_type") == "page_view") \
        .filter(col("page") == "product") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("product_views"),
            avg("duration").alias("avg_view_duration")
        )
    
    print("產品瀏覽統計:")
    product_views.show()
    
    # 9. 多層次聚合
    print("\n9️⃣ 多層次聚合")
    
    # 地區和設備統計
    location_device_stats = parsed_df.filter(col("event_type") == "user_login") \
        .groupBy("location", "device") \
        .agg(
            count("*").alias("login_count"),
            col("location").alias("region"),
            col("device").alias("device_type")
        )
    
    print("地區和設備統計:")
    location_device_stats.show()
    
    # 10. 實時 A/B 測試分析
    print("\n🔟 實時 A/B 測試分析")
    
    # 模擬 A/B 測試數據
    from pyspark.sql.functions import when, rand
    
    ab_test_df = parsed_df.withColumn(
        "test_group",
        when(rand() > 0.5, "A").otherwise("B")
    )
    
    # A/B 測試轉換率
    ab_conversion = ab_test_df.groupBy("test_group") \
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("purchases"),
            (spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)) * 100.0 / count("*")).alias("conversion_rate")
        )
    
    print("A/B 測試轉換率:")
    ab_conversion.show()
    
    # 11. 實時儀表板數據
    print("\n1️⃣1️⃣ 實時儀表板數據")
    
    # 創建實時儀表板需要的指標
    dashboard_metrics = parsed_df.agg(
        count("*").alias("total_events"),
        spark_sum(when(col("event_type") == "user_login", 1).otherwise(0)).alias("total_logins"),
        spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("total_page_views"),
        spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias("total_purchases"),
        spark_sum("price").alias("total_revenue"),
        avg("price").alias("avg_order_value")
    )
    
    print("儀表板指標:")
    dashboard_metrics.show()
    
    # 12. 實時警報系統
    print("\n1️⃣2️⃣ 實時警報系統")
    
    # 檢測需要警報的情況
    alerts = parsed_df.filter(
        (col("event_type") == "purchase") & (col("price") > 800)
    ).select(
        col("event_id"),
        col("user_id"),
        col("price"),
        col("timestamp"),
        lit("HIGH_VALUE_PURCHASE").alias("alert_type")
    )
    
    from pyspark.sql.functions import lit
    print("高價值購買警報:")
    alerts.show()
    
    # 13. 資料管道輸出格式
    print("\n1️⃣3️⃣ 資料管道輸出格式")
    
    # 準備輸出到下游系統的數據
    output_format = parsed_df.select(
        to_json(struct(
            col("event_id"),
            col("event_type"),
            col("user_id"),
            col("timestamp"),
            current_timestamp().alias("processed_at")
        )).alias("message_value")
    )
    
    print("輸出格式:")
    output_format.show(5, truncate=False)
    
    # 14. 實際 Kafka 配置示例
    print("\n1️⃣4️⃣ 實際 Kafka 配置示例")
    
    print("""
    # 從 Kafka 讀取的實際配置：
    kafka_df = spark.readStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("subscribe", "user_events") \\
        .option("startingOffsets", "latest") \\
        .load()
    
    # 寫入到 Kafka 的實際配置：
    query = processed_df.writeStream \\
        .format("kafka") \\
        .option("kafka.bootstrap.servers", "localhost:9092") \\
        .option("topic", "processed_events") \\
        .option("checkpointLocation", "/tmp/kafka_checkpoint") \\
        .start()
    """)
    
    # 15. 性能監控
    print("\n1️⃣5️⃣ 性能監控")
    
    # 模擬流處理性能指標
    performance_metrics = {
        "total_events_processed": parsed_df.count(),
        "processing_time": "模擬處理時間",
        "throughput": "每秒處理事件數",
        "latency": "端到端延遲",
        "memory_usage": "記憶體使用情況"
    }
    
    print("性能指標:")
    for metric, value in performance_metrics.items():
        print(f"  {metric}: {value}")
    
    # 停止 SparkSession
    spark.stop()
    print("\n✅ Kafka Streaming 整合示範完成")
    
    print("\n📝 實際部署建議:")
    print("1. 設置 Kafka 集群")
    print("2. 配置適當的分區策略")
    print("3. 監控消費者滯後")
    print("4. 設置適當的檢查點間隔")
    print("5. 調整批次大小和觸發間隔")
    print("6. 實施錯誤處理和重試機制")
    print("7. 設置監控和警報")

if __name__ == "__main__":
    main()