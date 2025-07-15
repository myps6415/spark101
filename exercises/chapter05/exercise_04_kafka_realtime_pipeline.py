#!/usr/bin/env python3
"""
第5章練習4：實時數據管道和 Kafka 整合
Kafka 與 Spark Streaming 整合的實時數據管道練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, window, count, \
    sum as spark_sum, avg, max as spark_max, min as spark_min, \
    when, expr, current_timestamp, date_format, \
    split, regexp_extract, concat_ws, \
    collect_list, explode, array_contains
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, \
    DoubleType, TimestampType, BooleanType, ArrayType
import time
import json

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Kafka實時數據管道練習") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-kafka-checkpoint") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 第5章練習4：實時數據管道和 Kafka 整合 ===")
    
    # 1. 模擬 Kafka 數據源
    print("\n1. 模擬 Kafka 數據源:")
    
    # 1.1 創建模擬的 JSON 消息流
    print("\n1.1 創建模擬 JSON 消息流:")
    
    # 使用 rate source 模擬 Kafka 消息
    kafka_like_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 20) \
        .option("numPartitions", 3) \
        .load()
    
    # 模擬 Kafka 消息格式
    kafka_messages = kafka_like_stream.select(
        col("timestamp"),
        col("value").alias("offset"),
        # 模擬不同類型的消息
        when(col("value") % 4 == 0, "user_events")
        .when(col("value") % 4 == 1, "transaction_events")
        .when(col("value") % 4 == 2, "system_logs")
        .otherwise("sensor_data").alias("topic"),
        # 模擬 JSON 消息內容
        when(col("value") % 4 == 0, 
             concat_ws("", 
                lit('{"user_id": '), (col("value") % 1000).cast("string"),
                lit(', "event_type": "'), 
                when((col("value") % 10) < 3, "login")
                .when((col("value") % 10) < 6, "page_view")
                .when((col("value") % 10) < 8, "purchase")
                .otherwise("logout"), lit('",'),
                lit(' "timestamp": "'), col("timestamp").cast("string"), lit('",'),
                lit(' "session_id": "sess_'), (col("value") % 100).cast("string"), lit('"}')
             ))
        .when(col("value") % 4 == 1,
             concat_ws("",
                lit('{"transaction_id": "txn_'), col("value").cast("string"), lit('",'),
                lit(' "user_id": '), (col("value") % 1000).cast("string"), lit(','),
                lit(' "amount": '), (col("value") % 1000 + 10).cast("string"), lit(','),
                lit(' "currency": "USD",'),
                lit(' "status": "'), 
                when((col("value") % 10) < 8, "completed").otherwise("failed"), lit('",'),
                lit(' "timestamp": "'), col("timestamp").cast("string"), lit('"}')
             ))
        .when(col("value") % 4 == 2,
             concat_ws("",
                lit('{"level": "'), 
                when((col("value") % 10) < 6, "INFO")
                .when((col("value") % 10) < 8, "WARN")
                .otherwise("ERROR"), lit('",'),
                lit(' "service": "'), 
                when((col("value") % 3) == 0, "auth-service")
                .when((col("value") % 3) == 1, "payment-service")
                .otherwise("user-service"), lit('",'),
                lit(' "message": "Operation completed",'),
                lit(' "timestamp": "'), col("timestamp").cast("string"), lit('"}')
             ))
        .otherwise(
             concat_ws("",
                lit('{"sensor_id": "sensor_'), (col("value") % 10).cast("string"), lit('",'),
                lit(' "temperature": '), (20 + (col("value") % 20)).cast("string"), lit(','),
                lit(' "humidity": '), (40 + (col("value") % 40)).cast("string"), lit(','),
                lit(' "location": "warehouse_'), ((col("value") % 5) + 1).cast("string"), lit('",'),
                lit(' "timestamp": "'), col("timestamp").cast("string"), lit('"}')
             )
        ).alias("value")
    )
    
    print("模擬 Kafka 消息流已創建")
    
    # 2. 多主題消息路由和解析
    print("\n2. 多主題消息路由和解析:")
    
    # 2.1 用戶事件流處理
    print("\n2.1 用戶事件流處理:")
    
    # 定義用戶事件 schema
    user_event_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("session_id", StringType(), True)
    ])
    
    # 解析用戶事件
    user_events = kafka_messages.filter(col("topic") == "user_events") \
        .select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value"), user_event_schema).alias("data")
        ).select(
            col("kafka_timestamp"),
            col("data.user_id"),
            col("data.event_type"),
            col("data.session_id"),
            col("data.timestamp").alias("event_timestamp")
        )
    
    # 用戶事件聚合
    user_event_aggregation = user_events \
        .withWatermark("kafka_timestamp", "2 minutes") \
        .groupBy(
            window(col("kafka_timestamp"), "1 minute"),
            col("event_type")
        ).agg(
            count("*").alias("event_count"),
            expr("approx_count_distinct(user_id)").alias("unique_users"),
            expr("approx_count_distinct(session_id)").alias("active_sessions")
        )
    
    # 啟動用戶事件處理
    user_event_query = user_event_aggregation.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("運行用戶事件處理...")
    time.sleep(15)
    user_event_query.stop()
    
    # 2.2 交易事件流處理
    print("\n2.2 交易事件流處理:")
    
    # 定義交易事件 schema
    transaction_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # 解析交易事件
    transaction_events = kafka_messages.filter(col("topic") == "transaction_events") \
        .select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value"), transaction_schema).alias("data")
        ).select(
            col("kafka_timestamp"),
            col("data.*")
        )
    
    # 交易實時監控
    transaction_monitoring = transaction_events \
        .withWatermark("kafka_timestamp", "1 minute") \
        .groupBy(
            window(col("kafka_timestamp"), "30 seconds"),
            col("status")
        ).agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount"),
            expr("approx_count_distinct(user_id)").alias("unique_users")
        ).withColumn(
            "amount_per_user",
            col("total_amount") / col("unique_users")
        )
    
    # 啟動交易監控
    transaction_query = transaction_monitoring.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("運行交易事件監控...")
    time.sleep(15)
    transaction_query.stop()
    
    # 3. 流間連接操作
    print("\n3. 流間連接操作:")
    
    # 3.1 用戶事件與交易事件連接
    print("\n3.1 用戶行為與交易關聯分析:")
    
    # 將用戶事件流與交易事件流連接
    user_transaction_join = user_events.alias("u") \
        .join(
            transaction_events.alias("t"),
            (col("u.user_id") == col("t.user_id")) &
            (col("t.kafka_timestamp") >= col("u.kafka_timestamp")) &
            (col("t.kafka_timestamp") <= col("u.kafka_timestamp") + expr("interval 5 minutes")),
            "inner"
        ).select(
            col("u.kafka_timestamp").alias("user_event_time"),
            col("t.kafka_timestamp").alias("transaction_time"),
            col("u.user_id"),
            col("u.event_type"),
            col("u.session_id"),
            col("t.transaction_id"),
            col("t.amount"),
            col("t.status")
        )
    
    # 用戶轉化分析
    conversion_analysis = user_transaction_join \
        .withWatermark("user_event_time", "5 minutes") \
        .groupBy(
            window(col("user_event_time"), "2 minutes"),
            col("event_type")
        ).agg(
            count("*").alias("events_leading_to_transaction"),
            spark_sum("amount").alias("revenue_from_event_type"),
            avg("amount").alias("avg_transaction_value"),
            expr("approx_count_distinct(user_id)").alias("converting_users")
        ).withColumn(
            "revenue_per_user",
            col("revenue_from_event_type") / col("converting_users")
        )
    
    # 啟動轉化分析
    conversion_query = conversion_analysis.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='8 seconds') \
        .start()
    
    print("運行用戶轉化分析...")
    time.sleep(20)
    conversion_query.stop()
    
    # 4. 實時數據管道
    print("\n4. 實時數據管道建設:")
    
    # 4.1 數據清洗和標準化管道
    print("\n4.1 數據清洗和標準化:")
    
    # 系統日誌解析
    log_schema = StructType([
        StructField("level", StringType(), True),
        StructField("service", StringType(), True),
        StructField("message", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    system_logs = kafka_messages.filter(col("topic") == "system_logs") \
        .select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value"), log_schema).alias("data")
        ).select(
            col("kafka_timestamp"),
            col("data.*")
        ).withColumn(
            "severity_score",
            when(col("level") == "ERROR", 3)
            .when(col("level") == "WARN", 2)
            .otherwise(1)
        )
    
    # 日誌聚合和告警
    log_aggregation = system_logs \
        .withWatermark("kafka_timestamp", "1 minute") \
        .groupBy(
            window(col("kafka_timestamp"), "1 minute"),
            col("service"),
            col("level")
        ).agg(
            count("*").alias("log_count"),
            spark_sum("severity_score").alias("total_severity"),
            collect_list("message").alias("sample_messages")
        ).withColumn(
            "alert_priority",
            when((col("level") == "ERROR") & (col("log_count") > 5), "CRITICAL")
            .when(col("total_severity") > 10, "HIGH")
            .when(col("total_severity") > 5, "MEDIUM")
            .otherwise("LOW")
        ).filter(col("alert_priority") != "LOW")
    
    # 啟動日誌監控
    log_query = log_aggregation.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='6 seconds') \
        .start()
    
    print("運行系統日誌監控...")
    time.sleep(18)
    log_query.stop()
    
    # 4.2 物聯網傳感器數據管道
    print("\n4.2 IoT 傳感器數據管道:")
    
    # 傳感器數據 schema
    sensor_schema = StructType([
        StructField("sensor_id", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    # 解析傳感器數據
    sensor_data = kafka_messages.filter(col("topic") == "sensor_data") \
        .select(
            col("timestamp").alias("kafka_timestamp"),
            from_json(col("value"), sensor_schema).alias("data")
        ).select(
            col("kafka_timestamp"),
            col("data.*")
        )
    
    # 傳感器異常檢測
    sensor_anomaly_detection = sensor_data \
        .withWatermark("kafka_timestamp", "2 minutes") \
        .groupBy(
            window(col("kafka_timestamp"), "1 minute"),
            col("location")
        ).agg(
            count("*").alias("reading_count"),
            avg("temperature").alias("avg_temperature"),
            avg("humidity").alias("avg_humidity"),
            spark_max("temperature").alias("max_temperature"),
            spark_min("temperature").alias("min_temperature"),
            expr("stddev(temperature)").alias("temp_stddev")
        ).withColumn(
            "temperature_alert",
            when(col("avg_temperature") > 30, "OVERHEATING")
            .when(col("avg_temperature") < 10, "TOO_COLD")
            .when(col("temp_stddev") > 5, "UNSTABLE")
            .otherwise("NORMAL")
        ).withColumn(
            "humidity_alert",
            when(col("avg_humidity") > 80, "TOO_HUMID")
            .when(col("avg_humidity") < 30, "TOO_DRY")
            .otherwise("NORMAL")
        ).filter(
            (col("temperature_alert") != "NORMAL") | (col("humidity_alert") != "NORMAL")
        )
    
    # 啟動傳感器監控
    sensor_query = sensor_anomaly_detection.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='7 seconds') \
        .start()
    
    print("運行傳感器異常檢測...")
    time.sleep(20)
    sensor_query.stop()
    
    # 5. 輸出到多個接收器
    print("\n5. 多接收器輸出管道:")
    
    # 5.1 準備輸出數據
    print("\n5.1 數據輸出到多個目標:")
    
    # 創建匯總數據用於輸出
    business_summary = kafka_messages \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "1 minute"),
            col("topic")
        ).agg(
            count("*").alias("message_count")
        ).withColumn(
            "summary_timestamp", current_timestamp()
        )
    
    # 5.2 同時輸出到多個目標
    print("\n5.2 啟動多目標輸出:")
    
    # 輸出到控制台
    console_query = business_summary.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .option("checkpointLocation", "/tmp/kafka-console-checkpoint") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # 模擬輸出到 Kafka (使用內存接收器模擬)
    kafka_output_query = business_summary.select(
        to_json(struct(
            col("window"),
            col("topic"),
            col("message_count"),
            col("summary_timestamp")
        )).alias("value")
    ).writeStream \
        .outputMode("update") \
        .format("memory") \
        .queryName("kafka_output_table") \
        .option("checkpointLocation", "/tmp/kafka-output-checkpoint") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("運行多目標輸出...")
    time.sleep(15)
    
    # 檢查輸出表內容
    print("\n輸出到模擬 Kafka 的數據:")
    spark.sql("SELECT * FROM kafka_output_table").show(10, truncate=False)
    
    console_query.stop()
    kafka_output_query.stop()
    
    # 6. 容錯和檢查點管理
    print("\n6. 容錯和檢查點管理:")
    
    # 6.1 檢查點配置
    print("\n6.1 容錯機制配置:")
    
    fault_tolerant_processing = business_summary.select(
        col("window"),
        col("topic"),
        col("message_count"),
        current_timestamp().alias("processed_at")
    )
    
    # 配置容錯處理
    fault_tolerant_query = fault_tolerant_processing.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("checkpointLocation", "/tmp/fault-tolerant-checkpoint") \
        .option("maxFilesPerTrigger", "1000") \
        .trigger(processingTime='4 seconds') \
        .start()
    
    print("測試容錯機制...")
    time.sleep(12)
    fault_tolerant_query.stop()
    
    # 7. 性能優化和監控
    print("\n7. 性能優化和監控:")
    
    # 7.1 批處理大小優化
    print("\n7.1 批處理優化:")
    
    optimized_processing = kafka_messages \
        .coalesce(2) \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(
            window(col("timestamp"), "30 seconds"),
            col("topic")
        ).agg(
            count("*").alias("message_count"),
            expr("approx_count_distinct(value)").alias("unique_messages")
        )
    
    # 7.2 監控指標收集
    monitoring_query = optimized_processing.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='3 seconds') \
        .start()
    
    print("運行性能優化處理...")
    time.sleep(10)
    monitoring_query.stop()
    
    # 8. 實時機器學習推理
    print("\n8. 實時機器學習推理:")
    
    # 8.1 簡化的實時推理
    print("\n8.1 實時推理管道:")
    
    # 模擬實時推理（基於規則的簡化版本）
    ml_inference = transaction_events \
        .withColumn(
            "fraud_score",
            when(col("amount") > 1000, 0.7)
            .when(col("amount") > 500, 0.4)
            .when(col("status") == "failed", 0.9)
            .otherwise(0.1)
        ).withColumn(
            "risk_category",
            when(col("fraud_score") > 0.6, "HIGH_RISK")
            .when(col("fraud_score") > 0.3, "MEDIUM_RISK")
            .otherwise("LOW_RISK")
        ).filter(col("risk_category") != "LOW_RISK")
    
    # 實時推理輸出
    ml_query = ml_inference.select(
        col("transaction_id"),
        col("user_id"),
        col("amount"),
        col("fraud_score"),
        col("risk_category"),
        current_timestamp().alias("inference_time")
    ).writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='6 seconds') \
        .start()
    
    print("運行實時推理管道...")
    time.sleep(18)
    ml_query.stop()
    
    # 9. 數據管道總結
    print("\n9. 實時數據管道總結:")
    
    # 實現的管道組件
    pipeline_components = [
        "多主題 Kafka 消息路由",
        "JSON 消息解析和驗證",
        "流間連接和聚合",
        "實時異常檢測",
        "多目標數據輸出",
        "容錯和檢查點管理",
        "性能優化策略",
        "實時機器學習推理",
        "業務指標監控"
    ]
    
    print("已實現的管道組件:")
    for i, component in enumerate(pipeline_components, 1):
        print(f"{i}. {component}")
    
    # 最佳實踐
    best_practices = [
        "合理設置批處理間隔平衡延遲和吞吐量",
        "使用適當的 watermark 處理延遲數據",
        "配置 checkpoint 確保容錯恢復",
        "監控流處理指標（延遲、吞吐量、錯誤率）",
        "實施背壓機制防止內存溢出",
        "使用適當的分區策略提高並行度",
        "設計合理的告警和通知機制"
    ]
    
    print("\nKafka 整合最佳實踐:")
    for i, practice in enumerate(best_practices, 1):
        print(f"{i}. {practice}")
    
    # 管道架構總結
    print("\n實時數據管道架構:")
    architecture_layers = {
        "數據源層": "Kafka Topics (user_events, transactions, logs, sensors)",
        "攝取層": "Spark Structured Streaming",
        "處理層": "實時聚合、連接、過濾、轉換",
        "分析層": "異常檢測、機器學習推理、業務指標",
        "輸出層": "多目標輸出 (Kafka, Database, File System)",
        "監控層": "指標收集、告警、性能監控"
    }
    
    for layer, description in architecture_layers.items():
        print(f"- {layer}: {description}")
    
    # 清理資源
    spark.stop()
    print("\n實時數據管道和 Kafka 整合練習完成！")

if __name__ == "__main__":
    main()