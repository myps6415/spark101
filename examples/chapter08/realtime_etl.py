#!/usr/bin/env python3
"""
第8章：實戰項目 - 實時 ETL 系統
構建一個完整的實時 ETL 數據管道，展示 Spark 在數據工程中的應用
"""

import json
import os
import random
import tempfile
import threading
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, collect_list, count, current_timestamp, date_format,
    sum as spark_sum, when, window
)
from pyspark.sql.types import (
    DoubleType, IntegerType, StringType, StructField, StructType
)

class RealTimeETL:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.temp_dir = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.temp_dir, "input")
        self.output_dir = os.path.join(self.temp_dir, "output")
        self.checkpoint_dir = os.path.join(self.temp_dir, "checkpoints")
        
        # 創建目錄
        os.makedirs(self.input_dir, exist_ok=True)
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)
        
        print(f"ETL 工作目錄: {self.temp_dir}")
        
    def generate_sample_data(self, duration_seconds=30):
        """生成示例數據流"""
        
        # 產品目錄
        products = [
            {"id": 1, "name": "Laptop", "category": "Electronics", "price": 1200.00},
            {"id": 2, "name": "Phone", "category": "Electronics", "price": 800.00},
            {"id": 3, "name": "Tablet", "category": "Electronics", "price": 500.00},
            {"id": 4, "name": "Headphones", "category": "Electronics", "price": 150.00},
            {"id": 5, "name": "Watch", "category": "Electronics", "price": 300.00},
            {"id": 6, "name": "Book", "category": "Books", "price": 25.00},
            {"id": 7, "name": "Shoes", "category": "Clothing", "price": 80.00},
            {"id": 8, "name": "Shirt", "category": "Clothing", "price": 40.00}
        ]
        
        # 用戶信息
        users = [
            {"id": 1, "name": "Alice", "email": "alice@example.com", "city": "New York"},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "city": "Los Angeles"},
            {"id": 3, "name": "Charlie", "email": "charlie@example.com", "city": "Chicago"},
            {"id": 4, "name": "Diana", "email": "diana@example.com", "city": "Houston"},
            {"id": 5, "name": "Eve", "email": "eve@example.com", "city": "Phoenix"}
        ]
        
        def generate_events():
            for i in range(duration_seconds):
                # 每秒生成多個事件
                for j in range(random.randint(3, 8)):
                    event_type = random.choice(["order", "product_view", "user_signup", "cart_add"])
                    
                    base_event = {
                        "event_id": f"evt_{i}_{j}",
                        "event_type": event_type,
                        "timestamp": datetime.now().isoformat(),
                        "user_id": random.choice(users)["id"]
                    }
                    
                    if event_type == "order":
                        product = random.choice(products)
                        quantity = random.randint(1, 5)
                        base_event.update({
                            "product_id": product["id"],
                            "quantity": quantity,
                            "amount": product["price"] * quantity,
                            "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"])
                        })
                    elif event_type == "product_view":
                        base_event.update({
                            "product_id": random.choice(products)["id"],
                            "view_duration": random.randint(10, 300)
                        })
                    elif event_type == "user_signup":
                        base_event.update({
                            "signup_method": random.choice(["email", "google", "facebook"]),
                            "referral_source": random.choice(["organic", "paid", "social", "referral"])
                        })
                    elif event_type == "cart_add":
                        base_event.update({
                            "product_id": random.choice(products)["id"],
                            "quantity": random.randint(1, 3)
                        })
                    
                    # 寫入事件文件
                    filename = f"event_{i}_{j}.json"
                    with open(os.path.join(self.input_dir, filename), 'w') as f:
                        json.dump(base_event, f)
                
                time.sleep(1)
        
        # 在後台線程中生成數據
        generator_thread = threading.Thread(target=generate_events)
        generator_thread.daemon = True
        generator_thread.start()
        
        return generator_thread
    
    def define_schemas(self):
        """定義數據架構"""
        
        # 事件架構
        event_schema = StructType([
            StructField("event_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("user_id", IntegerType(), True),
            StructField("product_id", IntegerType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField("view_duration", IntegerType(), True),
            StructField("signup_method", StringType(), True),
            StructField("referral_source", StringType(), True)
        ])
        
        return event_schema
    
    def create_streaming_source(self, schema):
        """創建流數據源"""
        
        # 從文件系統讀取JSON流
        streaming_df = self.spark.readStream \
            .format("json") \
            .schema(schema) \
            .option("path", self.input_dir) \
            .option("maxFilesPerTrigger", 10) \
            .load()
        
        return streaming_df
    
    def data_cleaning(self, df):
        """數據清洗"""
        
        print("\n🧹 數據清洗階段")
        
        # 1. 處理時間戳
        from pyspark.sql.functions import to_timestamp
        cleaned_df = df.withColumn("event_timestamp", to_timestamp(col("timestamp")))
        
        # 2. 處理空值
        cleaned_df = cleaned_df.fillna({
            "quantity": 0,
            "amount": 0.0,
            "view_duration": 0,
            "payment_method": "unknown",
            "signup_method": "unknown",
            "referral_source": "unknown"
        })
        
        # 3. 數據驗證
        cleaned_df = cleaned_df.filter(
            col("user_id").isNotNull() & 
            col("event_type").isNotNull() &
            col("event_timestamp").isNotNull()
        )
        
        # 4. 添加處理時間戳
        cleaned_df = cleaned_df.withColumn("processing_time", current_timestamp())
        
        return cleaned_df
    
    def data_transformation(self, df):
        """數據轉換"""
        
        print("\n🔄 數據轉換階段")
        
        # 1. 事件類型特定的轉換
        transformed_df = df.withColumn("is_purchase", 
                                     when(col("event_type") == "order", True).otherwise(False))
        
        # 2. 添加時間維度
        transformed_df = transformed_df.withColumn("hour", 
                                                 date_format(col("event_timestamp"), "HH")) \
                                     .withColumn("date", 
                                               date_format(col("event_timestamp"), "yyyy-MM-dd"))
        
        # 3. 計算衍生指標
        transformed_df = transformed_df.withColumn("revenue", 
                                                 when(col("event_type") == "order", col("amount")).otherwise(0))
        
        # 4. 添加分類標籤
        transformed_df = transformed_df.withColumn("customer_segment",
                                                 when(col("amount") > 500, "premium")
                                                 .when(col("amount") > 100, "regular")
                                                 .otherwise("basic"))
        
        return transformed_df
    
    def data_enrichment(self, df):
        """數據豐富化"""
        
        print("\n📈 數據豐富化階段")
        
        # 1. 創建維度表
        user_dim = self.spark.createDataFrame([
            (1, "Alice", "alice@example.com", "New York", "Premium"),
            (2, "Bob", "bob@example.com", "Los Angeles", "Regular"),
            (3, "Charlie", "charlie@example.com", "Chicago", "Regular"),
            (4, "Diana", "diana@example.com", "Houston", "Premium"),
            (5, "Eve", "eve@example.com", "Phoenix", "Basic")
        ], ["user_id", "user_name", "email", "city", "tier"])
        
        product_dim = self.spark.createDataFrame([
            (1, "Laptop", "Electronics", 1200.00, "High"),
            (2, "Phone", "Electronics", 800.00, "High"),
            (3, "Tablet", "Electronics", 500.00, "Medium"),
            (4, "Headphones", "Electronics", 150.00, "Medium"),
            (5, "Watch", "Electronics", 300.00, "Medium"),
            (6, "Book", "Books", 25.00, "Low"),
            (7, "Shoes", "Clothing", 80.00, "Low"),
            (8, "Shirt", "Clothing", 40.00, "Low")
        ], ["product_id", "product_name", "category", "price", "price_tier"])
        
        # 2. 聯結維度表
        enriched_df = df.join(user_dim, "user_id", "left") \
                        .join(product_dim, "product_id", "left")
        
        # 3. 添加計算列
        enriched_df = enriched_df.withColumn("is_high_value", 
                                           when(col("price_tier") == "High", True).otherwise(False))
        
        return enriched_df
    
    def create_aggregations(self, df):
        """創建聚合視圖"""
        
        print("\n📊 聚合視圖創建")
        
        # 1. 實時銷售指標
        sales_metrics = df.filter(col("event_type") == "order") \
                         .withWatermark("event_timestamp", "1 minute") \
                         .groupBy(window(col("event_timestamp"), "5 minutes")) \
                         .agg(
                             count("*").alias("order_count"),
                             spark_sum("amount").alias("total_revenue"),
                             avg("amount").alias("avg_order_value"),
                             col("user_id").countDistinct().alias("unique_customers")
                         )
        
        # 2. 產品分析
        product_metrics = df.filter(col("product_id").isNotNull()) \
                           .withWatermark("event_timestamp", "1 minute") \
                           .groupBy(
                               window(col("event_timestamp"), "10 minutes"),
                               col("product_name"),
                               col("category")
                           ) \
                           .agg(
                               count("*").alias("interaction_count"),
                               spark_sum("quantity").alias("total_quantity"),
                               spark_sum("revenue").alias("product_revenue")
                           )
        
        # 3. 用戶行為分析
        user_behavior = df.withWatermark("event_timestamp", "1 minute") \
                         .groupBy(
                             window(col("event_timestamp"), "15 minutes"),
                             col("user_id"),
                             col("user_name")
                         ) \
                         .agg(
                             count("*").alias("event_count"),
                             spark_sum("revenue").alias("user_revenue"),
                             collect_list("event_type").alias("event_sequence")
                         )
        
        return sales_metrics, product_metrics, user_behavior
    
    def quality_checks(self, df):
        """數據品質檢查"""
        
        print("\n✅ 數據品質檢查")
        
        # 1. 重複檢查
        duplicate_check = df.groupBy("event_id").count().filter(col("count") > 1)
        
        # 2. 空值檢查
        null_check = df.select([
            col(c).isNull().cast("int").alias(c) for c in df.columns
        ]).agg(*[
            spark_sum(col(c)).alias(f"{c}_nulls") for c in df.columns
        ])
        
        # 3. 範圍檢查
        range_check = df.select(
            spark_sum(when(col("amount") < 0, 1).otherwise(0)).alias("negative_amounts"),
            spark_sum(when(col("quantity") < 0, 1).otherwise(0)).alias("negative_quantities"),
            spark_sum(when(col("user_id") <= 0, 1).otherwise(0)).alias("invalid_user_ids")
        )
        
        return duplicate_check, null_check, range_check
    
    def setup_outputs(self, sales_metrics, product_metrics, user_behavior):
        """設置輸出流"""
        
        print("\n📤 設置輸出流")
        
        # 1. 控制台輸出
        console_query = sales_metrics.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='10 seconds') \
            .queryName("sales_console") \
            .start()
        
        # 2. 文件輸出
        file_query = product_metrics.writeStream \
            .outputMode("append") \
            .format("json") \
            .option("path", os.path.join(self.output_dir, "product_metrics")) \
            .option("checkpointLocation", os.path.join(self.checkpoint_dir, "product_checkpoint")) \
            .trigger(processingTime='15 seconds') \
            .queryName("product_file") \
            .start()
        
        # 3. 內存輸出（用於查詢）
        memory_query = user_behavior.writeStream \
            .outputMode("append") \
            .format("memory") \
            .trigger(processingTime='20 seconds') \
            .queryName("user_behavior_memory") \
            .start()
        
        return console_query, file_query, memory_query
    
    def monitoring_and_alerting(self, df):
        """監控和警報"""
        
        print("\n🚨 監控和警報")
        
        # 1. 異常檢測
        anomalies = df.filter(
            (col("amount") > 2000) |  # 高價值訂單
            (col("quantity") > 10) |   # 大量訂單
            (col("view_duration") > 600)  # 長時間瀏覽
        )
        
        # 2. 業務指標監控
        business_metrics = df.filter(col("event_type") == "order") \
                            .withWatermark("event_timestamp", "1 minute") \
                            .groupBy(window(col("event_timestamp"), "1 minute")) \
                            .agg(
                                count("*").alias("orders_per_minute"),
                                spark_sum("amount").alias("revenue_per_minute"),
                                avg("amount").alias("avg_order_value")
                            )
        
        # 3. 系統健康檢查
        health_check = df.withWatermark("event_timestamp", "1 minute") \
                        .groupBy(window(col("event_timestamp"), "5 minutes")) \
                        .agg(
                            count("*").alias("total_events"),
                            spark_sum(when(col("event_type").isNull(), 1).otherwise(0)).alias("malformed_events")
                        ) \
                        .withColumn("health_score", 
                                  when(col("malformed_events") / col("total_events") > 0.05, "Poor")
                                  .when(col("malformed_events") / col("total_events") > 0.01, "Fair")
                                  .otherwise("Good"))
        
        return anomalies, business_metrics, health_check
    
    def run_etl_pipeline(self):
        """運行完整的 ETL 管道"""
        
        print("\n🚀 啟動 ETL 管道")
        print("=" * 40)
        
        # 1. 定義架構
        schema = self.define_schemas()
        
        # 2. 創建流數據源
        streaming_df = self.create_streaming_source(schema)
        
        # 3. 數據清洗
        cleaned_df = self.data_cleaning(streaming_df)
        
        # 4. 數據轉換
        transformed_df = self.data_transformation(cleaned_df)
        
        # 5. 數據豐富化
        enriched_df = self.data_enrichment(transformed_df)
        
        # 6. 創建聚合視圖
        sales_metrics, product_metrics, user_behavior = self.create_aggregations(enriched_df)
        
        # 7. 設置輸出
        console_query, file_query, memory_query = self.setup_outputs(sales_metrics, product_metrics, user_behavior)
        
        # 8. 監控和警報
        anomalies, business_metrics, health_check = self.monitoring_and_alerting(enriched_df)
        
        # 9. 監控查詢
        monitoring_query = health_check.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='30 seconds') \
            .queryName("health_monitoring") \
            .start()
        
        return [console_query, file_query, memory_query, monitoring_query]
    
    def cleanup(self):
        """清理資源"""
        import shutil
        shutil.rmtree(self.temp_dir)
        print(f"清理工作目錄: {self.temp_dir}")

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Real-time ETL Pipeline") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/etl_checkpoints") \
        .config("spark.sql.streaming.stateStore.maintenanceInterval", "60s") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("🔄 實時 ETL 系統")
    print("=" * 30)
    
    # 初始化 ETL 系統
    etl = RealTimeETL(spark)
    
    try:
        # 1. 啟動數據生成器
        print("\n1️⃣ 啟動數據生成器")
        generator_thread = etl.generate_sample_data(60)  # 生成60秒的數據
        
        # 2. 啟動 ETL 管道
        print("\n2️⃣ 啟動 ETL 管道")
        queries = etl.run_etl_pipeline()
        
        # 3. 讓管道運行一段時間
        print("\n3️⃣ ETL 管道運行中...")
        time.sleep(90)  # 運行90秒
        
        # 4. 查看內存表結果
        print("\n4️⃣ 查看處理結果")
        try:
            user_behavior_df = spark.sql("SELECT * FROM user_behavior_memory")
            print("用戶行為分析結果:")
            user_behavior_df.show(10, truncate=False)
        except Exception as e:
            print(f"查看內存表時出錯: {e}")
        
        # 5. 查看查詢狀態
        print("\n5️⃣ 查詢狀態")
        for i, query in enumerate(queries):
            if query.isActive:
                print(f"查詢 {i+1}: {query.name} - 活動中")
                print(f"  最後進度: {query.lastProgress}")
            else:
                print(f"查詢 {i+1}: {query.name} - 已停止")
        
        # 6. 性能指標
        print("\n6️⃣ 性能指標")
        active_queries = spark.streams.active
        print(f"活動查詢數量: {len(active_queries)}")
        
        for query in active_queries:
            if query.lastProgress:
                batch_duration = query.lastProgress.get('durationMs', {}).get('triggerExecution', 0)
                input_rows = query.lastProgress.get('inputRowsPerSecond', 0)
                print(f"查詢 {query.name}:")
                print(f"  批次執行時間: {batch_duration} ms")
                print(f"  輸入行數/秒: {input_rows}")
        
        # 7. 停止所有查詢
        print("\n7️⃣ 停止查詢")
        for query in queries:
            if query.isActive:
                query.stop()
                print(f"已停止查詢: {query.name}")
        
        # 8. 展示架構圖
        print("\n8️⃣ ETL 架構總結")
        print("=" * 25)
        
        architecture = """
        數據源 → 數據清洗 → 數據轉換 → 數據豐富化 → 聚合處理 → 輸出
           │         │         │           │           │         │
           │         │         │           │           │         ├─ 控制台
           │         │         │           │           │         ├─ 文件
           │         │         │           │           │         └─ 內存表
           │         │         │           │           │
           │         │         │           │           └─ 監控警報
           │         │         │           │
           │         │         │           └─ 維度表聯結
           │         │         │
           │         │         └─ 業務邏輯轉換
           │         │
           │         └─ 品質檢查
           │
           └─ 流式數據攝取
        """
        
        print(architecture)
        
        # 9. 最佳實踐建議
        print("\n9️⃣ 最佳實踐建議")
        print("=" * 25)
        
        best_practices = [
            "✅ 使用 Watermark 處理延遲數據",
            "✅ 適當設置檢查點目錄",
            "✅ 監控流處理性能指標",
            "✅ 實施數據品質檢查",
            "✅ 設置適當的觸發間隔",
            "✅ 使用適當的輸出模式",
            "✅ 實施錯誤處理和恢復機制",
            "✅ 定期清理歷史數據"
        ]
        
        for practice in best_practices:
            print(f"  {practice}")
        
        print("\n✅ 實時 ETL 系統演示完成")
        
    except Exception as e:
        print(f"❌ ETL 管道執行錯誤: {e}")
        
    finally:
        # 清理資源
        etl.cleanup()
        spark.stop()

if __name__ == "__main__":
    main()