#!/usr/bin/env python3
"""
第5章：Spark Streaming - 流式處理基礎
學習結構化流處理的基本概念和操作
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, current_timestamp, window, count, sum as spark_sum, avg
import time
import threading
import json
import os
import tempfile
from datetime import datetime, timedelta

def generate_sample_data(output_dir, duration_seconds=30):
    """生成示例流數據"""
    import random
    import json
    import time
    from datetime import datetime
    
    users = ["alice", "bob", "charlie", "diana", "eve"]
    products = ["laptop", "phone", "tablet", "watch", "headphones"]
    
    for i in range(duration_seconds):
        # 每秒生成幾筆記錄
        for j in range(random.randint(1, 5)):
            record = {
                "user_id": random.choice(users),
                "product": random.choice(products),
                "quantity": random.randint(1, 3),
                "price": round(random.uniform(10, 1000), 2),
                "timestamp": datetime.now().isoformat()
            }
            
            # 寫入文件
            filename = f"data_{i}_{j}.json"
            with open(os.path.join(output_dir, filename), 'w') as f:
                json.dump(record, f)
        
        time.sleep(1)
    
    print(f"數據生成完成，共生成 {duration_seconds} 秒的數據")

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Spark Streaming Basics") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()
    
    # 降低日誌級別
    spark.sparkContext.setLogLevel("WARN")
    
    print("📺 Spark Streaming 基礎示範")
    print("=" * 40)
    
    # 創建臨時目錄
    temp_dir = tempfile.mkdtemp()
    input_dir = os.path.join(temp_dir, "input")
    output_dir = os.path.join(temp_dir, "output")
    os.makedirs(input_dir, exist_ok=True)
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"臨時目錄: {temp_dir}")
    print(f"輸入目錄: {input_dir}")
    print(f"輸出目錄: {output_dir}")
    
    # 1. 定義數據結構
    print("\n1️⃣ 定義數據結構")
    
    # 定義流數據的 Schema
    schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("product", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", IntegerType(), True),
        StructField("timestamp", StringType(), True)
    ])
    
    print("流數據 Schema:")
    print(schema)
    
    # 2. 創建流數據源
    print("\n2️⃣ 創建流數據源")
    
    # 從文件系統讀取流數據
    streaming_df = spark.readStream \
        .format("json") \
        .schema(schema) \
        .option("path", input_dir) \
        .load()
    
    print("流數據源已創建")
    
    # 3. 基本流處理
    print("\n3️⃣ 基本流處理")
    
    # 添加處理時間戳
    processed_df = streaming_df.withColumn("processing_time", current_timestamp())
    
    # 啟動數據生成線程
    print("啟動數據生成器...")
    generator_thread = threading.Thread(
        target=generate_sample_data,
        args=(input_dir, 15)  # 生成15秒的數據
    )
    generator_thread.daemon = True
    generator_thread.start()
    
    # 基本查詢 - 輸出到控制台
    print("\n開始基本流處理...")
    basic_query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # 讓查詢運行一段時間
    time.sleep(20)
    basic_query.stop()
    
    # 4. 聚合查詢
    print("\n4️⃣ 聚合查詢")
    
    # 按產品統計
    product_stats = streaming_df.groupBy("product") \
        .agg(
            count("*").alias("order_count"),
            spark_sum("quantity").alias("total_quantity"),
            avg("price").alias("avg_price")
        )
    
    print("開始產品統計...")
    aggregation_query = product_stats.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # 讓查詢運行一段時間
    time.sleep(20)
    aggregation_query.stop()
    
    # 5. 視窗操作
    print("\n5️⃣ 視窗操作")
    
    # 先將timestamp轉換為TimestampType
    from pyspark.sql.functions import to_timestamp
    windowed_df = streaming_df.withColumn(
        "event_time", 
        to_timestamp(col("timestamp"))
    )
    
    # 創建時間窗口統計
    windowed_stats = windowed_df \
        .withWatermark("event_time", "1 minute") \
        .groupBy(
            window(col("event_time"), "10 seconds", "5 seconds"),
            col("product")
        ) \
        .agg(
            count("*").alias("count"),
            spark_sum("quantity").alias("total_quantity")
        )
    
    print("開始視窗統計...")
    window_query = windowed_stats.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # 讓查詢運行一段時間
    time.sleep(20)
    window_query.stop()
    
    # 6. 輸出到文件
    print("\n6️⃣ 輸出到文件")
    
    # 寫入到 Parquet 文件
    file_query = processed_df.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", output_dir) \
        .option("checkpointLocation", "/tmp/checkpoint_file") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("開始寫入文件...")
    time.sleep(15)
    file_query.stop()
    
    # 檢查輸出文件
    try:
        output_df = spark.read.parquet(output_dir)
        print(f"輸出文件記錄數: {output_df.count()}")
        print("輸出文件內容預覽:")
        output_df.show(5)
    except Exception as e:
        print(f"讀取輸出文件時出錯: {e}")
    
    # 7. 狀態管理
    print("\n7️⃣ 狀態管理")
    
    # 創建有狀態的流處理
    from pyspark.sql.functions import struct
    
    # 用戶總購買統計
    user_totals = streaming_df.groupBy("user_id") \
        .agg(
            count("*").alias("total_orders"),
            spark_sum("quantity").alias("total_items"),
            spark_sum("price").alias("total_spent")
        )
    
    print("開始用戶統計...")
    user_query = user_totals.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # 重新啟動數據生成器
    generator_thread2 = threading.Thread(
        target=generate_sample_data,
        args=(input_dir, 10)
    )
    generator_thread2.daemon = True
    generator_thread2.start()
    
    time.sleep(15)
    user_query.stop()
    
    # 8. 錯誤處理和監控
    print("\n8️⃣ 錯誤處理和監控")
    
    # 創建一個包含錯誤處理的查詢
    try:
        # 過濾異常數據
        filtered_df = streaming_df.filter(
            (col("quantity") > 0) & 
            (col("price") > 0) & 
            (col("user_id").isNotNull())
        )
        
        monitoring_query = filtered_df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", "false") \
            .trigger(processingTime='5 seconds') \
            .start()
        
        print("開始監控查詢...")
        time.sleep(10)
        
        # 查看查詢狀態
        print(f"查詢狀態: {monitoring_query.status}")
        print(f"查詢進度: {monitoring_query.lastProgress}")
        
        monitoring_query.stop()
        
    except Exception as e:
        print(f"監控查詢錯誤: {e}")
    
    # 9. 多輸出查詢
    print("\n9️⃣ 多輸出查詢")
    
    # 創建多個輸出流
    console_query = processed_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='5 seconds') \
        .queryName("console_output") \
        .start()
    
    # 同時統計
    stats_query = product_stats.writeStream \
        .outputMode("complete") \
        .format("memory") \
        .trigger(processingTime='5 seconds') \
        .queryName("product_stats") \
        .start()
    
    # 重新啟動數據生成器
    generator_thread3 = threading.Thread(
        target=generate_sample_data,
        args=(input_dir, 8)
    )
    generator_thread3.daemon = True
    generator_thread3.start()
    
    time.sleep(12)
    
    # 查看內存表
    try:
        memory_df = spark.sql("SELECT * FROM product_stats")
        print("內存表內容:")
        memory_df.show()
    except Exception as e:
        print(f"查看內存表錯誤: {e}")
    
    # 停止所有查詢
    console_query.stop()
    stats_query.stop()
    
    # 10. 流處理性能調優
    print("\n🔟 流處理性能調優")
    
    # 優化配置的流處理
    optimized_df = streaming_df \
        .coalesce(2) \
        .filter(col("price") > 50) \
        .groupBy("product") \
        .agg(count("*").alias("high_value_orders"))
    
    print("開始優化查詢...")
    optimized_query = optimized_df.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='3 seconds') \
        .start()
    
    # 最後一次數據生成
    generator_thread4 = threading.Thread(
        target=generate_sample_data,
        args=(input_dir, 5)
    )
    generator_thread4.daemon = True
    generator_thread4.start()
    
    time.sleep(10)
    optimized_query.stop()
    
    # 11. 查詢管理
    print("\n1️⃣1️⃣ 查詢管理")
    
    # 顯示所有活動查詢
    active_queries = spark.streams.active
    print(f"活動查詢數量: {len(active_queries)}")
    
    # 等待所有查詢完成
    spark.streams.awaitAnyTermination(timeout=5)
    
    # 清理資源
    print("\n1️⃣2️⃣ 清理資源")
    
    # 清理臨時目錄
    import shutil
    shutil.rmtree(temp_dir)
    print(f"清理臨時目錄: {temp_dir}")
    
    # 停止 SparkSession
    spark.stop()
    print("\n✅ Spark Streaming 基礎示範完成")

if __name__ == "__main__":
    main()