#!/usr/bin/env python3
"""
第5章練習1：Spark Streaming 基礎
實時數據流處理練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, window, count, avg, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import time

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Streaming基礎練習") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 第5章練習1：Spark Streaming 基礎 ===")
    
    # 1. 建立基本的流式數據源（使用 rate source 模擬數據）
    print("\n1. 建立基本流式數據源:")
    
    # Rate source 每秒產生指定數量的記錄
    rate_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 5) \
        .load()
    
    # 2. 基本的流式數據轉換
    print("\n2. 基本流式數據轉換:")
    
    # 添加計算欄位
    transformed_stream = rate_stream \
        .withColumn("doubled_value", col("value") * 2) \
        .withColumn("is_even", col("value") % 2 == 0)
    
    # 3. 控制台輸出查詢
    print("\n3. 啟動流式查詢...")
    
    console_query = transformed_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .option("numRows", 10) \
        .trigger(processingTime='2 seconds') \
        .start()
    
    # 運行短時間以觀察結果
    time.sleep(10)
    console_query.stop()
    
    # 4. 模擬結構化數據流
    print("\n4. 處理結構化數據流:")
    
    # 定義 JSON 結構
    json_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])
    
    # 模擬 JSON 數據流
    json_data_stream = rate_stream \
        .withColumn("json_data", 
            col("value").cast("string").alias("user_id")) \
        .select(
            col("timestamp"),
            (col("value") % 100).cast("string").alias("user_id"),
            ((col("value") % 10) + 1).cast("string").alias("product_id"),
            (col("value") % 1000 + 10.0).alias("price"),
            (col("value") % 5 + 1).alias("quantity")
        )
    
    # 5. 聚合操作
    print("\n5. 流式聚合操作:")
    
    # 按時間窗口聚合
    windowed_counts = json_data_stream \
        .groupBy(
            window(col("timestamp"), "10 seconds", "5 seconds"),
            col("product_id")
        ).agg(
            count("*").alias("order_count"),
            avg("price").alias("avg_price")
        )
    
    # 啟動聚合查詢
    aggregation_query = windowed_counts.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='3 seconds') \
        .start()
    
    # 運行一段時間
    time.sleep(15)
    aggregation_query.stop()
    
    # 6. 文件輸出
    print("\n6. 文件輸出示例:")
    
    # 寫入到文件（Parquet 格式）
    file_query = transformed_stream \
        .writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", "/tmp/spark-streaming-output") \
        .option("checkpointLocation", "/tmp/spark-checkpoint-parquet") \
        .trigger(processingTime='5 seconds') \
        .start()
    
    # 短暫運行以寫入一些文件
    time.sleep(8)
    file_query.stop()
    
    print("\n7. 查詢狀態監控:")
    # 顯示一些基本的流式查詢信息
    print("活躍查詢數量:", len(spark.streams.active))
    
    # 清理資源
    spark.stop()
    print("\n練習完成！")

if __name__ == "__main__":
    main()