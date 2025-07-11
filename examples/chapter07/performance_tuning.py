#!/usr/bin/env python3
"""
第7章：性能調優 - Spark 性能優化
學習 Spark 的性能調優技巧和最佳實踐
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, broadcast
from pyspark.sql.functions import rand, randn, when, lit, monotonically_increasing_id
import time
import tempfile
import os

def measure_execution_time(func, description):
    """測量函數執行時間"""
    start_time = time.time()
    result = func()
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"{description}: {execution_time:.2f} 秒")
    return result, execution_time

def create_large_dataset(spark, size=100000):
    """創建大型測試數據集"""
    return spark.range(size) \
        .withColumn("value", (col("id") * 2).cast("double")) \
        .withColumn("category", (col("id") % 100).cast("string")) \
        .withColumn("random_value", rand(seed=42) * 1000) \
        .withColumn("status", when(col("id") % 5 == 0, "active").otherwise("inactive"))

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Performance Tuning") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("⚡ Spark 性能調優示範")
    print("=" * 40)
    
    # 1. 數據分區策略
    print("\n1️⃣ 數據分區策略")
    
    # 創建測試數據
    df = create_large_dataset(spark, 1000000)
    
    print(f"原始數據分區數: {df.rdd.getNumPartitions()}")
    print(f"數據行數: {df.count()}")
    
    # 重新分區
    df_repartitioned = df.repartition(4)
    print(f"重新分區後分區數: {df_repartitioned.rdd.getNumPartitions()}")
    
    # 按列分區
    df_partitioned_by_col = df.repartition(col("category"))
    print(f"按列分區後分區數: {df_partitioned_by_col.rdd.getNumPartitions()}")
    
    # 合併分區
    df_coalesced = df.coalesce(2)
    print(f"合併分區後分區數: {df_coalesced.rdd.getNumPartitions()}")
    
    # 2. 緩存策略
    print("\n2️⃣ 緩存策略")
    
    # 不使用緩存
    def without_cache():
        result1 = df.filter(col("status") == "active").count()
        result2 = df.filter(col("status") == "active").agg(avg("value")).collect()
        return result1, result2
    
    _, time_without_cache = measure_execution_time(without_cache, "不使用緩存")
    
    # 使用緩存
    df_cached = df.cache()
    
    def with_cache():
        result1 = df_cached.filter(col("status") == "active").count()
        result2 = df_cached.filter(col("status") == "active").agg(avg("value")).collect()
        return result1, result2
    
    _, time_with_cache = measure_execution_time(with_cache, "使用緩存")
    
    print(f"緩存改善: {((time_without_cache - time_with_cache) / time_without_cache * 100):.1f}%")
    
    # 3. 不同的緩存級別
    print("\n3️⃣ 不同的緩存級別")
    
    from pyspark import StorageLevel
    
    # 內存緩存
    df_memory = df.persist(StorageLevel.MEMORY_ONLY)
    
    # 內存+磁盤緩存
    df_memory_disk = df.persist(StorageLevel.MEMORY_AND_DISK)
    
    # 序列化緩存
    df_serialized = df.persist(StorageLevel.MEMORY_ONLY_SER)
    
    print("不同緩存級別已設置")
    
    # 4. 廣播變量
    print("\n4️⃣ 廣播變量")
    
    # 創建小表
    small_df = spark.createDataFrame([
        ("0", "Category A"),
        ("1", "Category B"),
        ("2", "Category C")
    ], ["id", "name"])
    
    # 不使用廣播
    def without_broadcast():
        return df.join(small_df, df.category == small_df.id, "left").count()
    
    _, time_without_broadcast = measure_execution_time(without_broadcast, "不使用廣播")
    
    # 使用廣播
    def with_broadcast():
        return df.join(broadcast(small_df), df.category == small_df.id, "left").count()
    
    _, time_with_broadcast = measure_execution_time(with_broadcast, "使用廣播")
    
    print(f"廣播改善: {((time_without_broadcast - time_with_broadcast) / time_without_broadcast * 100):.1f}%")
    
    # 5. 數據傾斜處理
    print("\n5️⃣ 數據傾斜處理")
    
    # 創建傾斜數據
    skewed_data = spark.range(100000) \
        .withColumn("key", when(col("id") % 100 == 0, "hot_key").otherwise(col("id").cast("string"))) \
        .withColumn("value", rand() * 1000)
    
    print("傾斜數據分布:")
    skewed_data.groupBy("key").count().orderBy(col("count").desc()).show(10)
    
    # 處理數據傾斜 - 加鹽
    salted_data = skewed_data.withColumn("salted_key", 
                                       when(col("key") == "hot_key", 
                                            col("key") + "_" + (rand() * 10).cast("int").cast("string"))
                                       .otherwise(col("key")))
    
    print("加鹽後的數據分布:")
    salted_data.groupBy("salted_key").count().orderBy(col("count").desc()).show(10)
    
    # 6. 執行計劃優化
    print("\n6️⃣ 執行計劃優化")
    
    # 查看執行計劃
    complex_query = df.filter(col("status") == "active") \
        .groupBy("category") \
        .agg(avg("value").alias("avg_value"), count("*").alias("count")) \
        .filter(col("count") > 100) \
        .orderBy("avg_value")
    
    print("查詢執行計劃:")
    complex_query.explain(True)
    
    # 7. 自適應查詢執行 (AQE)
    print("\n7️⃣ 自適應查詢執行 (AQE)")
    
    # 創建兩個大小不同的表
    large_df = create_large_dataset(spark, 100000)
    small_df_for_join = spark.range(1000).withColumn("join_key", col("id").cast("string"))
    
    # AQE 會自動優化這個查詢
    aqe_query = large_df.join(small_df_for_join, large_df.category == small_df_for_join.join_key, "left")
    
    _, aqe_time = measure_execution_time(lambda: aqe_query.count(), "AQE 優化查詢")
    
    # 8. 序列化優化
    print("\n8️⃣ 序列化優化")
    
    # 創建複雜對象
    complex_df = df.withColumn("complex_col", 
                              when(col("id") % 2 == 0, "even_" + col("id").cast("string"))
                              .otherwise("odd_" + col("id").cast("string")))
    
    # 測試序列化性能
    def serialization_test():
        return complex_df.rdd.map(lambda x: (x.id, x.complex_col)).take(1000)
    
    _, serialization_time = measure_execution_time(serialization_test, "序列化測試")
    
    # 9. 內存管理
    print("\n9️⃣ 內存管理")
    
    # 獲取內存使用情況
    storage_level = df_cached.storageLevel
    print(f"緩存存儲級別: {storage_level}")
    
    # 檢查 RDD 緩存狀態
    rdd_info = spark.sparkContext.statusTracker().getExecutorInfos()
    for info in rdd_info:
        print(f"執行器 {info.executorId}: 內存使用 {info.memoryUsed}/{info.maxMemory}")
    
    # 10. 分區修剪
    print("\n🔟 分區修剪")
    
    # 創建分區數據
    temp_dir = tempfile.mkdtemp()
    partitioned_path = os.path.join(temp_dir, "partitioned_data")
    
    # 按狀態分區寫入
    df.write.mode("overwrite").partitionBy("status").parquet(partitioned_path)
    
    # 讀取分區數據
    partitioned_df = spark.read.parquet(partitioned_path)
    
    # 分區修剪查詢
    def partition_pruning():
        return partitioned_df.filter(col("status") == "active").count()
    
    _, pruning_time = measure_execution_time(partition_pruning, "分區修剪查詢")
    
    # 11. 預聚合優化
    print("\n1️⃣1️⃣ 預聚合優化")
    
    # 預聚合數據
    pre_aggregated = df.groupBy("category", "status") \
        .agg(count("*").alias("count"), avg("value").alias("avg_value")) \
        .cache()
    
    # 使用預聚合數據
    def with_pre_aggregation():
        return pre_aggregated.filter(col("status") == "active").agg(spark_sum("count")).collect()
    
    _, pre_agg_time = measure_execution_time(with_pre_aggregation, "預聚合查詢")
    
    # 12. 列式存儲優化
    print("\n1️⃣2️⃣ 列式存儲優化")
    
    # 寫入 Parquet 格式
    parquet_path = os.path.join(temp_dir, "parquet_data")
    df.write.mode("overwrite").parquet(parquet_path)
    
    # 讀取 Parquet 並測試列修剪
    parquet_df = spark.read.parquet(parquet_path)
    
    def column_pruning():
        return parquet_df.select("id", "value").filter(col("value") > 500).count()
    
    _, column_pruning_time = measure_execution_time(column_pruning, "列修剪查詢")
    
    # 13. 連接優化
    print("\n1️⃣3️⃣ 連接優化")
    
    # 創建兩個表進行連接
    df1 = spark.range(10000).withColumn("key", col("id") % 1000)
    df2 = spark.range(1000).withColumn("key", col("id")).withColumn("info", lit("info"))
    
    # 排序合併連接
    def sort_merge_join():
        return df1.join(df2, "key", "left").count()
    
    _, smj_time = measure_execution_time(sort_merge_join, "排序合併連接")
    
    # 廣播雜湊連接
    def broadcast_hash_join():
        return df1.join(broadcast(df2), "key", "left").count()
    
    _, bhj_time = measure_execution_time(broadcast_hash_join, "廣播雜湊連接")
    
    print(f"連接優化改善: {((smj_time - bhj_time) / smj_time * 100):.1f}%")
    
    # 14. 動態分區修剪
    print("\n1️⃣4️⃣ 動態分區修剪")
    
    # 創建維度表
    dim_table = spark.range(10).withColumn("status", when(col("id") % 2 == 0, "active").otherwise("inactive"))
    
    # 動態分區修剪查詢
    def dynamic_partition_pruning():
        return partitioned_df.join(dim_table, "status", "inner").count()
    
    _, dpp_time = measure_execution_time(dynamic_partition_pruning, "動態分區修剪")
    
    # 15. 性能監控
    print("\n1️⃣5️⃣ 性能監控")
    
    # 查詢歷史記錄
    print("Spark 應用程式信息:")
    print(f"應用程式名稱: {spark.sparkContext.appName}")
    print(f"應用程式 ID: {spark.sparkContext.applicationId}")
    print(f"Spark 版本: {spark.version}")
    
    # 獲取執行器信息
    executor_infos = spark.sparkContext.statusTracker().getExecutorInfos()
    print(f"\n執行器數量: {len(executor_infos)}")
    
    for executor in executor_infos:
        print(f"執行器 {executor.executorId}:")
        print(f"  - 主機: {executor.host}")
        print(f"  - 核心數: {executor.totalCores}")
        print(f"  - 內存使用: {executor.memoryUsed}/{executor.maxMemory}")
        print(f"  - 活動任務: {executor.activeTasks}")
    
    # 16. 資源配置建議
    print("\n1️⃣6️⃣ 資源配置建議")
    
    # 計算建議的配置
    total_cores = sum(executor.totalCores for executor in executor_infos)
    total_memory = sum(executor.maxMemory for executor in executor_infos)
    
    print("配置建議:")
    print(f"  - 總核心數: {total_cores}")
    print(f"  - 總內存: {total_memory / (1024**3):.2f} GB")
    print(f"  - 建議分區數: {total_cores * 2} - {total_cores * 4}")
    print(f"  - 建議執行器內存: {total_memory / len(executor_infos) / (1024**3):.2f} GB")
    
    # 17. 清理資源
    print("\n1️⃣7️⃣ 清理資源")
    
    # 清除緩存
    df_cached.unpersist()
    df_memory.unpersist()
    df_memory_disk.unpersist()
    df_serialized.unpersist()
    pre_aggregated.unpersist()
    
    # 清理臨時文件
    import shutil
    shutil.rmtree(temp_dir)
    
    print("資源清理完成")
    
    # 18. 性能最佳實踐總結
    print("\n1️⃣8️⃣ 性能最佳實踐總結")
    
    best_practices = [
        "✅ 適當的分區策略（避免小文件問題）",
        "✅ 合理使用緩存（避免過度緩存）",
        "✅ 廣播小表進行連接",
        "✅ 處理數據傾斜問題",
        "✅ 啟用自適應查詢執行 (AQE)",
        "✅ 使用列式存儲格式（如 Parquet）",
        "✅ 預聚合頻繁查詢的數據",
        "✅ 優化序列化配置",
        "✅ 調整內存分配",
        "✅ 監控和調優資源使用"
    ]
    
    print("性能優化最佳實踐:")
    for practice in best_practices:
        print(f"  {practice}")
    
    # 停止 SparkSession
    spark.stop()
    print("\n✅ Spark 性能調優示範完成")

if __name__ == "__main__":
    main()