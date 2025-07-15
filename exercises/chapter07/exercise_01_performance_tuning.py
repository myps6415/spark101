#!/usr/bin/env python3
"""
第7章練習1：Spark 性能調優
性能優化技巧和實踐練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast, count, avg, sum as spark_sum, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

def main():
    # 創建 SparkSession（配置調優參數）
    spark = SparkSession.builder \
        .appName("性能調優練習") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 第7章練習1：Spark 性能調優 ===")
    
    # 1. 創建大數據集進行性能測試
    print("\n1. 創建測試數據集:")
    
    # 創建大型銷售數據
    num_records = 100000
    sales_data = []
    
    for i in range(num_records):
        sales_data.append((
            f"ORD-{i:06d}",
            f"CUST-{i % 1000:04d}",
            f"PROD-{i % 100:03d}",
            i % 10 + 1,  # quantity
            (i % 1000 + 10) * 1.5,  # price
            f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"  # date
        ))
    
    columns = ["order_id", "customer_id", "product_id", "quantity", "price", "order_date"]
    sales_df = spark.createDataFrame(sales_data, columns)
    
    print(f"創建了 {sales_df.count()} 筆銷售記錄")
    
    # 2. 分區策略優化
    print("\n2. 分區策略優化:")
    
    print(f"原始分區數: {sales_df.rdd.getNumPartitions()}")
    
    # 重新分區
    sales_partitioned = sales_df.repartition(8, "customer_id")
    print(f"重新分區後: {sales_partitioned.rdd.getNumPartitions()}")
    
    # 緩存優化
    print("\n3. 緩存策略:")
    
    # 緩存經常使用的數據
    sales_cached = sales_partitioned.cache()
    
    # 第一次訪問（觸發緩存）
    start_time = time.time()
    count1 = sales_cached.count()
    time1 = time.time() - start_time
    print(f"第一次計數 (緩存): {count1} 筆，耗時: {time1:.3f} 秒")
    
    # 第二次訪問（使用緩存）
    start_time = time.time()
    count2 = sales_cached.count()
    time2 = time.time() - start_time
    print(f"第二次計數 (緩存): {count2} 筆，耗時: {time2:.3f} 秒")
    print(f"性能提升: {((time1 - time2) / time1 * 100):.1f}%")
    
    # 4. Join 優化策略
    print("\n4. Join 優化策略:")
    
    # 創建小的維度表
    product_data = [(f"PROD-{i:03d}", f"Product {i}", f"Category {i % 10}") 
                   for i in range(100)]
    product_df = spark.createDataFrame(product_data, ["product_id", "product_name", "category"])
    
    # 普通 Join
    start_time = time.time()
    normal_join = sales_cached.join(product_df, "product_id")
    normal_count = normal_join.count()
    normal_time = time.time() - start_time
    print(f"普通 Join: {normal_count} 筆，耗時: {normal_time:.3f} 秒")
    
    # Broadcast Join 優化
    start_time = time.time()
    broadcast_join = sales_cached.join(broadcast(product_df), "product_id")
    broadcast_count = broadcast_join.count()
    broadcast_time = time.time() - start_time
    print(f"Broadcast Join: {broadcast_count} 筆，耗時: {broadcast_time:.3f} 秒")
    print(f"Join 優化提升: {((normal_time - broadcast_time) / normal_time * 100):.1f}%")
    
    # 5. 聚合優化
    print("\n5. 聚合操作優化:")
    
    # 多階段聚合
    start_time = time.time()
    customer_stats = sales_cached.groupBy("customer_id").agg(
        count("*").alias("order_count"),
        spark_sum("quantity").alias("total_quantity"),
        avg("price").alias("avg_price"),
        spark_max("price").alias("max_price")
    )
    
    # 強制執行並收集結果
    customer_results = customer_stats.collect()
    agg_time = time.time() - start_time
    print(f"客戶聚合統計: {len(customer_results)} 位客戶，耗時: {agg_time:.3f} 秒")
    
    # 6. 列存儲優化
    print("\n6. 列存儲格式優化:")
    
    # 寫入 Parquet 格式
    parquet_path = "/tmp/sales_parquet"
    sales_cached.write.mode("overwrite").parquet(parquet_path)
    
    # 讀取 Parquet
    start_time = time.time()
    parquet_df = spark.read.parquet(parquet_path)
    parquet_count = parquet_df.count()
    parquet_time = time.time() - start_time
    print(f"Parquet 讀取: {parquet_count} 筆，耗時: {parquet_time:.3f} 秒")
    
    # 7. 謂詞下推優化
    print("\n7. 謂詞下推優化:")
    
    # 使用列式存儲的過濾優勢
    start_time = time.time()
    filtered_orders = parquet_df.filter(
        (col("quantity") > 5) & 
        (col("price") > 500)
    ).select("order_id", "customer_id", "quantity", "price")
    
    filtered_count = filtered_orders.count()
    filter_time = time.time() - start_time
    print(f"過濾查詢: {filtered_count} 筆，耗時: {filter_time:.3f} 秒")
    
    # 8. 內存管理監控
    print("\n8. 資源使用監控:")
    
    sc = spark.sparkContext
    status = sc.statusTracker()
    
    print(f"活躍作業數: {len(status.getActiveJobIds())}")
    print(f"執行器數量: {len(status.getExecutorInfos())}")
    
    # 顯示緩存信息
    for rdd_info in sc.statusTracker().getRddInfos():
        if rdd_info.numCachedPartitions > 0:
            print(f"RDD {rdd_info.id}: {rdd_info.numCachedPartitions}/{rdd_info.numPartitions} 分區已緩存")
    
    # 9. SQL 優化示例
    print("\n9. SQL 查詢優化:")
    
    # 註冊臨時視圖
    sales_cached.createOrReplaceTempView("sales")
    product_df.createOrReplaceTempView("products")
    
    # 優化的 SQL 查詢
    sql_query = """
    SELECT 
        p.category,
        COUNT(*) as order_count,
        SUM(s.quantity * s.price) as total_revenue,
        AVG(s.price) as avg_price
    FROM sales s
    JOIN products p ON s.product_id = p.product_id
    WHERE s.quantity > 2
    GROUP BY p.category
    ORDER BY total_revenue DESC
    """
    
    start_time = time.time()
    sql_result = spark.sql(sql_query)
    sql_result.show()
    sql_time = time.time() - start_time
    print(f"SQL 查詢耗時: {sql_time:.3f} 秒")
    
    # 10. 性能調優總結
    print("\n10. 性能調優總結:")
    print("主要優化技術:")
    print("- 適當的分區策略")
    print("- 數據緩存機制")
    print("- Broadcast Join 優化")
    print("- 列存儲格式 (Parquet)")
    print("- 謂詞下推")
    print("- 自適應查詢執行 (AQE)")
    
    # 清理緩存
    sales_cached.unpersist()
    
    # 清理資源
    spark.stop()
    print("\n練習完成！")

if __name__ == "__main__":
    main()