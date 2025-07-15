#!/usr/bin/env python3
"""
第3章練習2：複雜數據轉換和聚合
高級 DataFrame 轉換和聚合操作練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace, split, explode, collect_list, \
    count, sum as spark_sum, avg, max as spark_max, min as spark_min, stddev, \
    row_number, rank, dense_rank, lag, lead, first, last, percentile_approx, \
    date_format, year, month, dayofmonth, hour, minute, \
    array, struct, map_keys, map_values, size, sort_array, \
    concat, concat_ws, upper, lower, trim, regexp_extract, \
    round as spark_round, ceil, floor, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("複雜數據轉換和聚合練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第3章練習2：複雜數據轉換和聚合 ===")
    
    # 1. 創建複雜的電商數據集
    print("\n1. 創建複雜的電商數據集:")
    
    # 訂單數據
    orders_data = [
        (1, "CUST001", "2024-01-15 10:30:00", "completed", 1250.00, "online", "["electronics", "books"]"),
        (2, "CUST002", "2024-01-15 14:20:00", "pending", 890.50, "store", "["clothing", "accessories"]"),
        (3, "CUST001", "2024-01-16 09:15:00", "completed", 2100.00, "online", "["electronics"]"),
        (4, "CUST003", "2024-01-16 16:45:00", "cancelled", 567.25, "online", "["books", "toys"]"),
        (5, "CUST002", "2024-01-17 11:30:00", "completed", 1456.75, "store", "["clothing"]"),
        (6, "CUST004", "2024-01-17 13:20:00", "completed", 789.00, "online", "["electronics", "home"]"),
        (7, "CUST001", "2024-01-18 15:10:00", "processing", 345.50, "online", "["books"]"),
        (8, "CUST005", "2024-01-18 10:40:00", "completed", 1890.25, "store", "["electronics", "clothing"]"),
        (9, "CUST003", "2024-01-19 12:15:00", "completed", 623.75, "online", "["toys", "books"]"),
        (10, "CUST004", "2024-01-19 17:30:00", "completed", 1234.00, "online", "["home", "electronics"]")
    ]
    
    orders_columns = ["order_id", "customer_id", "order_date", "status", "total_amount", "channel", "categories"]
    orders_df = spark.createDataFrame(orders_data, orders_columns)
    
    # 客戶數據
    customers_data = [
        ("CUST001", "Alice Johnson", "alice@email.com", "Premium", "New York", 28, "2023-01-15"),
        ("CUST002", "Bob Chen", "bob@email.com", "Gold", "Los Angeles", 35, "2022-06-20"),
        ("CUST003", "Charlie Wu", "charlie@email.com", "Silver", "Chicago", 42, "2023-03-10"),
        ("CUST004", "Diana Lin", "diana@email.com", "Premium", "Houston", 31, "2021-12-05"),
        ("CUST005", "Eve Brown", "eve@email.com", "Gold", "Phoenix", 29, "2022-09-18")
    ]
    
    customers_columns = ["customer_id", "name", "email", "tier", "city", "age", "join_date"]
    customers_df = spark.createDataFrame(customers_data, customers_columns)
    
    print("訂單數據:")
    orders_df.show(truncate=False)
    
    print("客戶數據:")
    customers_df.show(truncate=False)
    
    # 2. 字符串和數組操作
    print("\n2. 字符串和數組操作:")
    
    # 2.1 解析分類字符串為數組
    print("\n2.1 解析分類字符串為數組:")
    
    # 移除方括號並分割成數組
    orders_with_categories = orders_df.withColumn(
        "category_array",
        split(regexp_replace(regexp_replace(col("categories"), "\\[", ""), "\\]", ""), ", ")
    )
    
    print("解析後的分類數組:")
    orders_with_categories.select("order_id", "categories", "category_array").show(truncate=False)
    
    # 2.2 爆炸數組創建行
    print("\n2.2 將數組展開為多行:")
    
    orders_exploded = orders_with_categories.select(
        "order_id", "customer_id", "order_date", "status", "total_amount", "channel",
        explode(col("category_array")).alias("category")
    ).filter(col("category") != "")  # 過濾空值
    
    print("展開後的分類數據:")
    orders_exploded.show()
    
    # 3. 複雜聚合操作
    print("\n3. 複雜聚合操作:")
    
    # 3.1 多維聚合
    print("\n3.1 按客戶和渠道的多維聚合:")
    
    customer_channel_agg = orders_df.groupBy("customer_id", "channel").agg(
        count("*").alias("order_count"),
        spark_sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_order_value"),
        spark_max("total_amount").alias("max_order"),
        spark_min("total_amount").alias("min_order"),
        collect_list("status").alias("order_statuses")
    )
    
    customer_channel_agg.show(truncate=False)
    
    # 3.2 分類級別統計
    print("\n3.2 分類級別統計:")
    
    category_stats = orders_exploded.groupBy("category").agg(
        count("*").alias("order_count"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_revenue"),
        col("category").alias("cat")  # 保持分類名稱
    ).orderBy(col("total_revenue").desc())
    
    print("各分類統計:")
    category_stats.show()
    
    # 4. 窗口函數應用
    print("\n4. 窗口函數應用:")
    
    # 4.1 客戶排名分析
    print("\n4.1 客戶排名分析:")
    
    # 按總消費額排名
    customer_spending = orders_df.groupBy("customer_id").agg(
        spark_sum("total_amount").alias("total_spent"),
        count("*").alias("order_count"),
        avg("total_amount").alias("avg_order_value")
    )
    
    window_rank = Window.orderBy(col("total_spent").desc())
    
    customer_rankings = customer_spending.withColumn(
        "spending_rank", row_number().over(window_rank)
    ).withColumn(
        "spending_dense_rank", dense_rank().over(window_rank)
    )
    
    print("客戶消費排名:")
    customer_rankings.show()
    
    # 4.2 時間序列分析
    print("\n4.2 時間序列分析:")
    
    # 按客戶的訂單時間序列
    window_customer_time = Window.partitionBy("customer_id").orderBy("order_date")
    
    orders_time_analysis = orders_df.withColumn(
        "order_sequence", row_number().over(window_customer_time)
    ).withColumn(
        "prev_order_amount", lag("total_amount", 1).over(window_customer_time)
    ).withColumn(
        "next_order_amount", lead("total_amount", 1).over(window_customer_time)
    ).withColumn(
        "amount_change", col("total_amount") - col("prev_order_amount")
    )
    
    print("客戶訂單時間序列分析:")
    orders_time_analysis.select(
        "customer_id", "order_date", "order_sequence", "total_amount", 
        "prev_order_amount", "amount_change"
    ).show()
    
    # 5. 複雜條件轉換
    print("\n5. 複雜條件轉換:")
    
    # 5.1 客戶分層
    print("\n5.1 基於消費的客戶分層:")
    
    orders_with_segments = customer_spending.withColumn(
        "customer_segment",
        when(col("total_spent") >= 2000, "VIP")
        .when(col("total_spent") >= 1500, "Premium")
        .when(col("total_spent") >= 1000, "Gold")
        .otherwise("Regular")
    ).withColumn(
        "order_frequency",
        when(col("order_count") >= 4, "High")
        .when(col("order_count") >= 2, "Medium")
        .otherwise("Low")
    )
    
    print("客戶分層結果:")
    orders_with_segments.show()
    
    # 5.2 訂單複雜度評分
    print("\n5.2 訂單複雜度評分:")
    
    orders_complexity = orders_with_categories.withColumn(
        "category_count", size(col("category_array"))
    ).withColumn(
        "complexity_score",
        when(col("category_count") >= 3, 3)
        .when(col("category_count") == 2, 2)
        .otherwise(1)
        + when(col("total_amount") >= 1500, 2)
        .when(col("total_amount") >= 800, 1)
        .otherwise(0)
        + when(col("channel") == "online", 1).otherwise(0)
    )
    
    print("訂單複雜度評分:")
    orders_complexity.select(
        "order_id", "category_count", "total_amount", "channel", "complexity_score"
    ).show()
    
    # 6. 數據透視和重整
    print("\n6. 數據透視和重整:")
    
    # 6.1 創建透視表
    print("\n6.1 渠道-狀態透視表:")
    
    # 手動創建透視表效果
    pivot_data = orders_df.groupBy("channel") \
        .agg(
            spark_sum(when(col("status") == "completed", col("total_amount")).otherwise(0)).alias("completed_amount"),
            spark_sum(when(col("status") == "pending", col("total_amount")).otherwise(0)).alias("pending_amount"),
            spark_sum(when(col("status") == "processing", col("total_amount")).otherwise(0)).alias("processing_amount"),
            spark_sum(when(col("status") == "cancelled", col("total_amount")).otherwise(0)).alias("cancelled_amount"),
            count(when(col("status") == "completed", 1)).alias("completed_count"),
            count(when(col("status") == "pending", 1)).alias("pending_count"),
            count(when(col("status") == "processing", 1)).alias("processing_count"),
            count(when(col("status") == "cancelled", 1)).alias("cancelled_count")
        )
    
    pivot_data.show()
    
    # 7. 連接和合併
    print("\n7. 複雜連接操作:")
    
    # 7.1 豐富訂單數據
    print("\n7.1 訂單與客戶數據連接:")
    
    enriched_orders = orders_df.join(customers_df, "customer_id", "inner")
    
    print("豐富後的訂單數據:")
    enriched_orders.select(
        "order_id", "name", "tier", "city", "age", "total_amount", "status"
    ).show()
    
    # 7.2 高級聚合連接
    print("\n7.2 客戶統計與等級分析:")
    
    customer_analysis = enriched_orders.groupBy("customer_id", "name", "tier", "city", "age").agg(
        count("*").alias("total_orders"),
        spark_sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_order_value"),
        spark_max("total_amount").alias("max_order"),
        count(when(col("status") == "completed", 1)).alias("completed_orders"),
        count(when(col("status") == "cancelled", 1)).alias("cancelled_orders")
    ).withColumn(
        "completion_rate", 
        spark_round(col("completed_orders") / col("total_orders") * 100, 2)
    )
    
    print("客戶綜合分析:")
    customer_analysis.show(truncate=False)
    
    # 8. 時間維度分析
    print("\n8. 時間維度分析:")
    
    # 8.1 時間解析和分組
    print("\n8.1 按日期維度分析:")
    
    orders_time_breakdown = orders_df.withColumn(
        "order_year", year(col("order_date"))
    ).withColumn(
        "order_month", month(col("order_date"))
    ).withColumn(
        "order_day", dayofmonth(col("order_date"))
    ).withColumn(
        "order_hour", hour(col("order_date"))
    )
    
    # 按日期聚合
    daily_stats = orders_time_breakdown.groupBy("order_year", "order_month", "order_day").agg(
        count("*").alias("daily_orders"),
        spark_sum("total_amount").alias("daily_revenue"),
        avg("total_amount").alias("avg_daily_order")
    ).orderBy("order_year", "order_month", "order_day")
    
    print("每日統計:")
    daily_stats.show()
    
    # 8.2 小時級別分析
    print("\n8.2 按小時分析:")
    
    hourly_stats = orders_time_breakdown.groupBy("order_hour").agg(
        count("*").alias("hourly_orders"),
        spark_sum("total_amount").alias("hourly_revenue")
    ).orderBy("order_hour")
    
    print("每小時統計:")
    hourly_stats.show()
    
    # 9. 統計和分布分析
    print("\n9. 統計和分布分析:")
    
    # 9.1 訂單金額分布
    print("\n9.1 訂單金額分布分析:")
    
    amount_percentiles = orders_df.select(
        percentile_approx("total_amount", 0.25).alias("q1"),
        percentile_approx("total_amount", 0.5).alias("median"),
        percentile_approx("total_amount", 0.75).alias("q3"),
        percentile_approx("total_amount", 0.9).alias("p90"),
        percentile_approx("total_amount", 0.95).alias("p95")
    )
    
    amount_stats = orders_df.agg(
        count("*").alias("total_orders"),
        avg("total_amount").alias("mean_amount"),
        stddev("total_amount").alias("stddev_amount"),
        spark_min("total_amount").alias("min_amount"),
        spark_max("total_amount").alias("max_amount")
    )
    
    print("訂單金額統計:")
    amount_stats.show()
    
    print("訂單金額分位數:")
    amount_percentiles.show()
    
    # 9.2 金額區間分析
    print("\n9.2 金額區間分析:")
    
    amount_buckets = orders_df.withColumn(
        "amount_bucket",
        when(col("total_amount") < 500, "< $500")
        .when(col("total_amount") < 1000, "$500-$999")
        .when(col("total_amount") < 1500, "$1000-$1499")
        .when(col("total_amount") < 2000, "$1500-$1999")
        .otherwise("≥ $2000")
    )
    
    bucket_analysis = amount_buckets.groupBy("amount_bucket").agg(
        count("*").alias("order_count"),
        (count("*") * 100.0 / orders_df.count()).alias("percentage")
    ).orderBy("order_count")
    
    print("金額區間分布:")
    bucket_analysis.withColumn("percentage", spark_round("percentage", 2)).show()
    
    # 10. 數據品質檢查
    print("\n10. 數據品質檢查:")
    
    # 10.1 完整性檢查
    print("\n10.1 數據完整性檢查:")
    
    completeness_check = orders_df.agg(
        count("*").alias("total_records"),
        count(col("order_id")).alias("order_id_count"),
        count(col("customer_id")).alias("customer_id_count"),
        count(col("total_amount")).alias("amount_count"),
        spark_sum(when(col("total_amount") <= 0, 1).otherwise(0)).alias("invalid_amounts"),
        spark_sum(when(col("status").isin(["completed", "pending", "processing", "cancelled"]), 0).otherwise(1)).alias("invalid_status")
    )
    
    print("數據完整性統計:")
    completeness_check.show()
    
    # 10.2 一致性檢查
    print("\n10.2 客戶一致性檢查:")
    
    # 檢查客戶ID是否在客戶表中存在
    customer_consistency = orders_df.join(
        customers_df.select("customer_id"), 
        "customer_id", 
        "left"
    ).agg(
        count("*").alias("total_orders"),
        spark_sum(when(customers_df.customer_id.isNull(), 1).otherwise(0)).alias("orders_without_customer")
    )
    
    print("客戶一致性檢查:")
    customer_consistency.show()
    
    # 11. 綜合報告
    print("\n11. 綜合分析報告:")
    
    # 生成業務洞察
    total_orders = orders_df.count()
    total_revenue = orders_df.agg(spark_sum("total_amount")).collect()[0][0]
    avg_order_value = orders_df.agg(avg("total_amount")).collect()[0][0]
    completion_rate = orders_df.filter(col("status") == "completed").count() / total_orders * 100
    
    top_customer = customer_analysis.orderBy(col("total_spent").desc()).first()
    top_category = category_stats.first()
    
    print("=== 業務分析摘要 ===")
    print(f"總訂單數: {total_orders}")
    print(f"總收入: ${total_revenue:,.2f}")
    print(f"平均訂單價值: ${avg_order_value:.2f}")
    print(f"訂單完成率: {completion_rate:.1f}%")
    print(f"最佳客戶: {top_customer['name']} (${top_customer['total_spent']:,.2f})")
    print(f"最熱門分類: {top_category['category']} ({top_category['order_count']} 筆訂單)")
    
    # 清理資源
    spark.stop()
    print("\n複雜數據轉換和聚合練習完成！")

if __name__ == "__main__":
    main()