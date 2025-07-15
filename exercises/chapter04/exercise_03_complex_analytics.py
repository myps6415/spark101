#!/usr/bin/env python3
"""
第4章練習3：複雜數據分析和報表
高級分析查詢和商業智能報表練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, case, coalesce, isnull, isnan, \
    count, sum as spark_sum, avg, max as spark_max, min as spark_min, \
    stddev, variance, skewness, kurtosis, corr, \
    percentile_approx, approx_count_distinct, \
    row_number, rank, dense_rank, ntile, lag, lead, \
    date_format, year, month, quarter, dayofweek, \
    concat, concat_ws, regexp_replace, split, \
    array, struct, explode, collect_list, collect_set, \
    round as spark_round, floor, ceil, \
    broadcast
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("複雜數據分析和報表練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第4章練習3：複雜數據分析和報表 ===")
    
    # 1. 創建電商業務數據
    print("\n1. 創建電商業務數據:")
    
    # 訂單數據
    orders_data = [
        (1, "CUST001", "2024-01-15", 1250.00, "completed", "online", "electronics", 3),
        (2, "CUST002", "2024-01-16", 890.50, "completed", "store", "clothing", 2),
        (3, "CUST001", "2024-01-20", 2100.00, "completed", "online", "electronics", 5),
        (4, "CUST003", "2024-01-22", 567.25, "cancelled", "online", "books", 1),
        (5, "CUST002", "2024-02-01", 1456.75, "completed", "store", "clothing", 4),
        (6, "CUST004", "2024-02-03", 789.00, "completed", "online", "home", 2),
        (7, "CUST001", "2024-02-10", 345.50, "pending", "online", "books", 1),
        (8, "CUST005", "2024-02-15", 1890.25, "completed", "store", "electronics", 6),
        (9, "CUST003", "2024-02-20", 623.75, "completed", "online", "clothing", 3),
        (10, "CUST004", "2024-03-01", 1234.00, "completed", "online", "home", 4),
        (11, "CUST006", "2024-03-05", 456.30, "completed", "store", "books", 2),
        (12, "CUST005", "2024-03-10", 2345.80, "completed", "online", "electronics", 7),
        (13, "CUST002", "2024-03-15", 678.90, "processing", "store", "clothing", 2),
        (14, "CUST007", "2024-03-20", 987.65, "completed", "online", "home", 3),
        (15, "CUST001", "2024-03-25", 1567.40, "completed", "online", "electronics", 4)
    ]
    
    orders_columns = ["order_id", "customer_id", "order_date", "amount", "status", "channel", "category", "items_count"]
    orders_df = spark.createDataFrame(orders_data, orders_columns)
    
    # 客戶數據
    customers_data = [
        ("CUST001", "Alice Johnson", "Premium", "New York", 28, "2023-01-15", 4.5),
        ("CUST002", "Bob Chen", "Gold", "Los Angeles", 35, "2022-06-20", 4.2),
        ("CUST003", "Charlie Wu", "Silver", "Chicago", 42, "2023-03-10", 3.8),
        ("CUST004", "Diana Lin", "Premium", "Houston", 31, "2021-12-05", 4.7),
        ("CUST005", "Eve Brown", "Gold", "Phoenix", 29, "2022-09-18", 4.3),
        ("CUST006", "Frank Davis", "Silver", "Philadelphia", 38, "2023-05-22", 3.9),
        ("CUST007", "Grace Wilson", "Bronze", "San Antonio", 26, "2023-08-30", 3.5)
    ]
    
    customers_columns = ["customer_id", "name", "tier", "city", "age", "join_date", "satisfaction_score"]
    customers_df = spark.createDataFrame(customers_data, customers_columns)
    
    print("訂單數據:")
    orders_df.show()
    
    print("客戶數據:")
    customers_df.show()
    
    # 2. 基礎業務指標分析
    print("\n2. 基礎業務指標分析:")
    
    # 2.1 整體業務概覽
    print("\n2.1 整體業務概覽:")
    
    business_overview = orders_df.agg(
        count("*").alias("total_orders"),
        approx_count_distinct("customer_id").alias("unique_customers"),
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        spark_max("amount").alias("max_order_value"),
        spark_min("amount").alias("min_order_value"),
        stddev("amount").alias("order_value_stddev"),
        percentile_approx("amount", 0.5).alias("median_order_value"),
        percentile_approx("amount", 0.25).alias("q1_order_value"),
        percentile_approx("amount", 0.75).alias("q3_order_value")
    )
    
    print("業務概覽:")
    business_overview.show()
    
    # 2.2 訂單狀態分析
    print("\n2.2 訂單狀態分析:")
    
    status_analysis = orders_df.groupBy("status").agg(
        count("*").alias("order_count"),
        (count("*") * 100.0 / orders_df.count()).alias("percentage"),
        spark_sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount")
    ).orderBy(col("order_count").desc())
    
    print("訂單狀態分布:")
    status_analysis.withColumn("percentage", spark_round("percentage", 2)).show()
    
    # 3. 時間維度分析
    print("\n3. 時間維度分析:")
    
    # 3.1 月度趨勢分析
    print("\n3.1 月度趨勢分析:")
    
    monthly_trends = orders_df.withColumn(
        "order_month", date_format(col("order_date"), "yyyy-MM")
    ).groupBy("order_month").agg(
        count("*").alias("monthly_orders"),
        spark_sum("amount").alias("monthly_revenue"),
        avg("amount").alias("avg_order_value"),
        approx_count_distinct("customer_id").alias("unique_customers"),
        spark_sum("items_count").alias("total_items")
    ).orderBy("order_month")
    
    # 計算月度增長率
    window_monthly = Window.orderBy("order_month")
    monthly_growth = monthly_trends.withColumn(
        "prev_month_revenue", lag("monthly_revenue", 1).over(window_monthly)
    ).withColumn(
        "revenue_growth_rate",
        ((col("monthly_revenue") - col("prev_month_revenue")) / col("prev_month_revenue") * 100)
    ).withColumn(
        "prev_month_orders", lag("monthly_orders", 1).over(window_monthly)
    ).withColumn(
        "order_growth_rate",
        ((col("monthly_orders") - col("prev_month_orders")) / col("prev_month_orders") * 100)
    )
    
    print("月度趨勢分析:")
    monthly_growth.select(
        "order_month", "monthly_orders", "monthly_revenue", 
        "revenue_growth_rate", "order_growth_rate", "avg_order_value"
    ).show()
    
    # 3.2 季節性分析
    print("\n3.2 季節性分析:")
    
    seasonal_analysis = orders_df.withColumn(
        "quarter", quarter(col("order_date"))
    ).withColumn(
        "day_of_week", dayofweek(col("order_date"))
    ).groupBy("quarter").agg(
        count("*").alias("orders_count"),
        spark_sum("amount").alias("revenue"),
        avg("amount").alias("avg_order_value")
    ).orderBy("quarter")
    
    print("季度分析:")
    seasonal_analysis.show()
    
    # 4. 客戶分析
    print("\n4. 客戶分析:")
    
    # 4.1 客戶價值分析
    print("\n4.1 客戶價值分析:")
    
    # 計算客戶終身價值指標
    customer_value_analysis = orders_df.filter(col("status") == "completed") \
        .groupBy("customer_id").agg(
            count("*").alias("total_orders"),
            spark_sum("amount").alias("total_spent"),
            avg("amount").alias("avg_order_value"),
            spark_max("amount").alias("max_order_value"),
            spark_min("order_date").alias("first_order_date"),
            spark_max("order_date").alias("last_order_date"),
            collect_set("category").alias("categories_purchased"),
            spark_sum("items_count").alias("total_items")
        ).withColumn(
            "customer_segment",
            when(col("total_spent") >= 3000, "VIP")
            .when(col("total_spent") >= 2000, "High Value")
            .when(col("total_spent") >= 1000, "Medium Value")
            .otherwise("Low Value")
        ).withColumn(
            "purchase_frequency",
            when(col("total_orders") >= 4, "High")
            .when(col("total_orders") >= 2, "Medium")
            .otherwise("Low")
        )
    
    # 與客戶主數據關聯
    customer_enriched = customer_value_analysis.join(customers_df, "customer_id", "inner")
    
    print("客戶價值分析:")
    customer_enriched.select(
        "customer_id", "name", "tier", "total_orders", "total_spent", 
        "customer_segment", "purchase_frequency", "satisfaction_score"
    ).orderBy(col("total_spent").desc()).show()
    
    # 4.2 客戶分群分析
    print("\n4.2 客戶分群分析:")
    
    # RFM 分析 (Recency, Frequency, Monetary)
    from datetime import datetime, timedelta
    
    # 假設分析日期為 2024-04-01
    analysis_date = "2024-04-01"
    
    rfm_analysis = orders_df.filter(col("status") == "completed") \
        .groupBy("customer_id").agg(
            spark_max("order_date").alias("last_order_date"),
            count("*").alias("frequency"),
            spark_sum("amount").alias("monetary")
        ).withColumn(
            "recency_days",
            datediff(lit(analysis_date), col("last_order_date"))
        ).withColumn(
            "recency_score",
            when(col("recency_days") <= 30, 5)
            .when(col("recency_days") <= 60, 4)
            .when(col("recency_days") <= 90, 3)
            .when(col("recency_days") <= 180, 2)
            .otherwise(1)
        ).withColumn(
            "frequency_score",
            when(col("frequency") >= 5, 5)
            .when(col("frequency") >= 4, 4)
            .when(col("frequency") >= 3, 3)
            .when(col("frequency") >= 2, 2)
            .otherwise(1)
        ).withColumn(
            "monetary_score",
            when(col("monetary") >= 3000, 5)
            .when(col("monetary") >= 2000, 4)
            .when(col("monetary") >= 1000, 3)
            .when(col("monetary") >= 500, 2)
            .otherwise(1)
        ).withColumn(
            "rfm_score",
            concat(col("recency_score"), col("frequency_score"), col("monetary_score"))
        ).withColumn(
            "customer_type",
            when(col("rfm_score").isin("555", "554", "544", "545", "454", "455", "445"), "Champions")
            .when(col("rfm_score").isin("543", "444", "435", "355", "354", "345", "344", "335"), "Loyal Customers")
            .when(col("rfm_score").isin("512", "511", "422", "421", "412", "411", "311"), "Potential Loyalists")
            .when(col("rfm_score").isin("533", "532", "531", "523", "522", "521", "515", "514", "513", "425", "424", "413", "414", "415"), "New Customers")
            .when(col("rfm_score").isin("155", "154", "144", "214", "215", "115", "114"), "At Risk")
            .when(col("rfm_score").isin("155", "154", "144", "214", "215", "115"), "Cannot Lose Them")
            .otherwise("Others")
        )
    
    # 與客戶信息合併
    rfm_with_customers = rfm_analysis.join(customers_df, "customer_id", "inner")
    
    print("RFM 客戶分群:")
    rfm_with_customers.select(
        "customer_id", "name", "recency_days", "frequency", "monetary", 
        "rfm_score", "customer_type"
    ).orderBy("monetary", ascending=False).show()
    
    # 5. 產品和分類分析
    print("\n5. 產品和分類分析:")
    
    # 5.1 分類績效分析
    print("\n5.1 分類績效分析:")
    
    category_performance = orders_df.filter(col("status") == "completed") \
        .groupBy("category").agg(
            count("*").alias("order_count"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            spark_sum("items_count").alias("total_items"),
            approx_count_distinct("customer_id").alias("unique_customers")
        ).withColumn(
            "revenue_per_customer",
            col("total_revenue") / col("unique_customers")
        ).withColumn(
            "items_per_order",
            col("total_items") / col("order_count")
        )
    
    # 計算分類排名
    window_category = Window.orderBy(col("total_revenue").desc())
    category_ranked = category_performance.withColumn(
        "revenue_rank", row_number().over(window_category)
    ).withColumn(
        "revenue_share",
        col("total_revenue") / spark_sum("total_revenue").over(Window.partitionBy()) * 100
    )
    
    print("分類績效排名:")
    category_ranked.select(
        "category", "order_count", "total_revenue", "revenue_rank", 
        "revenue_share", "avg_order_value", "unique_customers"
    ).orderBy("revenue_rank").show()
    
    # 5.2 交叉銷售分析
    print("\n5.2 交叉銷售分析:")
    
    # 分析客戶購買的分類組合
    customer_categories = orders_df.filter(col("status") == "completed") \
        .groupBy("customer_id").agg(
            collect_set("category").alias("categories"),
            count("*").alias("total_orders"),
            approx_count_distinct("category").alias("unique_categories")
        ).withColumn(
            "category_diversity",
            when(col("unique_categories") >= 4, "High Diversity")
            .when(col("unique_categories") >= 2, "Medium Diversity")
            .otherwise("Low Diversity")
        )
    
    print("客戶分類多樣性:")
    customer_categories.select(
        "customer_id", "categories", "unique_categories", "category_diversity"
    ).show(truncate=False)
    
    # 6. 渠道分析
    print("\n6. 渠道分析:")
    
    # 6.1 渠道績效對比
    print("\n6.1 渠道績效對比:")
    
    channel_performance = orders_df.filter(col("status") == "completed") \
        .groupBy("channel").agg(
            count("*").alias("order_count"),
            spark_sum("amount").alias("total_revenue"),
            avg("amount").alias("avg_order_value"),
            spark_max("amount").alias("max_order_value"),
            spark_min("amount").alias("min_order_value"),
            approx_count_distinct("customer_id").alias("unique_customers"),
            stddev("amount").alias("order_value_stddev")
        ).withColumn(
            "revenue_per_customer",
            col("total_revenue") / col("unique_customers")
        )
    
    print("渠道績效對比:")
    channel_performance.show()
    
    # 6.2 渠道偏好分析
    print("\n6.2 客戶渠道偏好分析:")
    
    customer_channel_preference = orders_df.filter(col("status") == "completed") \
        .groupBy("customer_id", "channel").agg(
            count("*").alias("orders_in_channel"),
            spark_sum("amount").alias("amount_in_channel")
        )
    
    # 計算每個客戶的主要渠道
    window_customer = Window.partitionBy("customer_id").orderBy(col("orders_in_channel").desc())
    
    primary_channel = customer_channel_preference.withColumn(
        "channel_rank", row_number().over(window_customer)
    ).filter(col("channel_rank") == 1) \
    .select("customer_id", col("channel").alias("primary_channel"), "orders_in_channel", "amount_in_channel")
    
    # 與客戶信息合併
    customer_channel_analysis = primary_channel.join(customers_df, "customer_id", "inner")
    
    print("客戶主要渠道偏好:")
    customer_channel_analysis.select(
        "customer_id", "name", "tier", "primary_channel", "orders_in_channel"
    ).orderBy("customer_id").show()
    
    # 7. 高級統計分析
    print("\n7. 高級統計分析:")
    
    # 7.1 相關性分析
    print("\n7.1 相關性分析:")
    
    # 計算數值變數間的相關性
    correlation_analysis = orders_df.select(
        corr("amount", "items_count").alias("amount_items_corr"),
        corr("amount", "order_id").alias("amount_time_corr")  # 用order_id代表時間順序
    )
    
    print("相關性分析:")
    correlation_analysis.show()
    
    # 7.2 分布分析
    print("\n7.2 訂單金額分布分析:")
    
    distribution_stats = orders_df.select(
        avg("amount").alias("mean"),
        stddev("amount").alias("stddev"),
        skewness("amount").alias("skewness"),
        kurtosis("amount").alias("kurtosis"),
        percentile_approx("amount", 0.1).alias("p10"),
        percentile_approx("amount", 0.25).alias("p25"),
        percentile_approx("amount", 0.5).alias("p50"),
        percentile_approx("amount", 0.75).alias("p75"),
        percentile_approx("amount", 0.9).alias("p90")
    )
    
    print("訂單金額分布統計:")
    distribution_stats.show()
    
    # 8. 異常值檢測
    print("\n8. 異常值檢測:")
    
    # 8.1 統計異常值檢測
    print("\n8.1 統計異常值檢測:")
    
    # 計算IQR方法檢測異常值
    quartiles = orders_df.select(
        percentile_approx("amount", 0.25).alias("q1"),
        percentile_approx("amount", 0.75).alias("q3")
    ).collect()[0]
    
    q1, q3 = quartiles["q1"], quartiles["q3"]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers = orders_df.filter(
        (col("amount") < lower_bound) | (col("amount") > upper_bound)
    ).join(customers_df, "customer_id", "inner")
    
    print(f"異常值範圍: < {lower_bound:.2f} 或 > {upper_bound:.2f}")
    print("檢測到的異常訂單:")
    outliers.select(
        "order_id", "customer_id", "name", "amount", "category", "status"
    ).orderBy(col("amount").desc()).show()
    
    # 9. 業務預測分析
    print("\n9. 業務預測分析:")
    
    # 9.1 客戶生命週期價值預測
    print("\n9.1 客戶生命週期價值分析:")
    
    # 基於歷史數據預測客戶價值
    clv_analysis = customer_enriched.withColumn(
        "avg_monthly_spend",
        col("total_spent") / 3  # 假設3個月數據
    ).withColumn(
        "predicted_annual_value",
        col("avg_monthly_spend") * 12
    ).withColumn(
        "customer_lifetime_months",
        when(col("tier") == "Premium", 36)
        .when(col("tier") == "Gold", 24)
        .when(col("tier") == "Silver", 18)
        .otherwise(12)
    ).withColumn(
        "predicted_clv",
        col("predicted_annual_value") * (col("customer_lifetime_months") / 12)
    ).withColumn(
        "clv_tier",
        when(col("predicted_clv") >= 10000, "High CLV")
        .when(col("predicted_clv") >= 5000, "Medium CLV")
        .otherwise("Low CLV")
    )
    
    print("客戶生命週期價值預測:")
    clv_analysis.select(
        "customer_id", "name", "tier", "total_spent", "predicted_annual_value", 
        "predicted_clv", "clv_tier"
    ).orderBy(col("predicted_clv").desc()).show()
    
    # 10. 綜合業務儀表板
    print("\n10. 綜合業務儀表板:")
    
    # 10.1 關鍵績效指標 (KPI)
    print("\n10.1 關鍵績效指標:")
    
    # 計算主要KPI
    total_orders = orders_df.count()
    completed_orders = orders_df.filter(col("status") == "completed").count()
    total_revenue = orders_df.filter(col("status") == "completed").agg(spark_sum("amount")).collect()[0][0]
    unique_customers = orders_df.select("customer_id").distinct().count()
    avg_order_value = total_revenue / completed_orders if completed_orders > 0 else 0
    order_completion_rate = completed_orders / total_orders * 100
    
    # 按渠道統計
    channel_summary = orders_df.filter(col("status") == "completed") \
        .groupBy("channel").agg(
            count("*").alias("orders"),
            spark_sum("amount").alias("revenue")
        ).collect()
    
    # 按分類統計
    category_summary = orders_df.filter(col("status") == "completed") \
        .groupBy("category").agg(
            count("*").alias("orders"),
            spark_sum("amount").alias("revenue")
        ).orderBy(col("revenue").desc()).collect()
    
    print("=== 業務儀表板 ===")
    print(f"總訂單數: {total_orders}")
    print(f"完成訂單數: {completed_orders}")
    print(f"總收入: ${total_revenue:,.2f}")
    print(f"唯一客戶數: {unique_customers}")
    print(f"平均訂單價值: ${avg_order_value:.2f}")
    print(f"訂單完成率: {order_completion_rate:.1f}%")
    
    print("\n渠道表現:")
    for row in channel_summary:
        print(f"- {row['channel']}: {row['orders']} 訂單, ${row['revenue']:,.2f} 收入")
    
    print("\n分類表現:")
    for row in category_summary:
        print(f"- {row['category']}: {row['orders']} 訂單, ${row['revenue']:,.2f} 收入")
    
    # 10.2 趨勢和洞察
    print("\n10.2 業務洞察:")
    
    insights = [
        f"最有價值客戶群體: {clv_analysis.filter(col('clv_tier') == 'High CLV').count()} 位高價值客戶",
        f"主要收入來源: {category_summary[0]['category']} 分類貢獻最多收入",
        f"渠道偏好: {'線上' if channel_summary[0]['channel'] == 'online' else '實體店'} 渠道表現更佳",
        f"客戶留存: {rfm_with_customers.filter(col('customer_type').isin(['Champions', 'Loyal Customers'])).count()} 位忠誠客戶",
        f"風險預警: {rfm_with_customers.filter(col('customer_type') == 'At Risk').count()} 位客戶面臨流失風險"
    ]
    
    print("關鍵業務洞察:")
    for i, insight in enumerate(insights, 1):
        print(f"{i}. {insight}")
    
    # 清理資源
    spark.stop()
    print("\n複雜數據分析和報表練習完成！")

if __name__ == "__main__":
    main()