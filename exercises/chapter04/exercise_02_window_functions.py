#!/usr/bin/env python3
"""
第4章練習2：窗口函數和分析函數
高級窗口函數和分析查詢練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, row_number, rank, dense_rank, lag, lead, \
    first, last, nth_value, percent_rank, cume_dist, ntile, \
    sum as spark_sum, avg, count, max as spark_max, min as spark_min, \
    stddev, var_pop, collect_list, concat_ws, \
    date_format, year, month, dayofmonth, datediff, add_months
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("窗口函數和分析函數練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第4章練習2：窗口函數和分析函數 ===")
    
    # 1. 創建銷售績效數據
    print("\n1. 創建銷售績效數據:")
    
    sales_performance_data = [
        ("EMP001", "Alice Johnson", "North", "Electronics", "2024-01", 125000, 15, 8500),
        ("EMP001", "Alice Johnson", "North", "Electronics", "2024-02", 135000, 18, 7500),
        ("EMP001", "Alice Johnson", "North", "Electronics", "2024-03", 145000, 20, 7250),
        ("EMP002", "Bob Chen", "South", "Clothing", "2024-01", 89000, 12, 7400),
        ("EMP002", "Bob Chen", "South", "Clothing", "2024-02", 95000, 14, 6800),
        ("EMP002", "Bob Chen", "South", "Clothing", "2024-03", 110000, 16, 6900),
        ("EMP003", "Charlie Wu", "East", "Electronics", "2024-01", 156000, 22, 7100),
        ("EMP003", "Charlie Wu", "East", "Electronics", "2024-02", 148000, 20, 7400),
        ("EMP003", "Charlie Wu", "East", "Electronics", "2024-03", 162000, 24, 6750),
        ("EMP004", "Diana Lin", "West", "Home", "2024-01", 78000, 10, 7800),
        ("EMP004", "Diana Lin", "West", "Home", "2024-02", 82000, 11, 7450),
        ("EMP004", "Diana Lin", "West", "Home", "2024-03", 88000, 13, 6770),
        ("EMP005", "Eve Brown", "North", "Clothing", "2024-01", 112000, 16, 7000),
        ("EMP005", "Eve Brown", "North", "Clothing", "2024-02", 118000, 17, 6940),
        ("EMP005", "Eve Brown", "North", "Clothing", "2024-03", 128000, 19, 6740),
        ("EMP006", "Frank Davis", "South", "Electronics", "2024-01", 134000, 19, 7050),
        ("EMP006", "Frank Davis", "South", "Electronics", "2024-02", 142000, 21, 6760),
        ("EMP006", "Frank Davis", "South", "Electronics", "2024-03", 151000, 23, 6570),
        ("EMP007", "Grace Wilson", "East", "Home", "2024-01", 91000, 13, 7000),
        ("EMP007", "Grace Wilson", "East", "Home", "2024-02", 97000, 15, 6470),
        ("EMP007", "Grace Wilson", "East", "Home", "2024-03", 105000, 17, 6180),
        ("EMP008", "Henry Taylor", "West", "Clothing", "2024-01", 103000, 14, 7360),
        ("EMP008", "Henry Taylor", "West", "Clothing", "2024-02", 108000, 15, 7200),
        ("EMP008", "Henry Taylor", "West", "Clothing", "2024-03", 115000, 17, 6760)
    ]
    
    sales_columns = ["emp_id", "emp_name", "region", "category", "month", "sales_amount", "deals_closed", "avg_deal_size"]
    sales_df = spark.createDataFrame(sales_performance_data, sales_columns)
    
    print("銷售績效數據:")
    sales_df.show()
    
    # 2. 排名函數
    print("\n2. 排名函數應用:")
    
    # 2.1 全局排名
    print("\n2.1 全局銷售排名:")
    
    # 按銷售額全局排名
    window_global = Window.orderBy(col("sales_amount").desc())
    
    sales_with_global_rank = sales_df.withColumn(
        "global_rank", row_number().over(window_global)
    ).withColumn(
        "global_dense_rank", dense_rank().over(window_global)
    ).withColumn(
        "global_percent_rank", percent_rank().over(window_global)
    )
    
    print("全局銷售排名 (前10名):")
    sales_with_global_rank.select(
        "emp_name", "month", "sales_amount", "global_rank", "global_dense_rank", "global_percent_rank"
    ).orderBy("global_rank").show(10)
    
    # 2.2 分組排名
    print("\n2.2 按地區和月份分組排名:")
    
    # 按地區和月份分組排名
    window_region_month = Window.partitionBy("region", "month").orderBy(col("sales_amount").desc())
    
    sales_with_region_rank = sales_df.withColumn(
        "region_month_rank", row_number().over(window_region_month)
    ).withColumn(
        "region_month_dense_rank", dense_rank().over(window_region_month)
    )
    
    print("地區月度排名:")
    sales_with_region_rank.select(
        "region", "month", "emp_name", "sales_amount", "region_month_rank"
    ).orderBy("region", "month", "region_month_rank").show()
    
    # 2.3 分位數排名
    print("\n2.3 銷售業績分位數:")
    
    # 使用 ntile 將銷售員分為4個等級
    window_ntile = Window.orderBy(col("sales_amount").desc())
    
    sales_with_ntile = sales_df.withColumn(
        "performance_quartile", ntile(4).over(window_ntile)
    ).withColumn(
        "performance_level",
        when(col("performance_quartile") == 1, "Top Performer")
        .when(col("performance_quartile") == 2, "High Performer")
        .when(col("performance_quartile") == 3, "Average Performer")
        .otherwise("Needs Improvement")
    )
    
    print("銷售員業績分位:")
    sales_with_ntile.select(
        "emp_name", "month", "sales_amount", "performance_quartile", "performance_level"
    ).orderBy("performance_quartile", col("sales_amount").desc()).show()
    
    # 3. 聚合窗口函數
    print("\n3. 聚合窗口函數:")
    
    # 3.1 累積聚合
    print("\n3.1 累積銷售額和移動平均:")
    
    # 按員工和時間順序的累積統計
    window_emp_time = Window.partitionBy("emp_id").orderBy("month")
    
    sales_with_cumulative = sales_df.withColumn(
        "cumulative_sales", spark_sum("sales_amount").over(window_emp_time)
    ).withColumn(
        "cumulative_deals", spark_sum("deals_closed").over(window_emp_time)
    ).withColumn(
        "running_avg_sales", avg("sales_amount").over(window_emp_time)
    ).withColumn(
        "running_max_sales", spark_max("sales_amount").over(window_emp_time)
    )
    
    print("員工累積績效:")
    sales_with_cumulative.select(
        "emp_name", "month", "sales_amount", "cumulative_sales", "running_avg_sales", "running_max_sales"
    ).orderBy("emp_id", "month").show()
    
    # 3.2 滑動窗口聚合
    print("\n3.2 滑動窗口統計:")
    
    # 3個月滑動窗口
    window_sliding = Window.partitionBy("emp_id").orderBy("month").rowsBetween(-1, 1)
    
    sales_with_sliding = sales_df.withColumn(
        "sliding_avg_sales", avg("sales_amount").over(window_sliding)
    ).withColumn(
        "sliding_total_deals", spark_sum("deals_closed").over(window_sliding)
    ).withColumn(
        "sliding_sales_stddev", stddev("sales_amount").over(window_sliding)
    )
    
    print("3個月滑動窗口統計:")
    sales_with_sliding.select(
        "emp_name", "month", "sales_amount", "sliding_avg_sales", "sliding_total_deals", "sliding_sales_stddev"
    ).orderBy("emp_id", "month").show()
    
    # 4. 行比較函數
    print("\n4. 行比較函數:")
    
    # 4.1 前後行比較
    print("\n4.1 月度增長分析:")
    
    # 比較前後月份的銷售額
    window_lag_lead = Window.partitionBy("emp_id").orderBy("month")
    
    sales_with_comparison = sales_df.withColumn(
        "prev_month_sales", lag("sales_amount", 1).over(window_lag_lead)
    ).withColumn(
        "next_month_sales", lead("sales_amount", 1).over(window_lag_lead)
    ).withColumn(
        "mom_growth", 
        ((col("sales_amount") - col("prev_month_sales")) / col("prev_month_sales") * 100)
    ).withColumn(
        "growth_trend",
        when(col("mom_growth") > 10, "Strong Growth")
        .when(col("mom_growth") > 0, "Positive Growth")
        .when(col("mom_growth") > -10, "Slight Decline")
        .otherwise("Significant Decline")
    )
    
    print("月度增長分析:")
    sales_with_comparison.select(
        "emp_name", "month", "sales_amount", "prev_month_sales", "mom_growth", "growth_trend"
    ).orderBy("emp_id", "month").show()
    
    # 4.2 首末值比較
    print("\n4.2 季度首末值比較:")
    
    # 獲取每個員工的首次和最後一次銷售記錄
    window_first_last = Window.partitionBy("emp_id").orderBy("month")
    
    sales_with_first_last = sales_df.withColumn(
        "first_month_sales", first("sales_amount").over(window_first_last)
    ).withColumn(
        "last_month_sales", last("sales_amount").over(window_first_last)
    ).withColumn(
        "quarter_improvement",
        col("last_month_sales") - col("first_month_sales")
    ).withColumn(
        "quarter_improvement_pct",
        (col("last_month_sales") - col("first_month_sales")) / col("first_month_sales") * 100
    )
    
    print("季度改善情況:")
    sales_with_first_last.select(
        "emp_name", "first_month_sales", "last_month_sales", "quarter_improvement", "quarter_improvement_pct"
    ).distinct().orderBy("quarter_improvement_pct").show()
    
    # 5. 複雜窗口分析
    print("\n5. 複雜窗口分析:")
    
    # 5.1 多維度排名分析
    print("\n5.1 多維度排名分析:")
    
    # 在地區內按不同指標排名
    window_region = Window.partitionBy("region").orderBy(col("sales_amount").desc())
    window_category = Window.partitionBy("category").orderBy(col("sales_amount").desc())
    
    multi_rank_analysis = sales_df.withColumn(
        "region_sales_rank", row_number().over(window_region)
    ).withColumn(
        "category_sales_rank", row_number().over(window_category)
    ).withColumn(
        "avg_deal_rank_in_region", 
        row_number().over(Window.partitionBy("region").orderBy(col("avg_deal_size").desc()))
    )
    
    print("多維度排名:")
    multi_rank_analysis.select(
        "emp_name", "region", "category", "sales_amount", 
        "region_sales_rank", "category_sales_rank", "avg_deal_rank_in_region"
    ).orderBy("region", "region_sales_rank").show()
    
    # 5.2 相對績效分析
    print("\n5.2 相對績效分析:")
    
    # 計算相對於地區平均的績效
    window_region_all = Window.partitionBy("region")
    
    relative_performance = sales_df.withColumn(
        "region_avg_sales", avg("sales_amount").over(window_region_all)
    ).withColumn(
        "region_max_sales", spark_max("sales_amount").over(window_region_all)
    ).withColumn(
        "region_min_sales", spark_min("sales_amount").over(window_region_all)
    ).withColumn(
        "sales_vs_region_avg", 
        (col("sales_amount") - col("region_avg_sales")) / col("region_avg_sales") * 100
    ).withColumn(
        "sales_percentile_in_region",
        percent_rank().over(Window.partitionBy("region").orderBy("sales_amount"))
    )
    
    print("相對績效分析:")
    relative_performance.select(
        "emp_name", "region", "sales_amount", "region_avg_sales", 
        "sales_vs_region_avg", "sales_percentile_in_region"
    ).orderBy("region", col("sales_vs_region_avg").desc()).show()
    
    # 6. 時間序列窗口分析
    print("\n6. 時間序列窗口分析:")
    
    # 6.1 趨勢分析
    print("\n6.1 銷售趨勢分析:")
    
    # 線性趨勢分析
    window_trend = Window.partitionBy("emp_id").orderBy("month")
    
    trend_analysis = sales_df.withColumn(
        "month_number", 
        when(col("month") == "2024-01", 1)
        .when(col("month") == "2024-02", 2)
        .otherwise(3)
    ).withColumn(
        "two_month_ago_sales", lag("sales_amount", 2).over(window_trend)
    ).withColumn(
        "one_month_ago_sales", lag("sales_amount", 1).over(window_trend)
    ).withColumn(
        "trend_direction",
        when(
            (col("sales_amount") > col("one_month_ago_sales")) & 
            (col("one_month_ago_sales") > col("two_month_ago_sales")), 
            "Consistent Up"
        ).when(
            (col("sales_amount") < col("one_month_ago_sales")) & 
            (col("one_month_ago_sales") < col("two_month_ago_sales")), 
            "Consistent Down"
        ).when(
            col("sales_amount") > col("one_month_ago_sales"), 
            "Up"
        ).when(
            col("sales_amount") < col("one_month_ago_sales"), 
            "Down"
        ).otherwise("Stable")
    )
    
    print("銷售趨勢分析:")
    trend_analysis.filter(col("month") == "2024-03").select(
        "emp_name", "two_month_ago_sales", "one_month_ago_sales", 
        "sales_amount", "trend_direction"
    ).orderBy("emp_name").show()
    
    # 7. 高級分析場景
    print("\n7. 高級分析場景:")
    
    # 7.1 Top N 分析
    print("\n7.1 各地區Top 2銷售員:")
    
    # 每個地區的前2名銷售員
    window_region_top = Window.partitionBy("region").orderBy(col("sales_amount").desc())
    
    top_performers = sales_df.withColumn(
        "region_rank", row_number().over(window_region_top)
    ).filter(col("region_rank") <= 2)
    
    print("各地區前2名:")
    top_performers.select(
        "region", "emp_name", "sales_amount", "region_rank"
    ).orderBy("region", "region_rank").show()
    
    # 7.2 績效分布分析
    print("\n7.2 績效分布分析:")
    
    # 計算各種分位數
    performance_distribution = sales_df.withColumn(
        "sales_quartile", ntile(4).over(Window.orderBy("sales_amount"))
    ).withColumn(
        "deals_quartile", ntile(4).over(Window.orderBy("deals_closed"))
    ).withColumn(
        "combined_score",
        col("sales_quartile") + col("deals_quartile")
    ).withColumn(
        "overall_performance",
        when(col("combined_score") >= 7, "Excellent")
        .when(col("combined_score") >= 5, "Good")
        .when(col("combined_score") >= 3, "Average")
        .otherwise("Below Average")
    )
    
    print("績效分布:")
    performance_distribution.groupBy("overall_performance").count().orderBy("count").show()
    
    # 8. 窗口函數組合應用
    print("\n8. 窗口函數組合應用:")
    
    # 綜合分析報告
    comprehensive_analysis = sales_df.withColumn(
        # 全局排名
        "global_sales_rank", row_number().over(Window.orderBy(col("sales_amount").desc()))
    ).withColumn(
        # 地區內排名
        "regional_rank", row_number().over(Window.partitionBy("region").orderBy(col("sales_amount").desc()))
    ).withColumn(
        # 月度增長
        "prev_month_sales", lag("sales_amount", 1).over(Window.partitionBy("emp_id").orderBy("month"))
    ).withColumn(
        "growth_rate", 
        ((col("sales_amount") - col("prev_month_sales")) / col("prev_month_sales") * 100)
    ).withColumn(
        # 累積銷售
        "ytd_sales", spark_sum("sales_amount").over(Window.partitionBy("emp_id").orderBy("month"))
    ).withColumn(
        # 移動平均
        "three_month_avg", avg("sales_amount").over(
            Window.partitionBy("emp_id").orderBy("month").rowsBetween(-1, 1)
        )
    ).withColumn(
        # 績效等級
        "performance_tier", ntile(3).over(Window.orderBy(col("sales_amount").desc()))
    )
    
    print("綜合分析報告 (2024-03數據):")
    comprehensive_analysis.filter(col("month") == "2024-03").select(
        "emp_name", "region", "sales_amount", "global_sales_rank", 
        "regional_rank", "growth_rate", "ytd_sales", "performance_tier"
    ).orderBy("global_sales_rank").show()
    
    # 9. SQL 窗口函數
    print("\n9. SQL 窗口函數查詢:")
    
    # 註冊為臨時視圖
    sales_df.createOrReplaceTempView("sales_performance")
    
    # 使用 SQL 窗口函數
    sql_window_analysis = spark.sql("""
        SELECT 
            emp_name,
            region,
            month,
            sales_amount,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY sales_amount DESC) as region_rank,
            PERCENT_RANK() OVER (ORDER BY sales_amount) as sales_percentile,
            LAG(sales_amount, 1) OVER (PARTITION BY emp_id ORDER BY month) as prev_sales,
            sales_amount - LAG(sales_amount, 1) OVER (PARTITION BY emp_id ORDER BY month) as sales_change,
            AVG(sales_amount) OVER (PARTITION BY region) as region_avg,
            NTILE(4) OVER (ORDER BY sales_amount DESC) as quartile
        FROM sales_performance
        ORDER BY region, region_rank
    """)
    
    print("SQL 窗口函數結果:")
    sql_window_analysis.show()
    
    # 10. 窗口函數性能優化
    print("\n10. 窗口函數最佳實踐:")
    
    best_practices = [
        "合理使用分區鍵減少數據shuffling",
        "避免在大數據集上使用無分區的窗口函數",
        "優先使用row_number()而非rank()如果不需要並列排名",
        "合併多個窗口操作使用相同的窗口定義",
        "注意窗口函數的內存使用，特別是collect_list",
        "使用適當的窗口框架(rows/range between)",
        "考慮數據的排序和分區來優化性能"
    ]
    
    print("窗口函數最佳實踐:")
    for i, practice in enumerate(best_practices, 1):
        print(f"{i}. {practice}")
    
    # 統計摘要
    print("\n=== 窗口函數分析摘要 ===")
    total_records = sales_df.count()
    unique_employees = sales_df.select("emp_id").distinct().count()
    unique_regions = sales_df.select("region").distinct().count()
    
    print(f"總記錄數: {total_records}")
    print(f"員工數: {unique_employees}")
    print(f"地區數: {unique_regions}")
    print(f"時間週期: 3個月")
    
    # 清理資源
    spark.stop()
    print("\n窗口函數和分析函數練習完成！")

if __name__ == "__main__":
    main()