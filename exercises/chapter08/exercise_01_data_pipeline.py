#!/usr/bin/env python3
"""
第8章練習1：端到端數據管道
構建完整的數據處理管道練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_extract, split, count, avg, sum as spark_sum, \
    date_format, from_unixtime, unix_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
import os
import time

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("數據管道練習") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 第8章練習1：端到端數據管道 ===")
    
    # 1. 數據提取 (Extract)
    print("\n1. 數據提取階段:")
    
    # 讀取銷售數據
    sales_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .json("../../datasets/sales_data.json")
    
    # 讀取員工數據
    employees_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("../../datasets/employees_large.csv")
    
    # 讀取服務器日誌
    logs_df = spark.read.text("../../datasets/server_logs.txt")
    
    print("原始數據統計:")
    print(f"銷售記錄: {sales_df.count()} 筆")
    print(f"員工記錄: {employees_df.count()} 筆")
    print(f"日誌記錄: {logs_df.count()} 筆")
    
    # 2. 數據轉換 (Transform) - 銷售數據清洗
    print("\n2. 數據轉換階段:")
    
    # 銷售數據清洗和標準化
    print("\n2.1 銷售數據清洗:")
    sales_cleaned = sales_df \
        .filter(col("price") > 0) \
        .filter(col("quantity") > 0) \
        .withColumn("total_amount", col("price") * col("quantity")) \
        .withColumn("order_month", date_format(col("order_date"), "yyyy-MM"))
    
    print("清洗後銷售數據:")
    sales_cleaned.show(5)
    
    # 員工數據轉換
    print("\n2.2 員工數據轉換:")
    employees_transformed = employees_df \
        .withColumn("salary_grade", 
                   when(col("salary") >= 60000, "High")
                   .when(col("salary") >= 50000, "Medium")
                   .otherwise("Low")) \
        .withColumn("experience_level",
                   when(col("years_experience") >= 8, "Senior")
                   .when(col("years_experience") >= 4, "Mid")
                   .otherwise("Junior"))
    
    print("轉換後員工數據:")
    employees_transformed.select("name", "salary", "salary_grade", "years_experience", "experience_level").show(5)
    
    # 日誌數據解析
    print("\n2.3 日誌數據解析:")
    
    # 解析日誌格式: timestamp level message
    logs_parsed = logs_df \
        .withColumn("timestamp", regexp_extract(col("value"), r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", 1)) \
        .withColumn("level", regexp_extract(col("value"), r"\s+(INFO|WARN|ERROR|DEBUG)\s+", 1)) \
        .withColumn("message", regexp_extract(col("value"), r"(INFO|WARN|ERROR|DEBUG)\s+(.+)$", 2)) \
        .filter(col("level") != "")
    
    print("解析後日誌數據:")
    logs_parsed.select("timestamp", "level", "message").show(5, truncate=False)
    
    # 3. 數據聚合和分析
    print("\n3. 數據聚合和分析:")
    
    # 銷售分析
    print("\n3.1 銷售分析:")
    sales_analysis = sales_cleaned.groupBy("order_month", "category").agg(
        count("*").alias("order_count"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value")
    ).orderBy("order_month", "total_revenue")
    
    print("月度銷售分析:")
    sales_analysis.show()
    
    # 員工薪資分析
    print("\n3.2 員工薪資分析:")
    salary_analysis = employees_transformed.groupBy("department", "salary_grade").agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        avg("performance_rating").alias("avg_performance")
    ).orderBy("department", "salary_grade")
    
    print("部門薪資分析:")
    salary_analysis.show()
    
    # 日誌等級統計
    print("\n3.3 日誌等級統計:")
    log_stats = logs_parsed.groupBy("level").count().orderBy("count", ascending=False)
    log_stats.show()
    
    # 4. 數據質量檢查
    print("\n4. 數據質量檢查:")
    
    # 檢查銷售數據質量
    print("\n4.1 銷售數據質量:")
    sales_quality = sales_cleaned.agg(
        count("*").alias("total_records"),
        spark_sum(when(col("price").isNull(), 1).otherwise(0)).alias("null_price"),
        spark_sum(when(col("quantity") < 0, 1).otherwise(0)).alias("negative_quantity"),
        spark_sum(when(col("total_amount") > 100000, 1).otherwise(0)).alias("high_value_orders")
    )
    sales_quality.show()
    
    # 檢查員工數據完整性
    print("\n4.2 員工數據完整性:")
    employee_quality = employees_transformed.agg(
        count("*").alias("total_employees"),
        spark_sum(when(col("salary").isNull(), 1).otherwise(0)).alias("null_salary"),
        spark_sum(when(col("performance_rating") < 1, 1).otherwise(0)).alias("invalid_rating")
    )
    employee_quality.show()
    
    # 5. 數據豐富化 (Enrichment)
    print("\n5. 數據豐富化:")
    
    # 創建產品維度表
    product_lookup = sales_cleaned.select("product_id", "product_name", "category").distinct()
    
    # 豐富化銷售數據
    enriched_sales = sales_cleaned \
        .join(product_lookup, ["product_id", "product_name", "category"]) \
        .withColumn("revenue_tier",
                   when(col("total_amount") >= 10000, "Tier 1")
                   .when(col("total_amount") >= 5000, "Tier 2")
                   .when(col("total_amount") >= 1000, "Tier 3")
                   .otherwise("Tier 4"))
    
    print("豐富化後的銷售數據:")
    enriched_sales.select("order_id", "customer_name", "total_amount", "revenue_tier").show(10)
    
    # 6. 數據載入 (Load)
    print("\n6. 數據載入階段:")
    
    # 創建輸出目錄
    output_base = "/tmp/spark_pipeline_output"
    
    # 載入清洗後的數據
    print("\n6.1 載入清洗後數據:")
    
    # 保存銷售分析結果
    sales_analysis.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_base}/sales_analysis")
    
    # 保存員工分析結果
    salary_analysis.coalesce(1).write.mode("overwrite") \
        .option("header", "true") \
        .csv(f"{output_base}/salary_analysis")
    
    # 保存豐富化數據為 Parquet
    enriched_sales.write.mode("overwrite") \
        .partitionBy("order_month") \
        .parquet(f"{output_base}/enriched_sales")
    
    print("數據已成功載入到:")
    print(f"- 銷售分析: {output_base}/sales_analysis")
    print(f"- 薪資分析: {output_base}/salary_analysis") 
    print(f"- 豐富化銷售數據: {output_base}/enriched_sales")
    
    # 7. 數據管道監控
    print("\n7. 數據管道監控:")
    
    # 管道執行統計
    pipeline_stats = {
        "input_sales_records": sales_df.count(),
        "cleaned_sales_records": sales_cleaned.count(),
        "enriched_sales_records": enriched_sales.count(),
        "analysis_months": sales_analysis.select("order_month").distinct().count(),
        "departments_analyzed": salary_analysis.select("department").distinct().count()
    }
    
    print("管道執行統計:")
    for metric, value in pipeline_stats.items():
        print(f"- {metric}: {value}")
    
    # 8. 數據血緣追蹤
    print("\n8. 數據血緣追蹤:")
    
    lineage = {
        "source_datasets": ["sales_data.json", "employees_large.csv", "server_logs.txt"],
        "transformations": ["cleaning", "parsing", "enrichment", "aggregation"],
        "output_datasets": ["sales_analysis", "salary_analysis", "enriched_sales"],
        "pipeline_timestamp": current_timestamp()
    }
    
    print("數據血緣信息:")
    for key, value in lineage.items():
        if key != "pipeline_timestamp":
            print(f"- {key}: {value}")
    
    # 9. 性能指標
    print("\n9. 管道性能指標:")
    
    # 模擬記錄執行時間
    execution_metrics = {
        "total_execution_time": "模擬 45 秒",
        "extract_time": "模擬 8 秒",
        "transform_time": "模擬 25 秒", 
        "load_time": "模擬 12 秒",
        "data_throughput": f"{sales_df.count() + employees_df.count()} records/minute"
    }
    
    print("執行性能:")
    for metric, value in execution_metrics.items():
        print(f"- {metric}: {value}")
    
    # 10. 數據管道總結
    print("\n10. 數據管道總結:")
    print("✅ 數據提取：成功讀取多種格式數據源")
    print("✅ 數據轉換：完成清洗、解析、標準化")
    print("✅ 數據聚合：生成業務分析報告")
    print("✅ 質量檢查：驗證數據完整性和準確性")
    print("✅ 數據豐富化：增強數據價值")
    print("✅ 數據載入：保存到多種格式和分區")
    print("✅ 監控追蹤：記錄執行指標和血緣")
    
    # 清理資源
    spark.stop()
    print("\n練習完成！")

if __name__ == "__main__":
    main()