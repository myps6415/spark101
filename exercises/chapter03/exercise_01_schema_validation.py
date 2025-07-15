#!/usr/bin/env python3
"""
第3章練習1：自定義 Schema 和數據驗證
產品數據質量檢查練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, DateType
from pyspark.sql.functions import col, when, isnan, isnull, count, sum as spark_sum, avg, to_date

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Schema和數據驗證練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第3章練習1：Schema 和數據驗證 ===")
    
    # 定義產品數據的 Schema
    product_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("stock_quantity", IntegerType(), True),
        StructField("is_available", BooleanType(), True),
        StructField("launch_date", StringType(), True),  # 將在後面轉換為日期
        StructField("rating", DoubleType(), True)
    ])
    
    # 創建測試數據（包含一些問題）
    test_data = [
        ("P001", "Laptop", "Electronics", 1299.99, 50, True, "2024-01-01", 4.5),
        ("P002", "Mouse", "Electronics", 29.99, 200, True, "2024-01-15", 4.2),
        ("P003", None, "Books", 24.99, 100, True, "2024-02-01", 4.0),  # 缺失產品名
        ("P004", "Keyboard", "Electronics", -10.0, 75, True, "2024-01-20", 4.3),  # 負價格
        ("P005", "Monitor", "Electronics", 299.99, None, True, "2024-02-10", 4.7),  # 缺失庫存
        ("P006", "Book", "Books", 15.99, 50, True, "invalid-date", 6.0),  # 無效日期和評分
        ("P007", "", "Clothing", 49.99, 30, False, "2024-03-01", 3.8),  # 空字符串名稱
        ("P008", "Shoes", "Clothing", 89.99, 25, True, "2024-03-15", -1.0),  # 負評分
        ("P009", "Tablet", "Electronics", 599.99, 0, False, "2024-04-01", 4.1),
        ("P010", "Jacket", "Clothing", 129.99, 40, True, "2024-04-15", 4.6)
    ]
    
    products_df = spark.createDataFrame(test_data, product_schema)
    
    print("\n1. 原始數據:")
    products_df.show(truncate=False)
    
    print("\n2. Schema 信息:")
    products_df.printSchema()
    
    # 數據質量檢查
    print("\n=== 數據質量檢查 ===")
    
    # 3. 檢查空值
    print("\n3. 空值統計:")
    null_counts = products_df.select([
        count(when(col(c).isNull() | (col(c) == ""), c)).alias(f"{c}_null_count") 
        for c in products_df.columns
    ])
    null_counts.show()
    
    # 4. 數據範圍檢查
    print("\n4. 數據範圍問題:")
    
    # 價格檢查
    price_issues = products_df.filter(col("price") <= 0)
    print("價格異常記錄:")
    price_issues.select("product_id", "product_name", "price").show()
    
    # 評分檢查
    rating_issues = products_df.filter((col("rating") < 0) | (col("rating") > 5))
    print("評分異常記錄:")
    rating_issues.select("product_id", "product_name", "rating").show()
    
    # 5. 日期轉換和驗證
    print("\n5. 日期轉換:")
    products_with_date = products_df.withColumn(
        "launch_date_parsed", 
        to_date(col("launch_date"), "yyyy-MM-dd")
    )
    
    # 檢查日期轉換失敗的記錄
    date_issues = products_with_date.filter(col("launch_date_parsed").isNull())
    print("日期格式異常記錄:")
    date_issues.select("product_id", "launch_date").show()
    
    # 數據清洗
    print("\n=== 數據清洗 ===")
    
    # 6. 清洗數據
    cleaned_df = products_with_date \
        .fillna({"product_name": "Unknown Product"}) \
        .fillna({"stock_quantity": 0}) \
        .withColumn("price", when(col("price") <= 0, 0).otherwise(col("price"))) \
        .withColumn("rating", 
                   when(col("rating") < 0, 0)
                   .when(col("rating") > 5, 5)
                   .otherwise(col("rating"))) \
        .withColumn("product_name", 
                   when(col("product_name") == "", "Unknown Product")
                   .otherwise(col("product_name"))) \
        .drop("launch_date") \
        .withColumnRenamed("launch_date_parsed", "launch_date")
    
    print("\n6. 清洗後的數據:")
    cleaned_df.show(truncate=False)
    
    # 7. 數據質量報告
    print("\n=== 數據質量報告 ===")
    
    total_records = products_df.count()
    
    # 計算各種質量指標
    valid_names = cleaned_df.filter(
        col("product_name").isNotNull() & 
        (col("product_name") != "Unknown Product")
    ).count()
    
    valid_prices = cleaned_df.filter(col("price") > 0).count()
    valid_ratings = cleaned_df.filter(
        (col("rating") >= 0) & (col("rating") <= 5)
    ).count()
    valid_dates = cleaned_df.filter(col("launch_date").isNotNull()).count()
    
    print(f"\n7. 數據質量統計:")
    print(f"總記錄數: {total_records}")
    print(f"有效產品名稱: {valid_names}/{total_records} ({valid_names/total_records*100:.1f}%)")
    print(f"有效價格: {valid_prices}/{total_records} ({valid_prices/total_records*100:.1f}%)")
    print(f"有效評分: {valid_ratings}/{total_records} ({valid_ratings/total_records*100:.1f}%)")
    print(f"有效日期: {valid_dates}/{total_records} ({valid_dates/total_records*100:.1f}%)")
    
    # 8. 業務規則驗證
    print("\n8. 業務規則驗證:")
    
    # 檢查庫存為0但仍可用的產品
    availability_issues = cleaned_df.filter(
        (col("stock_quantity") == 0) & (col("is_available") == True)
    )
    print("庫存為0但標記為可用的產品:")
    availability_issues.select("product_id", "product_name", "stock_quantity", "is_available").show()
    
    # 檢查高價但低評分的產品
    price_rating_issues = cleaned_df.filter(
        (col("price") > 100) & (col("rating") < 3.0)
    )
    print("高價但低評分的產品:")
    price_rating_issues.select("product_id", "product_name", "price", "rating").show()
    
    # 9. 最終統計
    print("\n9. 最終數據統計:")
    final_stats = cleaned_df.groupBy("category").agg(
        count("*").alias("product_count"),
        avg("price").alias("avg_price"),
        avg("rating").alias("avg_rating"),
        spark_sum("stock_quantity").alias("total_stock")
    ).orderBy("category")
    
    final_stats.show()
    
    # 清理資源
    spark.stop()
    print("\n練習完成！")

if __name__ == "__main__":
    main()