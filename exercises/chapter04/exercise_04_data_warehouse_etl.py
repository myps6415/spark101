#!/usr/bin/env python3
"""
第4章練習4：數據倉庫操作和 ETL
企業級數據倉庫建設和ETL流程練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, isnull, lit, \
    count, sum as spark_sum, avg, max as spark_max, min as spark_min, \
    row_number, rank, dense_rank, first, last, \
    date_format, year, month, quarter, dayofmonth, \
    current_timestamp, date_add, date_sub, datediff, \
    concat, concat_ws, upper, trim, regexp_replace, \
    md5, sha1, monotonically_increasing_id, \
    broadcast, collect_list
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, \
    DateType, TimestampType, BooleanType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("數據倉庫操作和ETL練習") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("=== 第4章練習4：數據倉庫操作和 ETL ===")
    
    # 1. 創建源系統數據
    print("\n1. 創建源系統數據 (OLTP系統模擬):")
    
    # 1.1 客戶主檔 (Customer Master)
    customers_source = [
        (1, "Alice Johnson", "alice@email.com", "New York", "NY", "10001", "2023-01-15", "Active", "2024-01-01 10:00:00"),
        (2, "Bob Chen", "bob@email.com", "Los Angeles", "CA", "90001", "2022-06-20", "Active", "2024-01-01 10:00:00"),
        (3, "Charlie Wu", "charlie@email.com", "Chicago", "IL", "60601", "2023-03-10", "Active", "2024-01-01 10:00:00"),
        (4, "Diana Lin", "diana@email.com", "Houston", "TX", "77001", "2021-12-05", "Active", "2024-01-01 10:00:00"),
        (5, "Eve Brown", "eve@email.com", "Phoenix", "AZ", "85001", "2022-09-18", "Inactive", "2024-01-01 10:00:00")
    ]
    
    customers_schema = ["customer_id", "name", "email", "city", "state", "zip_code", "join_date", "status", "last_updated"]
    customers_df = spark.createDataFrame(customers_source, customers_schema)
    
    # 1.2 產品主檔 (Product Master)
    products_source = [
        (101, "Laptop Pro", "Electronics", "Computers", 1299.99, "Active", "2024-01-01 10:00:00"),
        (102, "Smartphone X", "Electronics", "Phones", 899.99, "Active", "2024-01-01 10:00:00"),
        (103, "Running Shoes", "Clothing", "Footwear", 129.99, "Active", "2024-01-01 10:00:00"),
        (104, "Coffee Maker", "Home", "Appliances", 199.99, "Active", "2024-01-01 10:00:00"),
        (105, "Textbook", "Books", "Education", 89.99, "Discontinued", "2024-01-01 10:00:00")
    ]
    
    products_schema = ["product_id", "product_name", "category", "subcategory", "price", "status", "last_updated"]
    products_df = spark.createDataFrame(products_source, products_schema)
    
    # 1.3 交易事實數據 (Transaction Facts)
    transactions_source = [
        (1001, 1, 101, "2024-01-15", 2, 1299.99, 2599.98, "Completed"),
        (1002, 2, 102, "2024-01-16", 1, 899.99, 899.99, "Completed"),
        (1003, 1, 103, "2024-01-17", 1, 129.99, 129.99, "Completed"),
        (1004, 3, 101, "2024-01-18", 1, 1299.99, 1299.99, "Completed"),
        (1005, 2, 104, "2024-01-19", 1, 199.99, 199.99, "Pending"),
        (1006, 4, 102, "2024-01-20", 2, 899.99, 1799.98, "Completed"),
        (1007, 1, 104, "2024-02-01", 1, 199.99, 199.99, "Completed"),
        (1008, 5, 103, "2024-02-02", 3, 129.99, 389.97, "Cancelled"),
        (1009, 3, 105, "2024-02-03", 2, 89.99, 179.98, "Completed"),
        (1010, 4, 101, "2024-02-04", 1, 1299.99, 1299.99, "Completed")
    ]
    
    transactions_schema = ["transaction_id", "customer_id", "product_id", "transaction_date", 
                          "quantity", "unit_price", "total_amount", "status"]
    transactions_df = spark.createDataFrame(transactions_source, transactions_schema)
    
    print("源系統數據已創建")
    print(f"客戶數: {customers_df.count()}")
    print(f"產品數: {products_df.count()}")
    print(f"交易數: {transactions_df.count()}")
    
    # 2. 數據質量檢查和清洗
    print("\n2. 數據質量檢查和清洗:")
    
    # 2.1 客戶數據質量檢查
    print("\n2.1 客戶數據質量檢查:")
    
    customer_quality = customers_df.agg(
        count("*").alias("total_records"),
        count(col("customer_id")).alias("valid_ids"),
        count(col("name")).alias("valid_names"),
        count(col("email")).alias("valid_emails"),
        spark_sum(when(col("email").contains("@"), 0).otherwise(1)).alias("invalid_emails"),
        spark_sum(when(col("zip_code").rlike("^[0-9]{5}$"), 0).otherwise(1)).alias("invalid_zip_codes")
    )
    
    print("客戶數據質量報告:")
    customer_quality.show()
    
    # 2.2 數據清洗和標準化
    print("\n2.2 數據清洗和標準化:")
    
    # 清洗客戶數據
    customers_cleaned = customers_df.withColumn(
        "name_clean", trim(upper(col("name")))
    ).withColumn(
        "email_clean", trim(lower(col("email")))
    ).withColumn(
        "city_clean", trim(upper(col("city")))
    ).withColumn(
        "state_clean", upper(col("state"))
    ).filter(
        col("email").contains("@")  # 過濾無效郵箱
    )
    
    # 清洗產品數據
    products_cleaned = products_df.withColumn(
        "category_clean", trim(upper(col("category")))
    ).withColumn(
        "subcategory_clean", trim(upper(col("subcategory")))
    ).filter(
        col("price") > 0  # 過濾無效價格
    )
    
    # 清洗交易數據
    transactions_cleaned = transactions_df.filter(
        (col("quantity") > 0) & (col("total_amount") > 0)
    ).withColumn(
        "transaction_date_clean", col("transaction_date").cast(DateType())
    )
    
    print("數據清洗完成")
    
    # 3. 維度表建設 (Dimension Tables)
    print("\n3. 維度表建設:")
    
    # 3.1 客戶維度表 (Customer Dimension)
    print("\n3.1 建設客戶維度表:")
    
    dim_customer = customers_cleaned.select(
        col("customer_id").alias("customer_key"),
        col("customer_id").alias("customer_id"),
        col("name_clean").alias("customer_name"),
        col("email_clean").alias("email"),
        col("city_clean").alias("city"),
        col("state_clean").alias("state"),
        col("zip_code"),
        col("join_date").cast(DateType()).alias("join_date"),
        col("status").alias("customer_status"),
        current_timestamp().alias("effective_date"),
        lit("9999-12-31").cast(DateType()).alias("expiry_date"),
        lit(True).alias("is_current"),
        md5(concat_ws("|", col("name_clean"), col("email_clean"), col("city_clean"))).alias("record_hash")
    )
    
    print("客戶維度表:")
    dim_customer.show()
    
    # 3.2 產品維度表 (Product Dimension)
    print("\n3.2 建設產品維度表:")
    
    dim_product = products_cleaned.select(
        col("product_id").alias("product_key"),
        col("product_id").alias("product_id"),
        col("product_name"),
        col("category_clean").alias("category"),
        col("subcategory_clean").alias("subcategory"),
        col("price"),
        col("status").alias("product_status"),
        current_timestamp().alias("effective_date"),
        lit("9999-12-31").cast(DateType()).alias("expiry_date"),
        lit(True).alias("is_current")
    )
    
    print("產品維度表:")
    dim_product.show()
    
    # 3.3 日期維度表 (Date Dimension)
    print("\n3.3 建設日期維度表:")
    
    # 生成日期維度表 (簡化版本)
    date_range = spark.sql("""
        SELECT sequence(
            to_date('2024-01-01'), 
            to_date('2024-12-31'), 
            interval 1 day
        ) as date_array
    """).select(explode(col("date_array")).alias("date_value"))
    
    dim_date = date_range.select(
        col("date_value").alias("date_key"),
        col("date_value").alias("full_date"),
        year(col("date_value")).alias("year"),
        quarter(col("date_value")).alias("quarter"),
        month(col("date_value")).alias("month"),
        dayofmonth(col("date_value")).alias("day"),
        date_format(col("date_value"), "EEEE").alias("day_name"),
        date_format(col("date_value"), "MMMM").alias("month_name"),
        when(month(col("date_value")).isin(1,2,3), "Q1")
        .when(month(col("date_value")).isin(4,5,6), "Q2")
        .when(month(col("date_value")).isin(7,8,9), "Q3")
        .otherwise("Q4").alias("quarter_name")
    )
    
    print("日期維度表 (前10天):")
    dim_date.limit(10).show()
    
    # 4. 事實表建設 (Fact Tables)
    print("\n4. 事實表建設:")
    
    # 4.1 銷售事實表 (Sales Fact Table)
    print("\n4.1 建設銷售事實表:")
    
    fact_sales = transactions_cleaned.join(
        dim_customer.select("customer_id", "customer_key"), "customer_id", "inner"
    ).join(
        dim_product.select("product_id", "product_key"), "product_id", "inner"
    ).join(
        dim_date.select("full_date", "date_key"), 
        transactions_cleaned.transaction_date_clean == dim_date.full_date, "inner"
    ).select(
        monotonically_increasing_id().alias("sales_fact_key"),
        col("transaction_id"),
        col("customer_key"),
        col("product_key"),
        col("date_key"),
        col("quantity"),
        col("unit_price"),
        col("total_amount"),
        col("status").alias("transaction_status"),
        current_timestamp().alias("created_date")
    )
    
    print("銷售事實表:")
    fact_sales.show()
    
    # 5. SCD (Slowly Changing Dimension) 實現
    print("\n5. 緩慢變化維度 (SCD Type 2) 實現:")
    
    # 5.1 模擬客戶信息變更
    print("\n5.1 模擬客戶信息變更:")
    
    # 模擬客戶搬家的情況
    customer_updates = [
        (1, "Alice Johnson", "alice@email.com", "Boston", "MA", "02101", "2023-01-15", "Active", "2024-02-01 10:00:00"),
        (6, "Frank Davis", "frank@email.com", "Seattle", "WA", "98101", "2024-02-01", "Active", "2024-02-01 10:00:00")  # 新客戶
    ]
    
    customers_updated_df = spark.createDataFrame(customer_updates, customers_schema)
    
    # 5.2 實現 SCD Type 2 邏輯
    print("\n5.2 實現 SCD Type 2 更新:")
    
    # 清洗更新數據
    customers_updated_cleaned = customers_updated_df.withColumn(
        "name_clean", trim(upper(col("name")))
    ).withColumn(
        "email_clean", trim(lower(col("email")))
    ).withColumn(
        "city_clean", trim(upper(col("city")))
    ).withColumn(
        "state_clean", upper(col("state"))
    ).withColumn(
        "record_hash", md5(concat_ws("|", col("name_clean"), col("email_clean"), col("city_clean")))
    )
    
    # 識別變更記錄
    existing_customers = dim_customer.filter(col("is_current") == True)
    
    # 找出需要更新的記錄
    changed_customers = customers_updated_cleaned.join(
        existing_customers.select("customer_id", "record_hash", "customer_key"),
        "customer_id", "inner"
    ).filter(
        customers_updated_cleaned.record_hash != existing_customers.record_hash
    )
    
    # 找出新客戶
    new_customers = customers_updated_cleaned.join(
        existing_customers.select("customer_id"), "customer_id", "leftanti"
    )
    
    print(f"變更客戶數: {changed_customers.count()}")
    print(f"新客戶數: {new_customers.count()}")
    
    # 5.3 更新維度表
    print("\n5.3 更新客戶維度表:")
    
    if changed_customers.count() > 0:
        # 過期舊記錄
        dim_customer_expired = dim_customer.join(
            changed_customers.select("customer_id"), "customer_id", "left"
        ).withColumn(
            "expiry_date",
            when(changed_customers.customer_id.isNotNull(), current_timestamp().cast(DateType()))
            .otherwise(col("expiry_date"))
        ).withColumn(
            "is_current",
            when(changed_customers.customer_id.isNotNull(), False)
            .otherwise(col("is_current"))
        )
        
        # 創建新記錄
        dim_customer_new_records = changed_customers.select(
            (col("customer_id") + 1000).alias("customer_key"),  # 新的代理鍵
            col("customer_id"),
            col("name_clean").alias("customer_name"),
            col("email_clean").alias("email"),
            col("city_clean").alias("city"),
            col("state_clean").alias("state"),
            col("zip_code"),
            col("join_date").cast(DateType()),
            col("status").alias("customer_status"),
            current_timestamp().alias("effective_date"),
            lit("9999-12-31").cast(DateType()).alias("expiry_date"),
            lit(True).alias("is_current"),
            col("record_hash")
        )
        
        # 合併維度表
        dim_customer_updated = dim_customer_expired.union(dim_customer_new_records)
        
        print("SCD Type 2 更新完成")
        print("更新後的客戶維度表:")
        dim_customer_updated.filter(col("customer_id") == 1).show()
    
    # 6. 數據聚合和OLAP立方體
    print("\n6. 數據聚合和OLAP立方體:")
    
    # 6.1 銷售匯總表
    print("\n6.1 建設銷售匯總表:")
    
    # 按年月匯總
    sales_monthly_summary = fact_sales.join(
        dim_date.select("date_key", "year", "month", "quarter"), "date_key", "inner"
    ).join(
        dim_product.select("product_key", "category"), "product_key", "inner"
    ).join(
        dim_customer.select("customer_key", "state"), "customer_key", "inner"
    ).groupBy("year", "month", "quarter", "category", "state").agg(
        count("*").alias("transaction_count"),
        spark_sum("quantity").alias("total_quantity"),
        spark_sum("total_amount").alias("total_sales"),
        avg("total_amount").alias("avg_transaction_value"),
        spark_max("total_amount").alias("max_transaction_value"),
        spark_min("total_amount").alias("min_transaction_value")
    )
    
    print("月度銷售匯總表:")
    sales_monthly_summary.orderBy("year", "month", "category").show()
    
    # 6.2 客戶分析立方體
    print("\n6.2 客戶分析立方體:")
    
    customer_cube = fact_sales.join(
        dim_customer.select("customer_key", "customer_name", "state", "customer_status"), 
        "customer_key", "inner"
    ).join(
        dim_product.select("product_key", "category"), "product_key", "inner"
    ).groupBy("customer_name", "state", "category", "customer_status").agg(
        count("*").alias("purchase_frequency"),
        spark_sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_order_value"),
        spark_sum("quantity").alias("total_items_purchased")
    ).withColumn(
        "customer_segment",
        when(col("total_spent") >= 2000, "High Value")
        .when(col("total_spent") >= 1000, "Medium Value")
        .otherwise("Low Value")
    )
    
    print("客戶分析立方體:")
    customer_cube.orderBy(col("total_spent").desc()).show()
    
    # 7. ETL 管道建設
    print("\n7. ETL 管道建設:")
    
    # 7.1 增量數據處理
    print("\n7.1 增量數據處理模擬:")
    
    # 模擬新的交易數據
    new_transactions = [
        (1011, 1, 102, "2024-02-05", 1, 899.99, 899.99, "Completed"),
        (1012, 2, 101, "2024-02-06", 1, 1299.99, 1299.99, "Pending"),
        (1013, 3, 104, "2024-02-07", 2, 199.99, 399.98, "Completed")
    ]
    
    new_transactions_df = spark.createDataFrame(new_transactions, transactions_schema)
    
    # 增量ETL處理
    incremental_fact = new_transactions_df.join(
        dim_customer.filter(col("is_current") == True).select("customer_id", "customer_key"), 
        "customer_id", "inner"
    ).join(
        dim_product.select("product_id", "product_key"), "product_id", "inner"
    ).join(
        dim_date.select("full_date", "date_key"), 
        new_transactions_df.transaction_date == dim_date.full_date, "inner"
    ).select(
        monotonically_increasing_id().alias("sales_fact_key"),
        col("transaction_id"),
        col("customer_key"),
        col("product_key"),
        col("date_key"),
        col("quantity"),
        col("unit_price"),
        col("total_amount"),
        col("status").alias("transaction_status"),
        current_timestamp().alias("created_date")
    )
    
    print("增量事實表數據:")
    incremental_fact.show()
    
    # 7.2 數據品質監控
    print("\n7.2 數據品質監控:")
    
    # 建立數據品質規則
    quality_checks = {
        "total_records": fact_sales.count(),
        "null_customer_keys": fact_sales.filter(col("customer_key").isNull()).count(),
        "null_product_keys": fact_sales.filter(col("product_key").isNull()).count(),
        "negative_amounts": fact_sales.filter(col("total_amount") < 0).count(),
        "zero_quantities": fact_sales.filter(col("quantity") <= 0).count(),
        "future_dates": fact_sales.join(
            dim_date.select("date_key", "full_date"), "date_key", "inner"
        ).filter(col("full_date") > current_timestamp().cast(DateType())).count()
    }
    
    print("數據品質檢查結果:")
    for check, result in quality_checks.items():
        status = "PASS" if result == 0 else "FAIL"
        print(f"- {check}: {result} ({status})")
    
    # 8. 數據血緣和元數據管理
    print("\n8. 數據血緣和元數據管理:")
    
    # 8.1 數據血緣跟踪
    data_lineage = {
        "source_systems": {
            "CRM": ["customers"],
            "PIM": ["products"], 
            "OMS": ["transactions"]
        },
        "staging_tables": {
            "customers_cleaned": "customers",
            "products_cleaned": "products",
            "transactions_cleaned": "transactions"
        },
        "dimension_tables": {
            "dim_customer": ["customers_cleaned"],
            "dim_product": ["products_cleaned"],
            "dim_date": ["generated"]
        },
        "fact_tables": {
            "fact_sales": ["transactions_cleaned", "dim_customer", "dim_product", "dim_date"]
        },
        "aggregation_tables": {
            "sales_monthly_summary": ["fact_sales", "dim_date", "dim_product", "dim_customer"],
            "customer_cube": ["fact_sales", "dim_customer", "dim_product"]
        }
    }
    
    print("數據血緣關係:")
    for layer, tables in data_lineage.items():
        print(f"\n{layer.upper()}:")
        for table, dependencies in tables.items():
            if isinstance(dependencies, list):
                print(f"  {table} <- {', '.join(dependencies)}")
            else:
                print(f"  {table} <- {dependencies}")
    
    # 9. 性能優化
    print("\n9. 性能優化策略:")
    
    # 9.1 分區策略
    print("\n9.1 表分區策略:")
    
    # 按年月分區的事實表
    partitioned_fact_sales = fact_sales.join(
        dim_date.select("date_key", "year", "month"), "date_key", "inner"
    ).withColumn("partition_key", concat(col("year"), col("month")))
    
    print("分區策略設計:")
    partition_summary = partitioned_fact_sales.groupBy("partition_key").agg(
        count("*").alias("record_count"),
        spark_sum("total_amount").alias("total_sales")
    ).orderBy("partition_key")
    
    partition_summary.show()
    
    # 9.2 索引建議
    print("\n9.2 索引建議:")
    
    index_recommendations = [
        "dim_customer: customer_id, customer_key, is_current",
        "dim_product: product_id, product_key, category",
        "dim_date: date_key, year, month, quarter",
        "fact_sales: customer_key, product_key, date_key",
        "fact_sales (composite): customer_key + date_key"
    ]
    
    print("建議索引:")
    for i, index in enumerate(index_recommendations, 1):
        print(f"{i}. {index}")
    
    # 10. 數據倉庫管理
    print("\n10. 數據倉庫管理和維護:")
    
    # 10.1 統計信息收集
    print("\n10.1 表統計信息:")
    
    table_stats = {
        "dim_customer": {
            "row_count": dim_customer.count(),
            "distinct_customers": dim_customer.select("customer_id").distinct().count(),
            "current_records": dim_customer.filter(col("is_current") == True).count()
        },
        "dim_product": {
            "row_count": dim_product.count(),
            "active_products": dim_product.filter(col("product_status") == "Active").count(),
            "categories": dim_product.select("category").distinct().count()
        },
        "fact_sales": {
            "row_count": fact_sales.count(),
            "total_sales": fact_sales.agg(spark_sum("total_amount")).collect()[0][0],
            "date_range": f"{fact_sales.join(dim_date, 'date_key').agg(spark_min('full_date'), spark_max('full_date')).collect()[0]}"
        }
    }
    
    print("表統計信息:")
    for table, stats in table_stats.items():
        print(f"\n{table.upper()}:")
        for metric, value in stats.items():
            print(f"  {metric}: {value}")
    
    # 10.2 數據倉庫監控
    print("\n10.2 數據倉庫監控指標:")
    
    monitoring_metrics = {
        "ETL成功率": "100%",
        "數據新鮮度": "< 1小時",
        "查詢平均響應時間": "< 5秒",
        "存儲使用率": "65%",
        "數據品質分數": "98%"
    }
    
    print("監控指標:")
    for metric, value in monitoring_metrics.items():
        print(f"- {metric}: {value}")
    
    # 總結報告
    print("\n=== 數據倉庫建設總結 ===")
    summary = {
        "維度表數量": 3,
        "事實表數量": 1,
        "匯總表數量": 2,
        "總數據量": f"{fact_sales.count() + dim_customer.count() + dim_product.count()} 筆記錄",
        "支持分析": "客戶分析、產品分析、銷售趨勢、財務報表",
        "數據更新頻率": "每日批次更新",
        "歷史數據保留": "3年",
        "備份策略": "每日增量，每週全量"
    }
    
    for key, value in summary.items():
        print(f"- {key}: {value}")
    
    # 清理資源
    spark.stop()
    print("\n數據倉庫操作和ETL練習完成！")

if __name__ == "__main__":
    main()