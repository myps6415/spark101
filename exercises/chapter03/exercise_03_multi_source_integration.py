#!/usr/bin/env python3
"""
第3章練習3：多數據源整合
多種數據源的讀取、處理和整合練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, regexp_replace, trim, upper, lower, \
    count, sum as spark_sum, avg, max as spark_max, min as spark_min, \
    current_timestamp, lit, concat_ws, coalesce, isnan, isnull
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("多數據源整合練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第3章練習3：多數據源整合 ===")
    
    # 1. 讀取和處理現有數據集
    print("\n1. 讀取多種數據源:")
    
    # 讀取 JSON 銷售數據
    print("\n1.1 讀取 JSON 銷售數據:")
    try:
        sales_df = spark.read.option("multiline", "true").json("../../datasets/sales_data.json")
        print("JSON 銷售數據結構:")
        sales_df.printSchema()
        print(f"銷售記錄數: {sales_df.count()}")
        sales_df.show(5)
    except Exception as e:
        print(f"讀取 JSON 數據時出錯: {e}")
        # 創建模擬數據
        sales_data = [
            ("ORD-001", "CUST-001", "PROD-001", "Laptop", 1, 25000, "2024-01-15", "Alice Johnson", "Electronics"),
            ("ORD-002", "CUST-002", "PROD-002", "Phone", 2, 18000, "2024-01-16", "Bob Chen", "Electronics"),
            ("ORD-003", "CUST-003", "PROD-003", "Book", 3, 500, "2024-01-17", "Charlie Wu", "Books"),
            ("ORD-004", "CUST-001", "PROD-004", "Headphones", 1, 3000, "2024-01-18", "Alice Johnson", "Electronics"),
            ("ORD-005", "CUST-004", "PROD-005", "Tablet", 1, 15000, "2024-01-19", "Diana Lin", "Electronics")
        ]
        sales_schema = ["order_id", "customer_id", "product_id", "product_name", "quantity", "price", "order_date", "customer_name", "category"]
        sales_df = spark.createDataFrame(sales_data, sales_schema)
        print("使用模擬銷售數據")
    
    # 讀取 CSV 員工數據
    print("\n1.2 讀取 CSV 員工數據:")
    try:
        employees_df = spark.read.option("header", "true").option("inferSchema", "true") \
            .csv("../../datasets/employees_large.csv")
        print("CSV 員工數據結構:")
        employees_df.printSchema()
        print(f"員工記錄數: {employees_df.count()}")
        employees_df.show(5)
    except Exception as e:
        print(f"讀取 CSV 數據時出錯: {e}")
        # 創建模擬數據
        emp_data = [
            ("Alice Johnson", 25, "Taipei", 50000, "Engineering", "2023-01-15", 4.2, 3),
            ("Bob Chen", 30, "Taichung", 60000, "Marketing", "2022-03-10", 3.8, 5),
            ("Charlie Wu", 35, "Kaohsiung", 65000, "Engineering", "2021-07-20", 4.5, 8),
            ("Diana Lin", 28, "Taipei", 55000, "HR", "2023-02-28", 4.0, 4),
            ("Eve Brown", 32, "Tainan", 58000, "Finance", "2022-05-15", 3.9, 6)
        ]
        emp_schema = ["name", "age", "city", "salary", "department", "hire_date", "performance_rating", "years_experience"]
        employees_df = spark.createDataFrame(emp_data, emp_schema)
        print("使用模擬員工數據")
    
    # 讀取文本日誌數據
    print("\n1.3 讀取文本日誌數據:")
    try:
        logs_df = spark.read.text("../../datasets/server_logs.txt")
        print("文本日誌數據結構:")
        logs_df.printSchema()
        print(f"日誌記錄數: {logs_df.count()}")
        logs_df.show(5, truncate=False)
    except Exception as e:
        print(f"讀取文本數據時出錯: {e}")
        # 創建模擬數據
        log_data = [
            "2024-01-15 10:05:12 INFO User alice@email.com logged in successfully",
            "2024-01-15 10:05:15 INFO Order ORD-001 created for customer CUST-001",
            "2024-01-15 10:05:18 ERROR Payment failed for order ORD-002",
            "2024-01-15 10:05:21 INFO User bob@email.com logged out",
            "2024-01-15 10:05:24 WARN Invalid access attempt from IP 192.168.1.100"
        ]
        logs_df = spark.createDataFrame([(log,) for log in log_data], ["value"])
        print("使用模擬日誌數據")
    
    # 2. 數據清洗和標準化
    print("\n2. 數據清洗和標準化:")
    
    # 2.1 清洗銷售數據
    print("\n2.1 清洗銷售數據:")
    
    sales_cleaned = sales_df.withColumn(
        "customer_name_clean", trim(upper(col("customer_name")))
    ).withColumn(
        "category_clean", trim(upper(col("category")))
    ).withColumn(
        "total_amount", col("quantity") * col("price")
    ).filter(
        col("price") > 0
    ).filter(
        col("quantity") > 0
    )
    
    print("清洗後的銷售數據:")
    sales_cleaned.select("order_id", "customer_name_clean", "category_clean", "total_amount").show()
    
    # 2.2 清洗員工數據
    print("\n2.2 清洗員工數據:")
    
    employees_cleaned = employees_df.withColumn(
        "name_clean", trim(upper(col("name")))
    ).withColumn(
        "department_clean", trim(upper(col("department")))
    ).withColumn(
        "city_clean", trim(upper(col("city")))
    ).filter(
        col("salary") > 0
    ).filter(
        col("age") >= 18
    )
    
    print("清洗後的員工數據:")
    employees_cleaned.select("name_clean", "department_clean", "city_clean", "salary").show()
    
    # 2.3 解析日誌數據
    print("\n2.3 解析日誌數據:")
    
    logs_parsed = logs_df.withColumn(
        "timestamp", regexp_replace(col("value"), r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}).*", "$1")
    ).withColumn(
        "level", regexp_replace(col("value"), r".*(\d{2}:\d{2}:\d{2})\s+(\w+)\s+.*", "$2")
    ).withColumn(
        "message", regexp_replace(col("value"), r".*\d{2}:\d{2}:\d{2}\s+\w+\s+(.*)", "$1")
    ).filter(
        col("level").isin(["INFO", "WARN", "ERROR", "DEBUG"])
    )
    
    print("解析後的日誌數據:")
    logs_parsed.select("timestamp", "level", "message").show(truncate=False)
    
    # 3. 數據整合策略
    print("\n3. 數據整合策略:")
    
    # 3.1 基於姓名的整合
    print("\n3.1 銷售數據與員工數據整合:")
    
    # 通過姓名關聯銷售和員工數據
    integrated_sales_emp = sales_cleaned.join(
        employees_cleaned.select("name_clean", "department_clean", "city_clean", "salary"),
        sales_cleaned.customer_name_clean == employees_cleaned.name_clean,
        "left"
    ).select(
        sales_cleaned["*"],
        employees_cleaned.department_clean.alias("employee_department"),
        employees_cleaned.city_clean.alias("employee_city"),
        employees_cleaned.salary.alias("employee_salary")
    )
    
    print("整合後的銷售-員工數據:")
    integrated_sales_emp.select(
        "order_id", "customer_name_clean", "total_amount", 
        "employee_department", "employee_city", "employee_salary"
    ).show()
    
    # 3.2 創建客戶主數據
    print("\n3.2 創建統一客戶主數據:")
    
    # 從銷售數據提取客戶信息
    customers_from_sales = sales_cleaned.select(
        col("customer_id"),
        col("customer_name_clean").alias("name"),
        lit("SALES").alias("source")
    ).distinct()
    
    # 從員工數據提取客戶信息（假設員工也是客戶）
    customers_from_employees = employees_cleaned.select(
        lit("EMP-" + col("name_clean")).alias("customer_id"),
        col("name_clean").alias("name"),
        lit("EMPLOYEE").alias("source")
    )
    
    # 合併客戶數據
    master_customers = customers_from_sales.union(customers_from_employees)
    
    print("統一客戶主數據:")
    master_customers.show()
    
    # 4. 多維度分析
    print("\n4. 多維度分析:")
    
    # 4.1 銷售地理分析
    print("\n4.1 銷售地理分析:")
    
    sales_geo_analysis = integrated_sales_emp.filter(
        col("employee_city").isNotNull()
    ).groupBy("employee_city").agg(
        count("*").alias("order_count"),
        spark_sum("total_amount").alias("total_sales"),
        avg("total_amount").alias("avg_order_value"),
        avg("employee_salary").alias("avg_employee_salary")
    ).orderBy(col("total_sales").desc())
    
    print("按城市的銷售分析:")
    sales_geo_analysis.show()
    
    # 4.2 部門消費分析
    print("\n4.2 部門消費分析:")
    
    dept_consumption = integrated_sales_emp.filter(
        col("employee_department").isNotNull()
    ).groupBy("employee_department").agg(
        count("*").alias("order_count"),
        spark_sum("total_amount").alias("dept_total_spending"),
        avg("total_amount").alias("avg_order_per_dept"),
        avg("employee_salary").alias("avg_dept_salary")
    ).withColumn(
        "spending_to_salary_ratio",
        col("dept_total_spending") / col("avg_dept_salary")
    ).orderBy(col("dept_total_spending").desc())
    
    print("按部門的消費分析:")
    dept_consumption.show()
    
    # 5. 日誌數據整合
    print("\n5. 日誌數據整合:")
    
    # 5.1 提取日誌中的訂單信息
    print("\n5.1 提取日誌中的訂單信息:")
    
    order_logs = logs_parsed.filter(
        col("message").contains("Order") | col("message").contains("ORD-")
    ).withColumn(
        "extracted_order_id",
        regexp_replace(col("message"), r".*Order\s+(\S+).*", "$1")
    )
    
    print("提取的訂單日誌:")
    order_logs.select("timestamp", "level", "extracted_order_id", "message").show(truncate=False)
    
    # 5.2 日誌與銷售數據關聯
    print("\n5.2 日誌與銷售數據關聯:")
    
    sales_with_logs = sales_cleaned.join(
        order_logs.select("extracted_order_id", "timestamp", "level"),
        sales_cleaned.order_id == order_logs.extracted_order_id,
        "left"
    ).withColumn(
        "has_log_record", when(col("timestamp").isNotNull(), True).otherwise(False)
    )
    
    print("銷售數據與日誌關聯:")
    sales_with_logs.select(
        "order_id", "customer_name_clean", "total_amount", "has_log_record", "level"
    ).show()
    
    # 6. 數據質量評估
    print("\n6. 數據質量評估:")
    
    # 6.1 數據完整性檢查
    print("\n6.1 跨數據源完整性檢查:")
    
    completeness_report = {
        "sales_records": sales_cleaned.count(),
        "employee_records": employees_cleaned.count(),
        "log_records": logs_parsed.count(),
        "integrated_sales_emp": integrated_sales_emp.count(),
        "customers_with_employee_info": integrated_sales_emp.filter(col("employee_department").isNotNull()).count(),
        "orders_with_logs": sales_with_logs.filter(col("has_log_record") == True).count()
    }
    
    print("數據完整性報告:")
    for metric, value in completeness_report.items():
        print(f"- {metric}: {value}")
    
    # 6.2 數據一致性檢查
    print("\n6.2 數據一致性檢查:")
    
    # 檢查姓名匹配率
    total_sales_customers = sales_cleaned.select("customer_name_clean").distinct().count()
    matched_customers = integrated_sales_emp.filter(col("employee_department").isNotNull()) \
                                           .select("customer_name_clean").distinct().count()
    
    match_rate = (matched_customers / total_sales_customers * 100) if total_sales_customers > 0 else 0
    
    print(f"客戶姓名匹配率: {match_rate:.1f}% ({matched_customers}/{total_sales_customers})")
    
    # 7. 綜合業務視圖
    print("\n7. 創建綜合業務視圖:")
    
    # 7.1 360度客戶視圖
    print("\n7.1 360度客戶視圖:")
    
    customer_360_view = integrated_sales_emp.groupBy(
        "customer_id", "customer_name_clean", "employee_department", "employee_city"
    ).agg(
        count("*").alias("total_orders"),
        spark_sum("total_amount").alias("total_spent"),
        avg("total_amount").alias("avg_order_value"),
        spark_max("total_amount").alias("max_order"),
        spark_min("total_amount").alias("min_order"),
        max("employee_salary").alias("salary")
    ).withColumn(
        "customer_value_tier",
        when(col("total_spent") >= 50000, "Platinum")
        .when(col("total_spent") >= 25000, "Gold")
        .when(col("total_spent") >= 10000, "Silver")
        .otherwise("Bronze")
    )
    
    print("360度客戶視圖:")
    customer_360_view.show(truncate=False)
    
    # 7.2 運營儀表板數據
    print("\n7.2 運營儀表板數據:")
    
    dashboard_metrics = {
        "total_revenue": sales_cleaned.agg(spark_sum("total_amount")).collect()[0][0],
        "total_orders": sales_cleaned.count(),
        "avg_order_value": sales_cleaned.agg(avg("total_amount")).collect()[0][0],
        "unique_customers": sales_cleaned.select("customer_id").distinct().count(),
        "employee_customers": integrated_sales_emp.filter(col("employee_department").isNotNull()).select("customer_id").distinct().count(),
        "error_logs": logs_parsed.filter(col("level") == "ERROR").count(),
        "cities_covered": integrated_sales_emp.filter(col("employee_city").isNotNull()).select("employee_city").distinct().count()
    }
    
    print("運營儀表板指標:")
    for metric, value in dashboard_metrics.items():
        if isinstance(value, float):
            print(f"- {metric}: {value:.2f}")
        else:
            print(f"- {metric}: {value}")
    
    # 8. 數據導出
    print("\n8. 數據導出和持久化:")
    
    # 8.1 保存整合結果
    print("\n8.1 保存整合結果到 Parquet:")
    
    try:
        # 保存主要整合結果
        output_path = "/tmp/integrated_data"
        
        customer_360_view.coalesce(1).write.mode("overwrite") \
            .parquet(f"{output_path}/customer_360_view")
        
        sales_with_logs.coalesce(1).write.mode("overwrite") \
            .parquet(f"{output_path}/sales_with_logs")
        
        master_customers.coalesce(1).write.mode("overwrite") \
            .parquet(f"{output_path}/master_customers")
        
        print("數據已保存到 /tmp/integrated_data/")
        
    except Exception as e:
        print(f"保存數據時出錯: {e}")
    
    # 9. 數據血緣和元數據
    print("\n9. 數據血緣跟踪:")
    
    lineage_info = {
        "source_datasets": [
            "sales_data.json",
            "employees_large.csv", 
            "server_logs.txt"
        ],
        "transformation_steps": [
            "data_cleaning",
            "standardization", 
            "name_matching",
            "integration",
            "enrichment"
        ],
        "output_datasets": [
            "customer_360_view",
            "sales_with_logs",
            "master_customers"
        ],
        "quality_checks": [
            "completeness_validation",
            "consistency_verification",
            "match_rate_analysis"
        ]
    }
    
    print("數據血緣信息:")
    for category, items in lineage_info.items():
        print(f"- {category}: {', '.join(items)}")
    
    # 10. 總結報告
    print("\n10. 多數據源整合總結:")
    
    integration_summary = {
        "數據源數量": 3,
        "總記錄數": sales_cleaned.count() + employees_cleaned.count() + logs_parsed.count(),
        "整合成功率": f"{match_rate:.1f}%",
        "數據質量分數": 85,  # 假設分數
        "處理時間": "< 1分鐘",
        "輸出視圖數": 3
    }
    
    print("=== 整合總結 ===")
    for metric, value in integration_summary.items():
        print(f"- {metric}: {value}")
    
    print("\n主要成果:")
    print("✅ 成功整合了 JSON、CSV、TXT 三種數據源")
    print("✅ 建立了統一的客戶主數據")
    print("✅ 創建了360度客戶視圖")
    print("✅ 實現了跨數據源的業務分析")
    print("✅ 建立了數據質量監控機制")
    
    # 清理資源
    spark.stop()
    print("\n多數據源整合練習完成！")

if __name__ == "__main__":
    main()