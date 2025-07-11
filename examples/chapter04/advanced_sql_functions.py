#!/usr/bin/env python3
"""
第4章：Spark SQL - 進階函數和功能
學習 Spark SQL 的進階函數、自訂函數和性能調優
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, MapType
from pyspark.sql.functions import udf, col, explode, collect_list, struct, map_values, map_keys
import json
from datetime import datetime

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Advanced SQL Functions") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    print("🚀 Spark SQL 進階函數示範")
    print("=" * 40)
    
    # 1. 準備複雜數據結構
    print("\n1️⃣ 複雜數據結構")
    
    # 包含嵌套結構的數據
    complex_data = [
        {
            "id": 1,
            "name": "Alice",
            "skills": ["Python", "Spark", "SQL"],
            "projects": [
                {"name": "Project A", "status": "completed", "budget": 50000},
                {"name": "Project B", "status": "in_progress", "budget": 75000}
            ],
            "metadata": {"level": "Senior", "location": "NYC", "remote": True}
        },
        {
            "id": 2,
            "name": "Bob",
            "skills": ["Java", "Scala", "Kafka"],
            "projects": [
                {"name": "Project C", "status": "completed", "budget": 60000},
                {"name": "Project D", "status": "planning", "budget": 80000}
            ],
            "metadata": {"level": "Lead", "location": "SF", "remote": False}
        },
        {
            "id": 3,
            "name": "Charlie",
            "skills": ["JavaScript", "React", "Node.js"],
            "projects": [
                {"name": "Project E", "status": "in_progress", "budget": 45000}
            ],
            "metadata": {"level": "Junior", "location": "LA", "remote": True}
        }
    ]
    
    # 從JSON創建DataFrame
    df = spark.read.json(spark.sparkContext.parallelize([json.dumps(record) for record in complex_data]))
    
    print("複雜數據結構:")
    df.show(truncate=False)
    df.printSchema()
    
    # 創建臨時視圖
    df.createOrReplaceTempView("employees")
    
    # 2. 陣列函數
    print("\n2️⃣ 陣列函數")
    
    # 陣列基本操作
    result = spark.sql("""
        SELECT name,
               skills,
               SIZE(skills) as skill_count,
               ARRAY_CONTAINS(skills, 'Python') as knows_python,
               skills[0] as first_skill
        FROM employees
    """)
    print("陣列基本操作:")
    result.show(truncate=False)
    
    # 陣列展開
    result = spark.sql("""
        SELECT name, explode(skills) as skill
        FROM employees
    """)
    print("陣列展開:")
    result.show()
    
    # 陣列聚合
    result = spark.sql("""
        SELECT explode(skills) as skill,
               COUNT(*) as employee_count
        FROM employees
        GROUP BY skill
        ORDER BY employee_count DESC
    """)
    print("技能統計:")
    result.show()
    
    # 3. 結構體函數
    print("\n3️⃣ 結構體函數")
    
    # 存取結構體欄位
    result = spark.sql("""
        SELECT name,
               metadata.level as level,
               metadata.location as location,
               metadata.remote as is_remote
        FROM employees
    """)
    print("結構體欄位存取:")
    result.show()
    
    # 展開結構體
    result = spark.sql("""
        SELECT name, explode(projects) as project
        FROM employees
    """)
    print("展開項目結構體:")
    result.show(truncate=False)
    
    # 存取嵌套結構
    result = spark.sql("""
        SELECT name,
               project.name as project_name,
               project.status as project_status,
               project.budget as project_budget
        FROM (
            SELECT name, explode(projects) as project
            FROM employees
        )
    """)
    print("項目詳情:")
    result.show()
    
    # 4. 自訂函數 (UDF)
    print("\n4️⃣ 自訂函數 (UDF)")
    
    # 定義Python UDF
    def calculate_skill_score(skills):
        """根據技能計算分數"""
        score_map = {
            "Python": 10, "Spark": 15, "SQL": 8,
            "Java": 12, "Scala": 14, "Kafka": 10,
            "JavaScript": 8, "React": 7, "Node.js": 6
        }
        return sum(score_map.get(skill, 5) for skill in skills)
    
    def format_employee_info(name, level, location):
        """格式化員工信息"""
        return f"{name} ({level}) - {location}"
    
    # 註冊UDF
    spark.udf.register("calculate_skill_score", calculate_skill_score, IntegerType())
    spark.udf.register("format_employee_info", format_employee_info, StringType())
    
    # 使用UDF
    result = spark.sql("""
        SELECT name,
               skills,
               calculate_skill_score(skills) as skill_score,
               format_employee_info(name, metadata.level, metadata.location) as employee_info
        FROM employees
    """)
    print("使用自訂函數:")
    result.show(truncate=False)
    
    # 5. 視窗函數進階應用
    print("\n5️⃣ 視窗函數進階應用")
    
    # 創建更多範例數據
    sales_data = [
        ("Alice", "Q1", 2024, 15000),
        ("Alice", "Q2", 2024, 18000),
        ("Alice", "Q3", 2024, 22000),
        ("Alice", "Q4", 2024, 25000),
        ("Bob", "Q1", 2024, 12000),
        ("Bob", "Q2", 2024, 16000),
        ("Bob", "Q3", 2024, 19000),
        ("Bob", "Q4", 2024, 21000),
        ("Charlie", "Q1", 2024, 10000),
        ("Charlie", "Q2", 2024, 13000),
        ("Charlie", "Q3", 2024, 15000),
        ("Charlie", "Q4", 2024, 18000)
    ]
    
    sales_df = spark.createDataFrame(sales_data, ["name", "quarter", "year", "sales"])
    sales_df.createOrReplaceTempView("sales")
    
    print("銷售數據:")
    sales_df.show()
    
    # 移動平均
    result = spark.sql("""
        SELECT name, quarter, sales,
               AVG(sales) OVER (
                   PARTITION BY name 
                   ORDER BY quarter 
                   ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
               ) as moving_avg,
               sales - LAG(sales, 1) OVER (
                   PARTITION BY name 
                   ORDER BY quarter
               ) as quarter_growth
        FROM sales
    """)
    print("移動平均和季度增長:")
    result.show()
    
    # 累計總計和百分比
    result = spark.sql("""
        SELECT name, quarter, sales,
               SUM(sales) OVER (
                   PARTITION BY name 
                   ORDER BY quarter 
                   ROWS UNBOUNDED PRECEDING
               ) as cumulative_sales,
               ROUND(
                   sales * 100.0 / SUM(sales) OVER (PARTITION BY name),
                   2
               ) as percentage_of_total
        FROM sales
    """)
    print("累計銷售和百分比:")
    result.show()
    
    # 6. 正則表達式和字串處理
    print("\n6️⃣ 正則表達式和字串處理")
    
    # 創建包含文本數據的樣本
    text_data = [
        ("user001", "Alice Johnson", "alice.johnson@company.com", "Phone: +1-555-0123"),
        ("user002", "Bob Smith Jr.", "bob.smith@company.com", "Phone: +1-555-0456"),
        ("user003", "Charlie Brown", "charlie.brown@company.com", "Phone: +1-555-0789")
    ]
    
    text_df = spark.createDataFrame(text_data, ["user_id", "full_name", "email", "contact"])
    text_df.createOrReplaceTempView("users")
    
    # 正則表達式提取
    result = spark.sql("""
        SELECT user_id,
               full_name,
               email,
               REGEXP_EXTRACT(full_name, '^([A-Za-z]+)', 1) as first_name,
               REGEXP_EXTRACT(full_name, '([A-Za-z]+)$', 1) as last_name,
               REGEXP_EXTRACT(email, '([^@]+)@', 1) as username,
               REGEXP_EXTRACT(contact, '\\+(\\d+-\\d+-\\d+)', 1) as phone_number
        FROM users
    """)
    print("正則表達式提取:")
    result.show()
    
    # 字串替換和格式化
    result = spark.sql("""
        SELECT user_id,
               UPPER(REGEXP_REPLACE(full_name, '\\s+', '_')) as username_format,
               REGEXP_REPLACE(email, '@.*', '@newdomain.com') as new_email,
               SPLIT(full_name, ' ')[0] as first_name_split
        FROM users
    """)
    print("字串替換和格式化:")
    result.show()
    
    # 7. 條件邏輯和CASE表達式
    print("\n7️⃣ 條件邏輯和CASE表達式")
    
    # 複雜條件分類
    result = spark.sql("""
        SELECT name,
               SIZE(skills) as skill_count,
               calculate_skill_score(skills) as skill_score,
               metadata.level as current_level,
               CASE
                   WHEN calculate_skill_score(skills) >= 40 THEN 'Expert'
                   WHEN calculate_skill_score(skills) >= 25 THEN 'Advanced'
                   WHEN calculate_skill_score(skills) >= 15 THEN 'Intermediate'
                   ELSE 'Beginner'
               END as skill_level_assessment,
               CASE
                   WHEN metadata.remote = true AND SIZE(skills) > 2 THEN 'Remote Expert'
                   WHEN metadata.remote = true THEN 'Remote Worker'
                   WHEN SIZE(skills) > 2 THEN 'Office Expert'
                   ELSE 'Office Worker'
               END as work_classification
        FROM employees
    """)
    print("複雜條件分類:")
    result.show()
    
    # 8. 日期時間函數
    print("\n8️⃣ 日期時間函數")
    
    # 添加當前時間戳
    result = spark.sql("""
        SELECT name,
               CURRENT_TIMESTAMP() as current_time,
               CURRENT_DATE() as current_date,
               DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyy-MM-dd HH:mm:ss') as formatted_time,
               UNIX_TIMESTAMP() as unix_timestamp,
               FROM_UNIXTIME(UNIX_TIMESTAMP(), 'yyyy-MM-dd') as date_from_unix
        FROM employees
    """)
    print("日期時間函數:")
    result.show()
    
    # 9. 數據透視和逆透視
    print("\n9️⃣ 數據透視")
    
    # 透視銷售數據
    result = spark.sql("""
        SELECT name,
               SUM(CASE WHEN quarter = 'Q1' THEN sales END) as Q1_sales,
               SUM(CASE WHEN quarter = 'Q2' THEN sales END) as Q2_sales,
               SUM(CASE WHEN quarter = 'Q3' THEN sales END) as Q3_sales,
               SUM(CASE WHEN quarter = 'Q4' THEN sales END) as Q4_sales,
               SUM(sales) as total_sales
        FROM sales
        GROUP BY name
    """)
    print("銷售數據透視:")
    result.show()
    
    # 10. 性能調優相關查詢
    print("\n🔟 性能調優相關查詢")
    
    # 查詢執行計劃
    result = spark.sql("SELECT * FROM employees WHERE SIZE(skills) > 2")
    print("查詢執行計劃:")
    result.explain(True)
    
    # 快取表格
    spark.sql("CACHE TABLE employees")
    print("表格已快取")
    
    # 查看快取狀態
    cached_tables = spark.sql("SHOW TABLES").collect()
    print(f"快取的表格數量: {len(cached_tables)}")
    
    # 11. 統計函數
    print("\n1️⃣1️⃣ 統計函數")
    
    # 統計分析
    result = spark.sql("""
        SELECT 
            COUNT(*) as total_employees,
            AVG(SIZE(skills)) as avg_skills,
            STDDEV(SIZE(skills)) as skills_stddev,
            MIN(SIZE(skills)) as min_skills,
            MAX(SIZE(skills)) as max_skills,
            PERCENTILE_APPROX(SIZE(skills), 0.5) as median_skills,
            PERCENTILE_APPROX(SIZE(skills), 0.75) as q3_skills,
            PERCENTILE_APPROX(SIZE(skills), 0.25) as q1_skills
        FROM employees
    """)
    print("統計分析:")
    result.show()
    
    # 12. 數據品質檢查
    print("\n1️⃣2️⃣ 數據品質檢查")
    
    # 檢查數據完整性
    result = spark.sql("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT id) as unique_ids,
            COUNT(CASE WHEN name IS NULL OR name = '' THEN 1 END) as null_names,
            COUNT(CASE WHEN SIZE(skills) = 0 THEN 1 END) as no_skills,
            COUNT(CASE WHEN SIZE(projects) = 0 THEN 1 END) as no_projects,
            AVG(SIZE(skills)) as avg_skills_per_person,
            AVG(SIZE(projects)) as avg_projects_per_person
        FROM employees
    """)
    print("數據品質檢查:")
    result.show()
    
    # 重複數據檢查
    result = spark.sql("""
        SELECT name, COUNT(*) as count
        FROM employees
        GROUP BY name
        HAVING COUNT(*) > 1
    """)
    print("重複數據檢查:")
    result.show()
    
    # 取消快取
    spark.sql("UNCACHE TABLE employees")
    print("快取已清除")
    
    # 停止 SparkSession
    spark.stop()
    print("\n✅ Spark SQL 進階函數示範完成")

if __name__ == "__main__":
    main()