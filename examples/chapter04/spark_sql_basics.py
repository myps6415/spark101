#!/usr/bin/env python3
"""
第4章：Spark SQL - SQL 查詢基礎
學習 Spark SQL 的基本語法和查詢操作
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
from pyspark.sql.functions import col, when, desc, asc
from datetime import date

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Spark SQL Basics") \
        .master("local[*]") \
        .getOrCreate()
    
    print("🔍 Spark SQL 基礎查詢示範")
    print("=" * 40)
    
    # 準備示例數據
    
    # 員工數據
    employees_data = [
        (1, "Alice", 25, "Engineer", 75000.0, "IT", date(2020, 1, 15)),
        (2, "Bob", 30, "Manager", 85000.0, "Sales", date(2019, 3, 20)),
        (3, "Charlie", 35, "Designer", 65000.0, "Marketing", date(2021, 7, 10)),
        (4, "Diana", 28, "Analyst", 70000.0, "Finance", date(2020, 11, 5)),
        (5, "Eve", 32, "Engineer", 80000.0, "IT", date(2018, 9, 12)),
        (6, "Frank", 29, "Manager", 90000.0, "IT", date(2019, 6, 18)),
        (7, "Grace", 26, "Designer", 62000.0, "Marketing", date(2021, 2, 28)),
        (8, "Henry", 33, "Analyst", 72000.0, "Finance", date(2020, 8, 22))
    ]
    
    employees_schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), False),
        StructField("age", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("salary", DoubleType(), True),
        StructField("department", StringType(), True),
        StructField("hire_date", DateType(), True)
    ])
    
    # 部門數據
    departments_data = [
        ("IT", "Information Technology", "Alice Johnson"),
        ("Sales", "Sales Department", "Bob Wilson"),
        ("Marketing", "Marketing Department", "Charlie Davis"),
        ("Finance", "Finance Department", "Diana Lee")
    ]
    
    departments_schema = StructType([
        StructField("dept_code", StringType(), False),
        StructField("dept_name", StringType(), False),
        StructField("manager", StringType(), True)
    ])
    
    # 創建 DataFrame
    employees_df = spark.createDataFrame(employees_data, employees_schema)
    departments_df = spark.createDataFrame(departments_data, departments_schema)
    
    print("員工數據:")
    employees_df.show()
    
    print("部門數據:")
    departments_df.show()
    
    # 1. 創建臨時視圖
    print("\n1️⃣ 創建臨時視圖")
    
    # 創建臨時視圖
    employees_df.createOrReplaceTempView("employees")
    departments_df.createOrReplaceTempView("departments")
    
    print("臨時視圖 'employees' 和 'departments' 已創建")
    
    # 2. 基本 SELECT 查詢
    print("\n2️⃣ 基本 SELECT 查詢")
    
    # 選擇所有列
    result = spark.sql("SELECT * FROM employees")
    print("選擇所有列:")
    result.show()
    
    # 選擇特定列
    result = spark.sql("SELECT name, age, salary FROM employees")
    print("選擇特定列:")
    result.show()
    
    # 使用別名
    result = spark.sql("""
        SELECT name AS employee_name, 
               age AS employee_age,
               salary AS monthly_salary
        FROM employees
    """)
    print("使用別名:")
    result.show()
    
    # 3. WHERE 條件查詢
    print("\n3️⃣ WHERE 條件查詢")
    
    # 數值條件
    result = spark.sql("SELECT * FROM employees WHERE age > 30")
    print("年齡大於 30 的員工:")
    result.show()
    
    # 字串條件
    result = spark.sql("SELECT * FROM employees WHERE department = 'IT'")
    print("IT 部門的員工:")
    result.show()
    
    # 多條件查詢
    result = spark.sql("""
        SELECT * FROM employees 
        WHERE age > 25 AND salary > 70000
    """)
    print("年齡大於 25 且薪資大於 70000 的員工:")
    result.show()
    
    # 範圍查詢
    result = spark.sql("SELECT * FROM employees WHERE salary BETWEEN 65000 AND 80000")
    print("薪資在 65000-80000 之間的員工:")
    result.show()
    
    # IN 查詢
    result = spark.sql("SELECT * FROM employees WHERE job IN ('Engineer', 'Manager')")
    print("職位是 Engineer 或 Manager 的員工:")
    result.show()
    
    # LIKE 查詢
    result = spark.sql("SELECT * FROM employees WHERE name LIKE 'A%'")
    print("姓名以 'A' 開頭的員工:")
    result.show()
    
    # 4. ORDER BY 排序
    print("\n4️⃣ ORDER BY 排序")
    
    # 升序排序
    result = spark.sql("SELECT * FROM employees ORDER BY age ASC")
    print("按年齡升序排序:")
    result.show()
    
    # 降序排序
    result = spark.sql("SELECT * FROM employees ORDER BY salary DESC")
    print("按薪資降序排序:")
    result.show()
    
    # 多欄位排序
    result = spark.sql("SELECT * FROM employees ORDER BY department ASC, salary DESC")
    print("按部門升序、薪資降序排序:")
    result.show()
    
    # 5. GROUP BY 分組查詢
    print("\n5️⃣ GROUP BY 分組查詢")
    
    # 按部門分組統計
    result = spark.sql("""
        SELECT department, 
               COUNT(*) as employee_count,
               AVG(salary) as avg_salary,
               MAX(salary) as max_salary,
               MIN(salary) as min_salary
        FROM employees 
        GROUP BY department
    """)
    print("按部門分組統計:")
    result.show()
    
    # 按職位分組統計
    result = spark.sql("""
        SELECT job,
               COUNT(*) as count,
               AVG(age) as avg_age,
               SUM(salary) as total_salary
        FROM employees
        GROUP BY job
        ORDER BY count DESC
    """)
    print("按職位分組統計:")
    result.show()
    
    # HAVING 子句
    result = spark.sql("""
        SELECT department,
               COUNT(*) as employee_count,
               AVG(salary) as avg_salary
        FROM employees
        GROUP BY department
        HAVING COUNT(*) > 1
    """)
    print("員工數量大於 1 的部門:")
    result.show()
    
    # 6. JOIN 連接查詢
    print("\n6️⃣ JOIN 連接查詢")
    
    # INNER JOIN
    result = spark.sql("""
        SELECT e.name, e.job, e.salary, d.dept_name
        FROM employees e
        INNER JOIN departments d ON e.department = d.dept_code
    """)
    print("員工和部門信息 (INNER JOIN):")
    result.show()
    
    # LEFT JOIN
    result = spark.sql("""
        SELECT e.name, e.job, e.salary, d.dept_name, d.manager
        FROM employees e
        LEFT JOIN departments d ON e.department = d.dept_code
    """)
    print("員工和部門信息 (LEFT JOIN):")
    result.show()
    
    # 7. 子查詢
    print("\n7️⃣ 子查詢")
    
    # 標量子查詢
    result = spark.sql("""
        SELECT name, salary,
               (SELECT AVG(salary) FROM employees) as avg_salary
        FROM employees
        WHERE salary > (SELECT AVG(salary) FROM employees)
    """)
    print("薪資高於平均薪資的員工:")
    result.show()
    
    # 相關子查詢
    result = spark.sql("""
        SELECT e1.name, e1.department, e1.salary
        FROM employees e1
        WHERE e1.salary = (
            SELECT MAX(e2.salary)
            FROM employees e2
            WHERE e2.department = e1.department
        )
    """)
    print("每個部門薪資最高的員工:")
    result.show()
    
    # 8. 窗口函數
    print("\n8️⃣ 窗口函數")
    
    # ROW_NUMBER
    result = spark.sql("""
        SELECT name, department, salary,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as rank
        FROM employees
    """)
    print("按部門薪資排名:")
    result.show()
    
    # RANK 和 DENSE_RANK
    result = spark.sql("""
        SELECT name, department, salary,
               RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
               DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank
        FROM employees
    """)
    print("按部門薪資排名 (RANK vs DENSE_RANK):")
    result.show()
    
    # 累計統計
    result = spark.sql("""
        SELECT name, department, salary,
               SUM(salary) OVER (PARTITION BY department ORDER BY salary) as cumulative_salary,
               AVG(salary) OVER (PARTITION BY department) as dept_avg_salary
        FROM employees
        ORDER BY department, salary
    """)
    print("累計薪資統計:")
    result.show()
    
    # 9. 常用函數
    print("\n9️⃣ 常用函數")
    
    # 字串函數
    result = spark.sql("""
        SELECT name,
               UPPER(name) as upper_name,
               LOWER(name) as lower_name,
               LENGTH(name) as name_length,
               CONCAT(name, ' - ', job) as name_job
        FROM employees
    """)
    print("字串函數:")
    result.show()
    
    # 數值函數
    result = spark.sql("""
        SELECT name, salary,
               ROUND(salary / 12, 2) as monthly_salary,
               CEIL(salary / 1000) as salary_k_ceil,
               FLOOR(salary / 1000) as salary_k_floor
        FROM employees
    """)
    print("數值函數:")
    result.show()
    
    # 日期函數
    result = spark.sql("""
        SELECT name, hire_date,
               YEAR(hire_date) as hire_year,
               MONTH(hire_date) as hire_month,
               DAYOFYEAR(hire_date) as day_of_year,
               DATEDIFF(CURRENT_DATE(), hire_date) as days_since_hire
        FROM employees
    """)
    print("日期函數:")
    result.show()
    
    # 10. 條件函數
    print("\n🔟 條件函數")
    
    # CASE WHEN
    result = spark.sql("""
        SELECT name, age, salary,
               CASE 
                   WHEN age < 30 THEN 'Young'
                   WHEN age < 35 THEN 'Middle'
                   ELSE 'Senior'
               END as age_group,
               CASE
                   WHEN salary > 80000 THEN 'High'
                   WHEN salary > 70000 THEN 'Medium'
                   ELSE 'Low'
               END as salary_level
        FROM employees
    """)
    print("條件分組:")
    result.show()
    
    # IF 函數
    result = spark.sql("""
        SELECT name, salary,
               IF(salary > 75000, 'High Earner', 'Regular') as earner_type
        FROM employees
    """)
    print("IF 函數:")
    result.show()
    
    # 11. 集合操作
    print("\n1️⃣1️⃣ 集合操作")
    
    # 創建兩個子集用於演示
    high_earners = spark.sql("SELECT name FROM employees WHERE salary > 75000")
    it_employees = spark.sql("SELECT name FROM employees WHERE department = 'IT'")
    
    # 註冊為臨時視圖
    high_earners.createOrReplaceTempView("high_earners")
    it_employees.createOrReplaceTempView("it_employees")
    
    # UNION
    result = spark.sql("""
        SELECT name, 'High Earner' as category FROM high_earners
        UNION ALL
        SELECT name, 'IT Employee' as category FROM it_employees
    """)
    print("UNION 操作:")
    result.show()
    
    # INTERSECT
    result = spark.sql("""
        SELECT name FROM high_earners
        INTERSECT
        SELECT name FROM it_employees
    """)
    print("INTERSECT 操作 (高薪且IT部門):")
    result.show()
    
    # EXCEPT
    result = spark.sql("""
        SELECT name FROM high_earners
        EXCEPT
        SELECT name FROM it_employees
    """)
    print("EXCEPT 操作 (高薪但非IT部門):")
    result.show()
    
    # 12. 複雜查詢示例
    print("\n1️⃣2️⃣ 複雜查詢示例")
    
    # 部門薪資分析
    result = spark.sql("""
        WITH dept_stats AS (
            SELECT department,
                   COUNT(*) as emp_count,
                   AVG(salary) as avg_salary,
                   MAX(salary) as max_salary,
                   MIN(salary) as min_salary,
                   STDDEV(salary) as salary_stddev
            FROM employees
            GROUP BY department
        )
        SELECT d.dept_name,
               ds.emp_count,
               ROUND(ds.avg_salary, 2) as avg_salary,
               ds.max_salary,
               ds.min_salary,
               ROUND(ds.salary_stddev, 2) as salary_stddev,
               ds.manager
        FROM dept_stats ds
        JOIN departments d ON ds.department = d.dept_code
        ORDER BY ds.avg_salary DESC
    """)
    print("部門薪資分析:")
    result.show()
    
    # 員工排名和薪資差異
    result = spark.sql("""
        SELECT name, department, salary,
               RANK() OVER (ORDER BY salary DESC) as overall_rank,
               RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
               salary - AVG(salary) OVER (PARTITION BY department) as salary_diff_from_dept_avg,
               salary - AVG(salary) OVER () as salary_diff_from_overall_avg
        FROM employees
        ORDER BY salary DESC
    """)
    print("員工排名和薪資差異:")
    result.show()
    
    # 停止 SparkSession
    spark.stop()
    print("\n✅ Spark SQL 基礎查詢示範完成")

if __name__ == "__main__":
    main()