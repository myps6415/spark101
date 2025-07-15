#!/usr/bin/env python3
"""
第4章練習1：基本 SQL 查詢和視圖管理
企業員工數據分析練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Spark SQL基本操作") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第4章練習1：基本 SQL 查詢和視圖管理 ===")
    
    # 員工數據
    employees_data = [
        (1, "John Doe", "Engineering", 75000, "Manager", "2020-01-15"),
        (2, "Alice Smith", "Marketing", 65000, "Analyst", "2021-03-10"),
        (3, "Bob Johnson", "Engineering", 80000, "Senior Engineer", "2019-06-20"),
        (4, "Charlie Brown", "Sales", 60000, "Representative", "2022-01-05"),
        (5, "Diana Wilson", "HR", 70000, "Specialist", "2020-11-30"),
        (6, "Eve Davis", "Engineering", 85000, "Lead Engineer", "2018-09-15"),
        (7, "Frank Miller", "Marketing", 58000, "Coordinator", "2021-08-22"),
        (8, "Grace Lee", "Sales", 72000, "Manager", "2019-12-01"),
        (9, "Henry Taylor", "Engineering", 78000, "Engineer", "2020-05-18"),
        (10, "Ivy Chen", "HR", 68000, "Generalist", "2021-02-14")
    ]
    
    employees_columns = ["emp_id", "name", "department", "salary", "position", "hire_date"]
    
    # 部門數據
    departments_data = [
        ("Engineering", "Eve Davis", "Tech Tower", 500000),
        ("Marketing", "Alice Smith", "Marketing Plaza", 250000),
        ("Sales", "Grace Lee", "Sales Center", 300000),
        ("HR", "Diana Wilson", "Admin Building", 150000)
    ]
    
    departments_columns = ["dept_name", "manager", "location", "budget"]
    
    # 專案數據
    projects_data = [
        (101, "Web Platform", "Engineering", "2024-01-01", "2024-06-30"),
        (102, "Mobile App", "Engineering", "2024-02-15", "2024-08-15"),
        (103, "Marketing Campaign", "Marketing", "2024-01-10", "2024-04-10"),
        (104, "Sales System", "Sales", "2024-03-01", "2024-09-01"),
        (105, "HR Portal", "HR", "2024-04-01", "2024-07-01")
    ]
    
    projects_columns = ["project_id", "project_name", "department", "start_date", "end_date"]
    
    # 創建 DataFrame
    employees_df = spark.createDataFrame(employees_data, employees_columns)
    departments_df = spark.createDataFrame(departments_data, departments_columns)
    projects_df = spark.createDataFrame(projects_data, projects_columns)
    
    print("\\n1. 原始數據展示:")
    print("員工數據:")
    employees_df.show()
    print("部門數據:")
    departments_df.show()
    print("專案數據:")
    projects_df.show()
    
    # 創建臨時視圖
    employees_df.createOrReplaceTempView("employees")
    departments_df.createOrReplaceTempView("departments")
    projects_df.createOrReplaceTempView("projects")
    
    print("\\n=== SQL 查詢練習 ===")
    
    # 2. 基本查詢
    print("\\n2. 部門薪資統計:")
    result1 = spark.sql(\"\"\"
        SELECT 
            department,
            COUNT(*) as employee_count,
            ROUND(AVG(salary), 2) as avg_salary,
            MAX(salary) as max_salary,
            MIN(salary) as min_salary
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    \"\"\")
    result1.show()
    
    # 3. WHERE 條件查詢
    print("\\n3. 高薪員工查詢 (薪資 > 70000):")
    result2 = spark.sql(\"\"\"
        SELECT name, department, salary, position
        FROM employees
        WHERE salary > 70000
        ORDER BY salary DESC
    \"\"\")
    result2.show()
    
    # 4. JOIN 查詢
    print("\\n4. 員工部門完整信息:")
    result3 = spark.sql(\"\"\"
        SELECT 
            e.name,
            e.position,
            e.salary,
            d.dept_name,
            d.location,
            d.manager as dept_manager,
            d.budget as dept_budget
        FROM employees e
        JOIN departments d ON e.department = d.dept_name
        ORDER BY e.salary DESC
    \"\"\")
    result3.show()
    
    # 5. 複雜 JOIN 查詢
    print("\\n5. 員工、部門、專案完整信息:")
    result4 = spark.sql(\"\"\"
        SELECT 
            e.name as employee_name,
            e.position,
            e.salary,
            d.dept_name,
            d.location,
            p.project_name,
            p.start_date,
            p.end_date
        FROM employees e
        JOIN departments d ON e.department = d.dept_name
        JOIN projects p ON e.department = p.department
        ORDER BY e.name, p.project_name
    \"\"\")
    result4.show()
    
    # 6. 使用 CTE (Common Table Expressions)
    print("\\n6. 使用 CTE 分析高薪員工和部門預算:")
    result5 = spark.sql(\"\"\"
        WITH high_salary_employees AS (
            SELECT department, COUNT(*) as high_earners
            FROM employees
            WHERE salary > 70000
            GROUP BY department
        ),
        dept_summary AS (
            SELECT 
                dept_name,
                budget,
                ROUND(budget / 1000, 0) as budget_k
            FROM departments
        )
        SELECT 
            ds.dept_name,
            ds.budget_k as budget_thousands,
            COALESCE(hse.high_earners, 0) as high_earners,
            ROUND(ds.budget / COALESCE(hse.high_earners, 1), 0) as budget_per_high_earner
        FROM dept_summary ds
        LEFT JOIN high_salary_employees hse ON ds.dept_name = hse.department
        ORDER BY ds.budget_k DESC
    \"\"\")
    result5.show()
    
    # 7. 子查詢
    print("\\n7. 薪資高於部門平均的員工:")
    result6 = spark.sql(\"\"\"
        SELECT 
            e1.name,
            e1.department,
            e1.salary,
            ROUND(dept_avg.avg_salary, 2) as dept_avg_salary,
            ROUND(e1.salary - dept_avg.avg_salary, 2) as salary_diff
        FROM employees e1
        JOIN (
            SELECT 
                department, 
                AVG(salary) as avg_salary
            FROM employees
            GROUP BY department
        ) dept_avg ON e1.department = dept_avg.department
        WHERE e1.salary > dept_avg.avg_salary
        ORDER BY e1.department, e1.salary DESC
    \"\"\")
    result6.show()
    
    # 8. 窗口函數
    print("\\n8. 使用窗口函數 - 部門內薪資排名:")
    result7 = spark.sql(\"\"\"
        SELECT 
            name,
            department,
            salary,
            ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank,
            RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank_with_ties,
            ROUND(AVG(salary) OVER (PARTITION BY department), 2) as dept_avg_salary
        FROM employees
        ORDER BY department, salary DESC
    \"\"\")
    result7.show()
    
    # 9. 日期函數
    print("\\n9. 員工入職時間分析:")
    result8 = spark.sql(\"\"\"
        SELECT 
            name,
            department,
            hire_date,
            YEAR(hire_date) as hire_year,
            DATEDIFF(CURRENT_DATE(), hire_date) as days_employed,
            CASE 
                WHEN DATEDIFF(CURRENT_DATE(), hire_date) > 1000 THEN 'Senior'
                WHEN DATEDIFF(CURRENT_DATE(), hire_date) > 500 THEN 'Experienced'
                ELSE 'New'
            END as tenure_category
        FROM employees
        ORDER BY hire_date
    \"\"\")
    result8.show()
    
    # 10. 統計分析
    print("\\n10. 綜合統計分析:")
    result9 = spark.sql(\"\"\"
        SELECT 
            department,
            COUNT(*) as total_employees,
            ROUND(AVG(salary), 2) as avg_salary,
            ROUND(STDDEV(salary), 2) as salary_stddev,
            MIN(hire_date) as earliest_hire,
            MAX(hire_date) as latest_hire,
            COUNT(DISTINCT position) as unique_positions
        FROM employees
        GROUP BY department
        ORDER BY avg_salary DESC
    \"\"\")
    result9.show()
    
    # 創建全局臨時視圖
    print("\\n=== 視圖管理 ===")
    employees_df.createGlobalTempView("global_employees")
    
    # 列出所有表和視圖
    print("\\n11. 當前數據庫中的表和視圖:")
    spark.sql("SHOW TABLES").show()
    
    # 查看表結構
    print("\\n12. 員工表結構:")
    spark.sql("DESCRIBE employees").show()
    
    # 清理資源
    spark.stop()
    print("\\n練習完成！")

if __name__ == "__main__":
    main()