"""
第4章：Spark SQL 的測試
"""

import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, concat, concat_ws, count,
                                   current_date, current_timestamp, date_add,
                                   date_sub, datediff, dayofmonth, dense_rank,
                                   first, lag, last, lead, lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import (month, rank, regexp_extract, regexp_replace,
                                   row_number, split)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_date, to_timestamp, when, year
from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)
from pyspark.sql.window import Window


class TestSparkSQL:
    """測試 Spark SQL 基本操作"""

    def test_create_temporary_view(self, spark_session, sample_data):
        """測試創建臨時視圖"""
        sample_data.createOrReplaceTempView("employees")

        # 驗證視圖是否創建成功
        result = spark_session.sql("SELECT COUNT(*) as count FROM employees")
        count = result.collect()[0].count
        assert count == 4

    def test_basic_sql_queries(self, spark_session, sample_data):
        """測試基本 SQL 查詢"""
        sample_data.createOrReplaceTempView("employees")

        # SELECT 查詢
        result = spark_session.sql("SELECT name, age FROM employees WHERE age > 28")
        assert result.count() == 2

        # 聚合查詢
        result = spark_session.sql(
            "SELECT AVG(age) as avg_age, MAX(salary) as max_salary FROM employees"
        )
        row = result.collect()[0]
        assert row.avg_age == 29.5  # (25+30+35+28)/4
        assert row.max_salary == 85000

        # GROUP BY 查詢
        result = spark_session.sql(
            "SELECT job, COUNT(*) as count FROM employees GROUP BY job"
        )
        assert result.count() == 4  # 4 different jobs

    def test_join_operations_sql(self, spark_session):
        """測試 SQL JOIN 操作"""
        # 創建員工表
        employees_data = [
            (1, "Alice", "Engineering"),
            (2, "Bob", "Sales"),
            (3, "Charlie", "Engineering"),
        ]
        employees_df = spark_session.createDataFrame(
            employees_data, ["id", "name", "department"]
        )
        employees_df.createOrReplaceTempView("employees")

        # 創建部門表
        departments_data = [
            ("Engineering", "Tech Building"),
            ("Sales", "Main Building"),
            ("HR", "Admin Building"),
        ]
        departments_df = spark_session.createDataFrame(
            departments_data, ["dept_name", "location"]
        )
        departments_df.createOrReplaceTempView("departments")

        # INNER JOIN
        inner_join_sql = """
        SELECT e.name, e.department, d.location 
        FROM employees e 
        INNER JOIN departments d ON e.department = d.dept_name
        """
        result = spark_session.sql(inner_join_sql)
        assert result.count() == 3

        # LEFT JOIN
        left_join_sql = """
        SELECT e.name, e.department, d.location 
        FROM employees e 
        LEFT JOIN departments d ON e.department = d.dept_name
        """
        result = spark_session.sql(left_join_sql)
        assert result.count() == 3

    def test_subqueries(self, spark_session, sample_data):
        """測試子查詢"""
        sample_data.createOrReplaceTempView("employees")

        # 子查詢
        subquery_sql = """
        SELECT name, salary 
        FROM employees 
        WHERE salary > (SELECT AVG(salary) FROM employees)
        """
        result = spark_session.sql(subquery_sql)
        assert result.count() == 2  # Bob 和 Charlie

        # 相關子查詢
        correlated_sql = """
        SELECT name, job, salary
        FROM employees e1
        WHERE salary = (
            SELECT MAX(salary) 
            FROM employees e2 
            WHERE e1.job = e2.job
        )
        """
        result = spark_session.sql(correlated_sql)
        assert result.count() == 4  # 每個職位都只有一個人，所以都是最高薪水

    def test_cte_queries(self, spark_session, sample_data):
        """測試公用表表達式 (CTE)"""
        sample_data.createOrReplaceTempView("employees")

        cte_sql = """
        WITH high_salary_employees AS (
            SELECT name, salary, job 
            FROM employees 
            WHERE salary > 70000
        ),
        department_stats AS (
            SELECT job, COUNT(*) as emp_count, AVG(salary) as avg_salary
            FROM employees
            GROUP BY job
        )
        SELECT h.name, h.salary, d.avg_salary
        FROM high_salary_employees h
        JOIN department_stats d ON h.job = d.job
        """
        result = spark_session.sql(cte_sql)
        assert result.count() == 2  # Bob 和 Charlie


class TestWindowFunctions:
    """測試視窗函數"""

    def test_ranking_functions(self, spark_session):
        """測試排名函數"""
        data = [
            ("Alice", "Engineering", 75000),
            ("Bob", "Engineering", 80000),
            ("Charlie", "Sales", 65000),
            ("Diana", "Sales", 70000),
            ("Eve", "Sales", 72000),
        ]

        df = spark_session.createDataFrame(data, ["name", "department", "salary"])
        df.createOrReplaceTempView("employees")

        # 使用視窗函數進行排名
        window_sql = """
        SELECT name, department, salary,
               ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as row_num,
               RANK() OVER (PARTITION BY department ORDER BY salary DESC) as rank,
               DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) as dense_rank
        FROM employees
        """

        result = spark_session.sql(window_sql)
        rows = result.collect()

        # 檢查排名結果
        engineering_rows = [r for r in rows if r.department == "Engineering"]
        assert len(engineering_rows) == 2
        assert engineering_rows[0].name == "Bob"  # 最高薪水
        assert engineering_rows[0].row_num == 1

    def test_aggregate_window_functions(self, spark_session):
        """測試聚合視窗函數"""
        data = [
            ("2023-01-01", 1000),
            ("2023-01-02", 1200),
            ("2023-01-03", 800),
            ("2023-01-04", 1500),
            ("2023-01-05", 900),
        ]

        df = spark_session.createDataFrame(data, ["date", "sales"])
        df.createOrReplaceTempView("daily_sales")

        window_sql = """
        SELECT date, sales,
               SUM(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3day_sum,
               AVG(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as rolling_3day_avg,
               LAG(sales, 1) OVER (ORDER BY date) as prev_day_sales,
               LEAD(sales, 1) OVER (ORDER BY date) as next_day_sales
        FROM daily_sales
        ORDER BY date
        """

        result = spark_session.sql(window_sql)
        rows = result.collect()

        # 檢查滾動計算
        assert rows[2].rolling_3day_sum == 3000  # 1000 + 1200 + 800
        assert rows[0].prev_day_sales is None
        assert rows[-1].next_day_sales is None

    def test_window_functions_dataframe(self, spark_session):
        """測試 DataFrame API 視窗函數"""
        data = [
            ("Alice", "Engineering", 75000),
            ("Bob", "Engineering", 80000),
            ("Charlie", "Sales", 65000),
            ("Diana", "Sales", 70000),
        ]

        df = spark_session.createDataFrame(data, ["name", "department", "salary"])

        # 定義視窗規範
        window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

        # 使用視窗函數
        df_with_rank = (
            df.withColumn("rank", rank().over(window_spec))
            .withColumn("row_number", row_number().over(window_spec))
            .withColumn("dense_rank", dense_rank().over(window_spec))
        )

        # 檢查結果
        rows = df_with_rank.collect()
        engineering_rows = [r for r in rows if r.department == "Engineering"]

        assert len(engineering_rows) == 2
        bob_row = next(r for r in engineering_rows if r.name == "Bob")
        assert bob_row.rank == 1
        assert bob_row.row_number == 1


class TestAdvancedSQL:
    """測試進階 SQL 功能"""

    def test_case_when_statements(self, spark_session, sample_data):
        """測試 CASE WHEN 語句"""
        sample_data.createOrReplaceTempView("employees")

        case_sql = """
        SELECT name, age, salary,
               CASE 
                   WHEN age < 30 THEN 'Young'
                   WHEN age >= 30 AND age < 35 THEN 'Middle'
                   ELSE 'Senior'
               END as age_group,
               CASE 
                   WHEN salary > 75000 THEN 'High'
                   WHEN salary > 65000 THEN 'Medium'
                   ELSE 'Low'
               END as salary_level
        FROM employees
        """

        result = spark_session.sql(case_sql)
        rows = result.collect()

        age_groups = [r.age_group for r in rows]
        salary_levels = [r.salary_level for r in rows]

        assert "Young" in age_groups
        assert "Middle" in age_groups
        assert "High" in salary_levels

    def test_string_functions(self, spark_session):
        """測試字符串函數"""
        data = [
            ("Alice Smith", "alice.smith@company.com"),
            ("Bob Johnson", "bob.johnson@company.com"),
            ("Charlie Brown", "charlie.brown@company.com"),
        ]

        df = spark_session.createDataFrame(data, ["full_name", "email"])
        df.createOrReplaceTempView("users")

        string_sql = """
        SELECT full_name, email,
               UPPER(full_name) as name_upper,
               LOWER(email) as email_lower,
               SUBSTRING(full_name, 1, POSITION(' ' IN full_name) - 1) as first_name,
               REGEXP_EXTRACT(email, '([^@]+)', 1) as username,
               LENGTH(full_name) as name_length,
               CONCAT('Hello, ', full_name, '!') as greeting
        FROM users
        """

        result = spark_session.sql(string_sql)
        rows = result.collect()

        # 檢查字符串操作結果
        alice_row = next(r for r in rows if "Alice" in r.full_name)
        assert alice_row.name_upper == "ALICE SMITH"
        assert alice_row.first_name == "Alice"
        assert alice_row.username == "alice.smith"
        assert alice_row.greeting == "Hello, Alice Smith!"

    def test_date_functions(self, spark_session):
        """測試日期函數"""
        data = [
            ("2023-01-15", "Alice"),
            ("2023-02-20", "Bob"),
            ("2023-03-10", "Charlie"),
        ]

        df = spark_session.createDataFrame(data, ["hire_date", "name"])
        df = df.withColumn("hire_date", to_date(col("hire_date")))
        df.createOrReplaceTempView("employees")

        date_sql = """
        SELECT name, hire_date,
               YEAR(hire_date) as hire_year,
               MONTH(hire_date) as hire_month,
               DAYOFMONTH(hire_date) as hire_day,
               DATE_ADD(hire_date, 90) as probation_end,
               DATEDIFF(CURRENT_DATE(), hire_date) as days_employed,
               DATE_FORMAT(hire_date, 'MMM dd, yyyy') as formatted_date
        FROM employees
        """

        result = spark_session.sql(date_sql)
        rows = result.collect()

        # 檢查日期操作結果
        alice_row = next(r for r in rows if r.name == "Alice")
        assert alice_row.hire_year == 2023
        assert alice_row.hire_month == 1
        assert alice_row.hire_day == 15

    def test_null_functions(self, spark_session):
        """測試 NULL 處理函數"""
        data = [
            ("Alice", 75000, "Engineering"),
            ("Bob", None, "Sales"),
            ("Charlie", 65000, None),
            (None, 80000, "Marketing"),
        ]

        df = spark_session.createDataFrame(data, ["name", "salary", "department"])
        df.createOrReplaceTempView("employees")

        null_sql = """
        SELECT 
            COALESCE(name, 'Unknown') as name,
            COALESCE(salary, 0) as salary,
            COALESCE(department, 'Unassigned') as department,
            ISNULL(name) as name_is_null,
            ISNOTNULL(salary) as salary_is_not_null,
            CASE WHEN name IS NULL THEN 'Missing Name' ELSE name END as name_checked
        FROM employees
        """

        result = spark_session.sql(null_sql)
        rows = result.collect()

        # 檢查 NULL 處理結果
        unknown_row = next(r for r in rows if r.name == "Unknown")
        assert unknown_row.name_is_null == True
        assert unknown_row.name_checked == "Missing Name"

    def test_set_operations(self, spark_session):
        """測試集合操作"""
        # 創建兩個數據集
        data1 = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
        data2 = [("Bob", 2), ("Charlie", 3), ("Diana", 4)]

        df1 = spark_session.createDataFrame(data1, ["name", "id"])
        df2 = spark_session.createDataFrame(data2, ["name", "id"])

        df1.createOrReplaceTempView("set1")
        df2.createOrReplaceTempView("set2")

        # UNION
        union_sql = "SELECT * FROM set1 UNION SELECT * FROM set2"
        union_result = spark_session.sql(union_sql)
        assert union_result.count() == 4  # 去重後的結果

        # UNION ALL
        union_all_sql = "SELECT * FROM set1 UNION ALL SELECT * FROM set2"
        union_all_result = spark_session.sql(union_all_sql)
        assert union_all_result.count() == 6  # 不去重的結果

        # INTERSECT
        intersect_sql = "SELECT * FROM set1 INTERSECT SELECT * FROM set2"
        intersect_result = spark_session.sql(intersect_sql)
        assert intersect_result.count() == 2  # Bob 和 Charlie

        # EXCEPT
        except_sql = "SELECT * FROM set1 EXCEPT SELECT * FROM set2"
        except_result = spark_session.sql(except_sql)
        assert except_result.count() == 1  # 只有 Alice


class TestComplexQueries:
    """測試複雜查詢"""

    def test_pivot_operations(self, spark_session):
        """測試數據透視"""
        data = [
            ("Alice", "Q1", 1000),
            ("Alice", "Q2", 1200),
            ("Alice", "Q3", 1100),
            ("Bob", "Q1", 1100),
            ("Bob", "Q2", 1300),
            ("Bob", "Q3", 1250),
        ]

        df = spark_session.createDataFrame(data, ["name", "quarter", "sales"])
        df.createOrReplaceTempView("sales_data")

        # 使用 DataFrame API 進行透視
        pivot_df = df.groupBy("name").pivot("quarter").sum("sales")

        # 檢查透視結果
        assert "Q1" in pivot_df.columns
        assert "Q2" in pivot_df.columns
        assert "Q3" in pivot_df.columns
        assert pivot_df.count() == 2

    def test_percentile_and_quantiles(self, spark_session):
        """測試百分位數和分位數"""
        data = [(i, i * 1000) for i in range(1, 101)]  # 1到100的數據
        df = spark_session.createDataFrame(data, ["id", "value"])
        df.createOrReplaceTempView("data")

        percentile_sql = """
        SELECT 
            PERCENTILE_APPROX(value, 0.25) as q1,
            PERCENTILE_APPROX(value, 0.5) as median,
            PERCENTILE_APPROX(value, 0.75) as q3,
            PERCENTILE_APPROX(value, 0.9) as p90
        FROM data
        """

        result = spark_session.sql(percentile_sql)
        row = result.collect()[0]

        # 檢查百分位數結果
        assert abs(row.q1 - 25000) < 1000  # 近似第一四分位數
        assert abs(row.median - 50000) < 1000  # 近似中位數
        assert abs(row.q3 - 75000) < 1000  # 近似第三四分位數

    def test_recursive_queries(self, spark_session):
        """測試遞歸查詢（層次數據）"""
        # 組織架構數據
        data = [
            (1, "CEO", None),
            (2, "VP Engineering", 1),
            (3, "VP Sales", 1),
            (4, "Senior Engineer", 2),
            (5, "Junior Engineer", 2),
            (6, "Sales Manager", 3),
            (7, "Sales Rep", 6),
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "manager_id"])
        df.createOrReplaceTempView("employees")

        # 查找所有直接和間接下屬
        hierarchy_sql = """
        SELECT e1.name as employee, e2.name as manager
        FROM employees e1
        LEFT JOIN employees e2 ON e1.manager_id = e2.id
        """

        result = spark_session.sql(hierarchy_sql)
        rows = result.collect()

        # 檢查層次結構
        ceo_row = next(r for r in rows if r.employee == "CEO")
        assert ceo_row.manager is None

        vp_eng_row = next(r for r in rows if r.employee == "VP Engineering")
        assert vp_eng_row.manager == "CEO"

    def test_analytical_functions(self, spark_session):
        """測試分析函數"""
        data = [
            ("2023-01-01", 1000),
            ("2023-01-02", 1200),
            ("2023-01-03", 800),
            ("2023-01-04", 1500),
            ("2023-01-05", 900),
            ("2023-01-06", 1100),
            ("2023-01-07", 1300),
        ]

        df = spark_session.createDataFrame(data, ["date", "sales"])
        df.createOrReplaceTempView("daily_sales")

        analytical_sql = """
        SELECT date, sales,
               sales - LAG(sales, 1) OVER (ORDER BY date) as daily_change,
               (sales - LAG(sales, 1) OVER (ORDER BY date)) / LAG(sales, 1) OVER (ORDER BY date) * 100 as pct_change,
               SUM(sales) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sales,
               AVG(sales) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING) as moving_avg_5day,
               NTILE(4) OVER (ORDER BY sales) as quartile
        FROM daily_sales
        ORDER BY date
        """

        result = spark_session.sql(analytical_sql)
        rows = result.collect()

        # 檢查分析結果
        assert len(rows) == 7
        assert rows[0].daily_change is None  # 第一天沒有前一天數據
        assert rows[1].daily_change == 200  # 1200 - 1000
        assert rows[-1].cumulative_sales == sum(row[1] for row in data)  # 累計銷售
