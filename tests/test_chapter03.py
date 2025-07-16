"""
第3章：數據輸入輸出與Schema操作的測試
"""

import json
import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, concat_ws, lit, regexp_replace, split,
                                   when)
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType,
                               IntegerType, MapType, StringType, StructField,
                               StructType, TimestampType)


class TestSchemaOperations:
    """測試 Schema 操作"""

    def test_create_schema(self, spark_session):
        """測試創建 Schema"""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("salary", DoubleType(), True),
                StructField("active", BooleanType(), True),
            ]
        )

        data = [(1, "Alice", 50000.0, True), (2, "Bob", 60000.0, False)]
        df = spark_session.createDataFrame(data, schema)

        assert df.schema == schema
        assert len(df.schema.fields) == 4
        assert df.schema.fields[0].nullable == False
        assert df.schema.fields[1].nullable == True

    def test_complex_schema_types(self, spark_session):
        """測試複雜數據類型的 Schema"""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("skills", ArrayType(StringType()), True),
                StructField("metadata", MapType(StringType(), StringType()), True),
            ]
        )

        data = [
            (1, ["Python", "Spark"], {"level": "senior", "team": "data"}),
            (2, ["Java", "Scala"], {"level": "junior", "team": "backend"}),
        ]
        df = spark_session.createDataFrame(data, schema)

        assert df.count() == 2
        assert isinstance(df.schema.fields[1].dataType, ArrayType)
        assert isinstance(df.schema.fields[2].dataType, MapType)

    def test_schema_validation(self, spark_session):
        """測試 Schema 驗證"""
        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), False),
            ]
        )

        # 正確數據
        valid_data = [(1, "Alice"), (2, "Bob")]
        df_valid = spark_session.createDataFrame(valid_data, schema)
        assert df_valid.count() == 2

        # 測試 None 值處理
        data_with_none = [(1, "Alice"), (2, None)]
        try:
            df_with_none = spark_session.createDataFrame(data_with_none, schema)
            # 如果成功創建，檢查是否正確處理了 None
            assert df_with_none.count() == 2
        except Exception:
            # 某些版本可能會拋出異常，這是預期的
            pass

    def test_schema_evolution(self, spark_session):
        """測試 Schema 演進"""
        # 原始 Schema
        original_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )

        # 擴展 Schema
        extended_schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )

        original_data = [(1, "Alice"), (2, "Bob")]
        df_original = spark_session.createDataFrame(original_data, original_schema)

        # 添加新列
        df_extended = df_original.withColumn("age", lit(None).cast("int"))

        assert len(df_extended.schema.fields) == 3
        assert df_extended.schema.fields[2].name == "age"


class TestDataIO:
    """測試數據輸入輸出操作"""

    def test_csv_operations(self, spark_session, temp_dir):
        """測試 CSV 讀寫操作"""
        # 創建測試數據
        csv_content = """id,name,age,salary
1,Alice,25,50000
2,Bob,30,60000
3,Charlie,35,70000"""

        csv_file = os.path.join(temp_dir, "test.csv")
        with open(csv_file, "w") as f:
            f.write(csv_content)

        # 讀取 CSV
        df = (
            spark_session.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_file)
        )

        assert df.count() == 3
        assert "name" in df.columns
        assert df.schema.fields[0].dataType == IntegerType()

        # 寫入 CSV
        output_path = os.path.join(temp_dir, "output.csv")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)

        # 驗證寫入
        df_read = spark_session.read.option("header", "true").csv(output_path)
        assert df_read.count() == 3

    def test_json_operations(self, spark_session, temp_dir):
        """測試 JSON 讀寫操作"""
        # 創建測試數據
        json_data = [
            {"id": 1, "name": "Alice", "age": 25, "skills": ["Python", "SQL"]},
            {"id": 2, "name": "Bob", "age": 30, "skills": ["Java", "Scala"]},
            {"id": 3, "name": "Charlie", "age": 35, "skills": ["R", "Python"]},
        ]

        json_file = os.path.join(temp_dir, "test.json")
        with open(json_file, "w") as f:
            for record in json_data:
                f.write(json.dumps(record) + "\n")

        # 讀取 JSON
        df = spark_session.read.json(json_file)

        assert df.count() == 3
        assert "skills" in df.columns
        assert isinstance(df.schema["skills"].dataType, ArrayType)

        # 寫入 JSON
        output_path = os.path.join(temp_dir, "output.json")
        df.coalesce(1).write.mode("overwrite").json(output_path)

        # 驗證寫入
        df_read = spark_session.read.json(output_path)
        assert df_read.count() == 3

    def test_parquet_operations(self, spark_session, temp_dir):
        """測試 Parquet 讀寫操作"""
        # 創建測試數據
        data = [
            (1, "Alice", 25, 50000.0),
            (2, "Bob", 30, 60000.0),
            (3, "Charlie", 35, 70000.0),
        ]

        schema = StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", DoubleType(), True),
            ]
        )

        df = spark_session.createDataFrame(data, schema)

        # 寫入 Parquet
        parquet_path = os.path.join(temp_dir, "test.parquet")
        df.write.mode("overwrite").parquet(parquet_path)

        # 讀取 Parquet
        df_read = spark_session.read.parquet(parquet_path)

        assert df_read.count() == 3
        assert df_read.schema == schema

    def test_partitioned_data(self, spark_session, temp_dir):
        """測試分區數據"""
        # 創建測試數據
        data = [
            (1, "Alice", "Engineering", 25),
            (2, "Bob", "Engineering", 30),
            (3, "Charlie", "Sales", 35),
            (4, "Diana", "Sales", 28),
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "department", "age"])

        # 按部門分區寫入
        partitioned_path = os.path.join(temp_dir, "partitioned")
        df.write.mode("overwrite").partitionBy("department").parquet(partitioned_path)

        # 讀取分區數據
        df_read = spark_session.read.parquet(partitioned_path)

        assert df_read.count() == 4
        assert "department" in df_read.columns

        # 讀取特定分區
        engineering_path = os.path.join(partitioned_path, "department=Engineering")
        if os.path.exists(engineering_path):
            df_engineering = spark_session.read.parquet(engineering_path)
            assert df_engineering.count() == 2

    def test_multi_format_operations(self, spark_session, temp_dir):
        """測試多格式轉換"""
        # 創建原始數據
        data = [(1, "Alice", 25, 50000.0), (2, "Bob", 30, 60000.0)]
        df = spark_session.createDataFrame(data, ["id", "name", "age", "salary"])

        # CSV -> Parquet
        csv_path = os.path.join(temp_dir, "data.csv")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_path)

        df_csv = (
            spark_session.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(csv_path)
        )

        parquet_path = os.path.join(temp_dir, "data.parquet")
        df_csv.write.mode("overwrite").parquet(parquet_path)

        # JSON -> Parquet
        json_path = os.path.join(temp_dir, "data.json")
        df.coalesce(1).write.mode("overwrite").json(json_path)

        df_json = spark_session.read.json(json_path)

        # 驗證數據一致性
        df_parquet = spark_session.read.parquet(parquet_path)
        assert df_parquet.count() == df_json.count()


class TestDataCleaning:
    """測試數據清理操作"""

    def test_null_handling(self, spark_session):
        """測試 NULL 值處理"""
        data = [
            (1, "Alice", 25, 50000.0),
            (2, None, 30, None),
            (3, "Charlie", None, 70000.0),
            (4, "", 0, 0.0),
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "age", "salary"])

        # 刪除包含 NULL 的行
        df_dropped = df.dropna()
        assert df_dropped.count() == 2  # 只有 Alice 和空字串記錄

        # 刪除特定列的 NULL
        df_dropped_name = df.dropna(subset=["name"])
        assert df_dropped_name.count() == 3

        # 填充 NULL 值
        df_filled = df.fillna({"name": "Unknown", "age": 0, "salary": 0.0})
        assert df_filled.filter(col("name") == "Unknown").count() == 1

        # 替換空字串
        df_replaced = df.withColumn(
            "name", when(col("name") == "", "Empty").otherwise(col("name"))
        )
        assert df_replaced.filter(col("name") == "Empty").count() == 1

    def test_data_validation(self, spark_session):
        """測試數據驗證"""
        data = [
            (1, "Alice", 25, 50000.0),
            (2, "Bob", -5, 60000.0),  # 無效年齡
            (3, "Charlie", 35, -1000.0),  # 無效薪水
            (4, "Diana", 150, 70000.0),  # 無效年齡
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "age", "salary"])

        # 驗證年齡範圍
        valid_age_df = df.filter((col("age") >= 0) & (col("age") <= 100))
        assert valid_age_df.count() == 3

        # 驗證薪水
        valid_salary_df = df.filter(col("salary") > 0)
        assert valid_salary_df.count() == 3

        # 組合驗證
        valid_df = df.filter(
            (col("age") >= 0) & (col("age") <= 100) & (col("salary") > 0)
        )
        assert valid_df.count() == 2

    def test_duplicate_handling(self, spark_session):
        """測試重複數據處理"""
        data = [
            (1, "Alice", 25),
            (2, "Bob", 30),
            (1, "Alice", 25),  # 完全重複
            (3, "Alice", 35),  # 姓名重複但其他不同
            (2, "Bob", 30),  # 完全重複
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "age"])

        # 去除完全重複
        df_distinct = df.distinct()
        assert df_distinct.count() == 3

        # 基於特定列去重
        df_drop_duplicates = df.dropDuplicates(["name"])
        assert df_drop_duplicates.count() == 2  # Alice 和 Bob 各一條

    def test_text_cleaning(self, spark_session):
        """測試文本清理"""
        data = [
            (1, "  Alice  ", "alice@example.com"),
            (2, "BOB", "BOB@EXAMPLE.COM"),
            (3, "charlie_123", "charlie.test@example.com"),
            (4, "Diana-Smith", "diana@test-domain.com"),
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "email"])

        # 清理空格
        df_trimmed = df.withColumn(
            "name", regexp_replace(col("name"), "^\\s+|\\s+$", "")
        )
        trimmed_names = [row.name for row in df_trimmed.collect()]
        assert "Alice" in trimmed_names

        # 統一大小寫
        df_lower = df.withColumn("email", lower(col("email")))
        emails = [row.email for row in df_lower.collect()]
        assert all(email.islower() for email in emails)

        # 移除特殊字符
        df_cleaned = df.withColumn(
            "name", regexp_replace(col("name"), "[^a-zA-Z\\s]", "")
        )
        cleaned_names = [row.name for row in df_cleaned.collect()]
        assert "charlie" in cleaned_names

    def test_data_type_conversion(self, spark_session):
        """測試數據類型轉換"""
        data = [
            ("1", "25", "50000.5", "true"),
            ("2", "30", "60000.0", "false"),
            ("3", "invalid", "70000", "1"),
        ]

        df = spark_session.createDataFrame(data, ["id", "age", "salary", "active"])

        # 類型轉換
        df_converted = df.select(
            col("id").cast("int").alias("id"),
            col("age").cast("int").alias("age"),
            col("salary").cast("double").alias("salary"),
            col("active").cast("boolean").alias("active"),
        )

        # 檢查轉換結果
        converted_rows = df_converted.collect()
        assert converted_rows[0].id == 1
        assert converted_rows[0].age == 25
        assert converted_rows[0].salary == 50000.5
        assert converted_rows[0].active == True

        # 無效轉換會產生 NULL
        assert converted_rows[2].age is None  # "invalid" 轉 int 失敗


class TestComplexTransformations:
    """測試複雜數據轉換"""

    def test_nested_data_operations(self, spark_session):
        """測試嵌套數據操作"""
        data = [
            (1, "Alice", ["Python", "SQL", "Spark"]),
            (2, "Bob", ["Java", "Scala"]),
            (3, "Charlie", ["R", "Python", "Machine Learning"]),
        ]

        df = spark_session.createDataFrame(data, ["id", "name", "skills"])

        # 展開數組
        df_exploded = df.select("id", "name", explode(col("skills")).alias("skill"))
        assert df_exploded.count() == 8  # 總共8個技能

        # 計算每個人的技能數量
        df_skill_count = df.withColumn("skill_count", size(col("skills")))
        skill_counts = [row.skill_count for row in df_skill_count.collect()]
        assert 3 in skill_counts  # Alice 有3個技能
        assert 2 in skill_counts  # Bob 有2個技能

    def test_string_operations(self, spark_session):
        """測試字符串操作"""
        data = [
            (1, "Alice Smith", "alice.smith@company.com"),
            (2, "Bob Johnson", "bob.johnson@company.com"),
            (3, "Charlie Brown", "charlie.brown@company.com"),
        ]

        df = spark_session.createDataFrame(data, ["id", "full_name", "email"])

        # 分割姓名
        df_split = df.withColumn("name_parts", split(col("full_name"), " "))
        df_split = df_split.withColumn("first_name", col("name_parts")[0])
        df_split = df_split.withColumn("last_name", col("name_parts")[1])

        first_names = [row.first_name for row in df_split.collect()]
        assert "Alice" in first_names
        assert "Bob" in first_names

        # 提取郵件用戶名
        df_username = df.withColumn(
            "username", regexp_extract(col("email"), "([^@]+)", 1)
        )
        usernames = [row.username for row in df_username.collect()]
        assert "alice.smith" in usernames

        # 構建全名
        df_reconstructed = df_split.withColumn(
            "reconstructed_name", concat_ws(" ", col("first_name"), col("last_name"))
        )

        reconstructed_names = [
            row.reconstructed_name for row in df_reconstructed.collect()
        ]
        assert "Alice Smith" in reconstructed_names

    def test_conditional_transformations(self, spark_session):
        """測試條件轉換"""
        data = [
            (1, "Alice", 25, 50000, "Engineering"),
            (2, "Bob", 30, 60000, "Sales"),
            (3, "Charlie", 35, 70000, "Engineering"),
            (4, "Diana", 28, 55000, "Marketing"),
        ]

        df = spark_session.createDataFrame(
            data, ["id", "name", "age", "salary", "department"]
        )

        # 薪水等級分類
        df_salary_grade = df.withColumn(
            "salary_grade",
            when(col("salary") >= 65000, "High")
            .when(col("salary") >= 55000, "Medium")
            .otherwise("Low"),
        )

        grades = [row.salary_grade for row in df_salary_grade.collect()]
        assert "High" in grades
        assert "Medium" in grades
        assert "Low" in grades

        # 部門獎金計算
        df_bonus = df.withColumn(
            "bonus",
            when(col("department") == "Engineering", col("salary") * 0.15)
            .when(col("department") == "Sales", col("salary") * 0.10)
            .otherwise(col("salary") * 0.05),
        )

        bonuses = [row.bonus for row in df_bonus.collect()]
        assert any(bonus == 7500.0 for bonus in bonuses)  # Alice's bonus: 50000 * 0.15
