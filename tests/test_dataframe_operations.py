"""
第3章：DataFrame 操作的測試
"""

import os
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, lit, lower
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import upper, when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


class TestDataFrameOperations:
    """測試 DataFrame 操作"""

    def test_dataframe_creation_with_schema(self, spark_session):
        """測試使用 Schema 創建 DataFrame"""
        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", DoubleType(), True),
            ]
        )

        data = [("Alice", 25, 50000.0), ("Bob", 30, 60000.0)]
        df = spark_session.createDataFrame(data, schema)

        assert df.count() == 2
        assert df.schema == schema

    def test_dataframe_select_operations(self, sample_data):
        """測試 DataFrame select 操作"""
        # 基本選擇
        df1 = sample_data.select("name", "age")
        assert df1.columns == ["name", "age"]

        # 使用 col 函數
        df2 = sample_data.select(col("name"), col("age"))
        assert df2.columns == ["name", "age"]

        # 帶表達式的選擇
        df3 = sample_data.select(
            col("name"), (col("salary") / 12).alias("monthly_salary")
        )
        assert df3.columns == ["name", "monthly_salary"]

    def test_dataframe_filter_operations(self, sample_data):
        """測試 DataFrame filter 操作"""
        # 數值過濾
        df1 = sample_data.filter(col("age") > 28)
        assert df1.count() == 2

        # 字串過濾
        df2 = sample_data.filter(col("job") == "Engineer")
        assert df2.count() == 1

        # 複合條件
        df3 = sample_data.filter((col("age") > 25) & (col("salary") > 70000))
        assert df3.count() == 2

    def test_dataframe_with_column_operations(self, sample_data):
        """測試 DataFrame withColumn 操作"""
        # 添加新列
        df1 = sample_data.withColumn("bonus", col("salary") * 0.1)
        assert "bonus" in df1.columns

        # 修改現有列
        df2 = sample_data.withColumn("name", upper(col("name")))
        names = [row.name for row in df2.collect()]
        assert "ALICE" in names

        # 條件列
        df3 = sample_data.withColumn(
            "level", when(col("age") < 30, "Junior").otherwise("Senior")
        )
        assert "level" in df3.columns

    def test_dataframe_groupby_operations(self, sample_data):
        """測試 DataFrame groupBy 操作"""
        # 基本分組
        df1 = sample_data.groupBy("job").count()
        assert df1.count() == 4  # 4 different jobs

        # 聚合操作
        df2 = sample_data.groupBy("job").agg(
            avg("salary").alias("avg_salary"), spark_max("age").alias("max_age")
        )
        assert "avg_salary" in df2.columns
        assert "max_age" in df2.columns

    def test_dataframe_orderby_operations(self, sample_data):
        """測試 DataFrame orderBy 操作"""
        # 升序排序
        df1 = sample_data.orderBy("age")
        ages = [row.age for row in df1.collect()]
        assert ages == sorted(ages)

        # 降序排序
        df2 = sample_data.orderBy(col("salary").desc())
        salaries = [row.salary for row in df2.collect()]
        assert salaries == sorted(salaries, reverse=True)

    def test_dataframe_join_operations(self, spark_session):
        """測試 DataFrame join 操作"""
        # 創建兩個 DataFrame
        df1 = spark_session.createDataFrame(
            [("Alice", 1), ("Bob", 2), ("Charlie", 3)], ["name", "id"]
        )

        df2 = spark_session.createDataFrame(
            [(1, "HR"), (2, "IT"), (3, "Finance")], ["id", "department"]
        )

        # Inner join
        joined_df = df1.join(df2, "id")
        assert joined_df.count() == 3
        assert "department" in joined_df.columns

        # Left join
        left_joined_df = df1.join(df2, "id", "left")
        assert left_joined_df.count() == 3

    def test_dataframe_union_operations(self, spark_session):
        """測試 DataFrame union 操作"""
        df1 = spark_session.createDataFrame(
            [("Alice", 25), ("Bob", 30)], ["name", "age"]
        )

        df2 = spark_session.createDataFrame(
            [("Charlie", 35), ("Diana", 28)], ["name", "age"]
        )

        union_df = df1.union(df2)
        assert union_df.count() == 4

    def test_dataframe_distinct_operations(self, spark_session):
        """測試 DataFrame distinct 操作"""
        df = spark_session.createDataFrame(
            [("Alice", 25), ("Bob", 30), ("Alice", 25)], ["name", "age"]
        )

        distinct_df = df.distinct()
        assert distinct_df.count() == 2

    def test_dataframe_drop_operations(self, sample_data):
        """測試 DataFrame drop 操作"""
        # 刪除列
        df1 = sample_data.drop("salary")
        assert "salary" not in df1.columns
        assert len(df1.columns) == 3

        # 刪除多列
        df2 = sample_data.drop("salary", "job")
        assert "salary" not in df2.columns
        assert "job" not in df2.columns
        assert len(df2.columns) == 2

    def test_dataframe_rename_operations(self, sample_data):
        """測試 DataFrame rename 操作"""
        df = sample_data.withColumnRenamed("name", "employee_name")
        assert "employee_name" in df.columns
        assert "name" not in df.columns

    def test_dataframe_cast_operations(self, sample_data):
        """測試 DataFrame cast 操作"""
        df = sample_data.withColumn("age_str", col("age").cast("string"))
        assert "age_str" in df.columns

        # 檢查數據類型
        age_str_field = [
            field for field in df.schema.fields if field.name == "age_str"
        ][0]
        assert age_str_field.dataType == StringType()

    def test_dataframe_null_operations(self, spark_session):
        """測試 DataFrame null 處理操作"""
        # 創建包含 null 的 DataFrame
        df = spark_session.createDataFrame(
            [("Alice", 25, 50000), ("Bob", None, 60000), ("Charlie", 35, None)],
            ["name", "age", "salary"],
        )

        # 刪除包含 null 的行
        df_no_null = df.dropna()
        assert df_no_null.count() == 1

        # 填充 null 值
        df_filled = df.fillna({"age": 0, "salary": 0})
        assert df_filled.count() == 3

        # 檢查 null 值
        null_age_count = df.filter(col("age").isNull()).count()
        assert null_age_count == 1

    def test_dataframe_when_otherwise(self, sample_data):
        """測試 DataFrame when/otherwise 操作"""
        df = sample_data.withColumn(
            "salary_category",
            when(col("salary") > 75000, "High")
            .when(col("salary") > 65000, "Medium")
            .otherwise("Low"),
        )

        categories = [row.salary_category for row in df.collect()]
        assert "High" in categories
        assert "Medium" in categories
        assert "Low" in categories


class TestDataFrameIO:
    """測試 DataFrame 輸入輸出操作"""

    def test_read_csv(self, spark_session, sample_csv_file):
        """測試讀取 CSV 文件"""
        df = spark_session.read.option("header", "true").csv(sample_csv_file)
        assert df.count() == 3
        assert "name" in df.columns

    def test_read_json(self, spark_session, sample_json_file):
        """測試讀取 JSON 文件"""
        df = spark_session.read.json(sample_json_file)
        assert df.count() == 3
        assert "name" in df.columns

    def test_write_csv(self, sample_data, temp_dir):
        """測試寫入 CSV 文件"""
        output_path = os.path.join(temp_dir, "output.csv")

        sample_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(
            output_path
        )

        # 檢查文件是否創建
        assert os.path.exists(output_path)

        # 檢查是否可以讀取
        spark_session = sample_data.sql_ctx.sparkSession
        df_read = spark_session.read.option("header", "true").csv(output_path)
        assert df_read.count() == sample_data.count()

    def test_write_json(self, sample_data, temp_dir):
        """測試寫入 JSON 文件"""
        output_path = os.path.join(temp_dir, "output.json")

        sample_data.coalesce(1).write.mode("overwrite").json(output_path)

        # 檢查文件是否創建
        assert os.path.exists(output_path)

        # 檢查是否可以讀取
        spark_session = sample_data.sql_ctx.sparkSession
        df_read = spark_session.read.json(output_path)
        assert df_read.count() == sample_data.count()

    def test_write_parquet(self, sample_data, temp_dir):
        """測試寫入 Parquet 文件"""
        output_path = os.path.join(temp_dir, "output.parquet")

        sample_data.write.mode("overwrite").parquet(output_path)

        # 檢查文件是否創建
        assert os.path.exists(output_path)

        # 檢查是否可以讀取
        spark_session = sample_data.sql_ctx.sparkSession
        df_read = spark_session.read.parquet(output_path)
        assert df_read.count() == sample_data.count()

    def test_partitioned_write(self, sample_data, temp_dir):
        """測試分區寫入"""
        output_path = os.path.join(temp_dir, "partitioned_output")

        sample_data.write.mode("overwrite").partitionBy("job").parquet(output_path)

        # 檢查分區目錄是否創建
        assert os.path.exists(output_path)

        # 檢查是否可以讀取
        spark_session = sample_data.sql_ctx.sparkSession
        df_read = spark_session.read.parquet(output_path)
        assert df_read.count() == sample_data.count()


class TestDataFrameAdvanced:
    """測試 DataFrame 進階操作"""

    def test_window_functions(self, spark_session):
        """測試視窗函數"""
        from pyspark.sql.functions import dense_rank, rank, row_number
        from pyspark.sql.window import Window

        df = spark_session.createDataFrame(
            [
                ("Alice", "HR", 50000),
                ("Bob", "HR", 55000),
                ("Charlie", "IT", 60000),
                ("Diana", "IT", 65000),
            ],
            ["name", "department", "salary"],
        )

        window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

        df_with_rank = df.withColumn("rank", rank().over(window_spec))

        ranks = [row.rank for row in df_with_rank.collect()]
        assert 1 in ranks  # 每個部門都應該有排名1

    def test_pivot_operations(self, spark_session):
        """測試透視表操作"""
        df = spark_session.createDataFrame(
            [
                ("Alice", "Q1", 1000),
                ("Alice", "Q2", 1200),
                ("Bob", "Q1", 1100),
                ("Bob", "Q2", 1300),
            ],
            ["name", "quarter", "sales"],
        )

        pivot_df = df.groupBy("name").pivot("quarter").sum("sales")

        assert "Q1" in pivot_df.columns
        assert "Q2" in pivot_df.columns

    def test_user_defined_functions(self, spark_session):
        """測試使用者定義函數"""
        from pyspark.sql.functions import udf
        from pyspark.sql.types import IntegerType

        def string_length(s):
            return len(s) if s else 0

        length_udf = udf(string_length, IntegerType())

        df = spark_session.createDataFrame(
            [("Alice",), ("Bob",), ("Charlie",)], ["name"]
        )

        df_with_length = df.withColumn("name_length", length_udf(col("name")))

        lengths = [row.name_length for row in df_with_length.collect()]
        assert 5 in lengths  # Alice has 5 characters
        assert 3 in lengths  # Bob has 3 characters
        assert 7 in lengths  # Charlie has 7 characters

    def test_dataframe_explain(self, sample_data):
        """測試 DataFrame explain 操作"""
        # 這個測試主要確保 explain 方法不會出錯
        try:
            sample_data.explain()
            sample_data.explain(True)
            assert True
        except Exception as e:
            pytest.fail(f"DataFrame.explain() 失敗: {e}")

    def test_dataframe_cache(self, sample_data):
        """測試 DataFrame 緩存"""
        # 緩存 DataFrame
        cached_df = sample_data.cache()

        # 執行一些操作觸發緩存
        count1 = cached_df.count()
        count2 = cached_df.count()

        assert count1 == count2

        # 取消緩存
        cached_df.unpersist()

    def test_dataframe_checkpoint(self, sample_data, temp_dir):
        """測試 DataFrame 檢查點"""
        spark_session = sample_data.sql_ctx.sparkSession
        spark_session.sparkContext.setCheckpointDir(temp_dir)

        try:
            # 設置檢查點
            sample_data.checkpoint()

            # 檢查點後的操作
            count = sample_data.count()
            assert count == 4

        except Exception as e:
            # 某些 Spark 版本可能不支援檢查點
            pytest.skip(f"Checkpoint not supported: {e}")

    def test_dataframe_repartition(self, sample_data):
        """測試 DataFrame 重新分區"""
        original_partitions = sample_data.rdd.getNumPartitions()

        # 重新分區
        repartitioned_df = sample_data.repartition(2)
        assert repartitioned_df.rdd.getNumPartitions() == 2

        # 合併分區
        coalesced_df = sample_data.coalesce(1)
        assert coalesced_df.rdd.getNumPartitions() == 1

    def test_dataframe_broadcast(self, spark_session):
        """測試 DataFrame 廣播"""
        from pyspark.sql.functions import broadcast

        large_df = spark_session.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")], ["id", "name"]
        )

        small_df = spark_session.createDataFrame(
            [(1, "HR"), (2, "IT")], ["id", "department"]
        )

        # 使用廣播連接
        joined_df = large_df.join(broadcast(small_df), "id")

        assert joined_df.count() == 2  # 只有 id 1 和 2 匹配
