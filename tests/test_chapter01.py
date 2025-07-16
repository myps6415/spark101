"""
第1章：Spark 基礎概念的測試
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


class TestSparkBasics:
    """測試 Spark 基礎概念"""

    def test_spark_session_creation(self, spark_session):
        """測試 SparkSession 創建"""
        assert spark_session is not None
        assert isinstance(spark_session, SparkSession)
        assert spark_session.sparkContext.appName == "TestSpark"

    def test_dataframe_creation(self, spark_session):
        """測試 DataFrame 創建"""
        data = [("Alice", 25), ("Bob", 30)]
        columns = ["name", "age"]
        df = spark_session.createDataFrame(data, columns)

        assert df.count() == 2
        assert df.columns == ["name", "age"]

    def test_dataframe_show(self, sample_data):
        """測試 DataFrame show 方法"""
        # 這個測試主要確保 show() 方法不會出錯
        try:
            sample_data.show()
            assert True
        except Exception as e:
            pytest.fail(f"DataFrame.show() 失敗: {e}")

    def test_dataframe_schema(self, sample_data):
        """測試 DataFrame schema"""
        schema = sample_data.schema
        field_names = [field.name for field in schema.fields]

        assert "name" in field_names
        assert "age" in field_names
        assert "job" in field_names
        assert "salary" in field_names

    def test_dataframe_filter(self, sample_data):
        """測試 DataFrame 過濾操作"""
        filtered_df = sample_data.filter(col("age") > 28)
        result = filtered_df.collect()

        assert len(result) == 2  # Bob 和 Charlie
        names = [row.name for row in result]
        assert "Bob" in names
        assert "Charlie" in names

    def test_dataframe_select(self, sample_data):
        """測試 DataFrame 選擇操作"""
        selected_df = sample_data.select("name", "age")
        columns = selected_df.columns

        assert len(columns) == 2
        assert "name" in columns
        assert "age" in columns
        assert "job" not in columns

    def test_dataframe_with_column(self, sample_data):
        """測試 DataFrame 添加列操作"""
        df_with_group = sample_data.withColumn(
            "age_group", when(col("age") < 30, "Young").otherwise("Senior")
        )

        columns = df_with_group.columns
        assert "age_group" in columns

        # 檢查age_group的值
        young_count = df_with_group.filter(col("age_group") == "Young").count()
        senior_count = df_with_group.filter(col("age_group") == "Senior").count()

        assert young_count == 2  # Alice 和 Diana
        assert senior_count == 2  # Bob 和 Charlie

    def test_dataframe_count(self, sample_data):
        """測試 DataFrame 計數操作"""
        count = sample_data.count()
        assert count == 4

    def test_dataframe_collect(self, sample_data):
        """測試 DataFrame collect 操作"""
        rows = sample_data.collect()
        assert len(rows) == 4

        # 檢查第一行數據
        first_row = rows[0]
        assert first_row.name == "Alice"
        assert first_row.age == 25

    def test_dataframe_to_pandas(self, sample_data):
        """測試 DataFrame 轉換為 Pandas"""
        pandas_df = sample_data.toPandas()

        assert len(pandas_df) == 4
        assert list(pandas_df.columns) == ["name", "age", "job", "salary"]
        assert pandas_df.iloc[0]["name"] == "Alice"

    def test_dataframe_describe(self, sample_data):
        """測試 DataFrame 統計描述"""
        desc_df = sample_data.describe()

        # 檢查描述性統計的基本結構
        assert desc_df.count() == 5  # count, mean, stddev, min, max
        summary_stats = [row.summary for row in desc_df.collect()]
        assert "count" in summary_stats
        assert "mean" in summary_stats
        assert "stddev" in summary_stats
        assert "min" in summary_stats
        assert "max" in summary_stats

    def test_dataframe_groupby(self, sample_data):
        """測試 DataFrame 分組操作"""
        grouped_df = sample_data.groupBy("job").count()
        result = grouped_df.collect()

        # 每個職位都只有一個人
        assert len(result) == 4
        for row in result:
            assert row.count == 1

    def test_dataframe_orderby(self, sample_data):
        """測試 DataFrame 排序操作"""
        ordered_df = sample_data.orderBy("age")
        ages = [row.age for row in ordered_df.collect()]

        assert ages == [25, 28, 30, 35]  # 應該是升序排列

    def test_dataframe_union(self, spark_session):
        """測試 DataFrame 聯集操作"""
        df1 = spark_session.createDataFrame([("Alice", 25)], ["name", "age"])
        df2 = spark_session.createDataFrame([("Bob", 30)], ["name", "age"])

        union_df = df1.union(df2)
        assert union_df.count() == 2

        names = [row.name for row in union_df.collect()]
        assert "Alice" in names
        assert "Bob" in names
