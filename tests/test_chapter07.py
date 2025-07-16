"""
第7章：性能調優的測試
"""

import os
import tempfile
import time

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql.functions import (broadcast, col, lit,
                                   monotonically_increasing_id,
                                   spark_partition_id, when)
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)
from pyspark.storagelevel import StorageLevel


class TestPartitioning:
    """測試分區策略"""

    def test_default_partitioning(self, spark_session):
        """測試默認分區"""
        # 創建測試數據
        data = [(i, f"name_{i}", i * 100) for i in range(100)]
        df = spark_session.createDataFrame(data, ["id", "name", "value"])

        # 檢查默認分區數
        default_partitions = df.rdd.getNumPartitions()
        assert default_partitions > 0

        # 檢查數據分佈
        partition_sizes = df.rdd.mapPartitions(
            lambda iterator: [len(list(iterator))]
        ).collect()
        assert sum(partition_sizes) == 100

    def test_repartition_operations(self, spark_session):
        """測試重新分區操作"""
        # 創建測試數據
        data = [(i, f"category_{i % 5}", i * 10) for i in range(50)]
        df = spark_session.createDataFrame(data, ["id", "category", "value"])

        original_partitions = df.rdd.getNumPartitions()

        # 增加分區數
        repartitioned_df = df.repartition(8)
        assert repartitioned_df.rdd.getNumPartitions() == 8
        assert repartitioned_df.count() == 50  # 數據完整性

        # 減少分區數
        coalesced_df = df.coalesce(2)
        assert coalesced_df.rdd.getNumPartitions() == min(2, original_partitions)
        assert coalesced_df.count() == 50

    def test_hash_partitioning(self, spark_session):
        """測試哈希分區"""
        # 創建測試數據
        data = [(i, f"category_{i % 5}", i * 10) for i in range(100)]
        df = spark_session.createDataFrame(data, ["id", "category", "value"])

        # 按category列進行哈希分區
        hash_partitioned_df = df.repartition(5, "category")

        # 檢查分區數
        assert hash_partitioned_df.rdd.getNumPartitions() == 5

        # 檢查相同category的數據是否在同一分區
        # 添加分區ID列
        with_partition_id = hash_partitioned_df.withColumn(
            "partition_id", spark_partition_id()
        )

        # 檢查每個category的分區分佈
        partition_distribution = (
            with_partition_id.groupBy("category", "partition_id").count().collect()
        )

        # 每個category應該只出現在一個分區中
        category_partitions = {}
        for row in partition_distribution:
            category = row.category
            partition_id = row.partition_id
            if category not in category_partitions:
                category_partitions[category] = set()
            category_partitions[category].add(partition_id)

        # 每個category應該只在一個分區中
        for category, partitions in category_partitions.items():
            assert (
                len(partitions) == 1
            ), f"Category {category} spans multiple partitions: {partitions}"

    def test_range_partitioning(self, spark_session):
        """測試範圍分區"""
        # 創建測試數據
        data = [(i, f"name_{i}", i * 10) for i in range(100)]
        df = spark_session.createDataFrame(data, ["id", "name", "value"])

        # 按id進行範圍分區（通過排序實現）
        sorted_df = df.orderBy("id").repartition(4)

        # 檢查分區數
        assert sorted_df.rdd.getNumPartitions() == 4

        # 檢查數據完整性
        assert sorted_df.count() == 100

    def test_partition_pruning(self, spark_session, temp_dir):
        """測試分區裁剪"""
        # 創建分區數據
        data = []
        for year in [2022, 2023, 2024]:
            for month in range(1, 13):
                for day in range(1, 11):  # 每月10天
                    data.append(
                        (f"{year}-{month:02d}-{day:02d}", year, month, day, day * 100)
                    )

        df = spark_session.createDataFrame(
            data, ["date", "year", "month", "day", "sales"]
        )

        # 寫入分區表
        partitioned_path = os.path.join(temp_dir, "partitioned_sales")
        df.write.mode("overwrite").partitionBy("year", "month").parquet(
            partitioned_path
        )

        # 讀取分區表
        partitioned_df = spark_session.read.parquet(partitioned_path)

        # 測試分區裁剪 - 只查詢2023年的數據
        filtered_df = partitioned_df.filter(col("year") == 2023)

        # 檢查結果
        assert filtered_df.count() == 120  # 12個月 * 10天

        # 測試多條件分區裁剪
        specific_df = partitioned_df.filter((col("year") == 2023) & (col("month") == 6))
        assert specific_df.count() == 10  # 6月的10天


class TestCaching:
    """測試緩存策略"""

    def test_memory_caching(self, spark_session):
        """測試內存緩存"""
        # 創建測試數據
        data = [(i, f"name_{i}", i * 100) for i in range(1000)]
        df = spark_session.createDataFrame(data, ["id", "name", "value"])

        # 緩存到內存
        cached_df = df.cache()

        # 觸發緩存（執行action）
        count1 = cached_df.count()

        # 檢查是否緩存
        assert cached_df.is_cached

        # 再次執行action，應該從緩存讀取
        count2 = cached_df.count()
        assert count1 == count2

        # 清理緩存
        cached_df.unpersist()
        assert not cached_df.is_cached

    def test_disk_caching(self, spark_session):
        """測試磁盤緩存"""
        # 創建較大的測試數據
        data = [(i, f"data_{i}", i * 0.5) for i in range(5000)]
        df = spark_session.createDataFrame(data, ["id", "data", "value"])

        # 緩存到磁盤
        disk_cached_df = df.persist(StorageLevel.DISK_ONLY)

        # 觸發緩存
        initial_count = disk_cached_df.count()

        # 檢查緩存狀態
        assert disk_cached_df.is_cached

        # 執行轉換和action
        filtered_df = disk_cached_df.filter(col("value") > 1000)
        filtered_count = filtered_df.count()

        assert filtered_count < initial_count

        # 清理緩存
        disk_cached_df.unpersist()

    def test_memory_and_disk_caching(self, spark_session):
        """測試內存+磁盤緩存"""
        # 創建測試數據
        data = [(i, f"record_{i}", i * 10.5) for i in range(2000)]
        df = spark_session.createDataFrame(data, ["id", "record", "score"])

        # 使用內存+磁盤緩存
        cached_df = df.persist(StorageLevel.MEMORY_AND_DISK)

        # 執行多個操作
        total_count = cached_df.count()
        avg_score = cached_df.agg({"score": "avg"}).collect()[0][0]
        max_score = cached_df.agg({"score": "max"}).collect()[0][0]

        assert total_count == 2000
        assert avg_score > 0
        assert max_score > avg_score

        cached_df.unpersist()

    def test_checkpoint(self, spark_session, temp_dir):
        """測試檢查點"""
        # 設置檢查點目錄
        checkpoint_dir = os.path.join(temp_dir, "checkpoint")
        spark_session.sparkContext.setCheckpointDir(checkpoint_dir)

        # 創建測試數據
        data = [(i, f"item_{i}", i % 10) for i in range(1000)]
        df = spark_session.createDataFrame(data, ["id", "item", "category"])

        # 進行複雜的轉換
        transformed_df = (
            df.filter(col("category") < 5)
            .withColumn("processed", col("id") * 2)
            .filter(col("processed") > 100)
        )

        # 設置檢查點
        transformed_df.checkpoint()

        # 觸發檢查點
        count = transformed_df.count()

        # 檢查檢查點目錄是否創建
        assert os.path.exists(checkpoint_dir)

        # 繼續使用檢查點後的DataFrame
        final_result = transformed_df.filter(col("category") == 1).count()
        assert final_result <= count


class TestJoinOptimization:
    """測試連接優化"""

    def test_broadcast_join(self, spark_session):
        """測試廣播連接"""
        # 創建大表
        large_data = [(i, f"user_{i}", i % 100) for i in range(10000)]
        large_df = spark_session.createDataFrame(
            large_data, ["user_id", "username", "department_id"]
        )

        # 創建小表
        small_data = [(i, f"dept_{i}") for i in range(100)]
        small_df = spark_session.createDataFrame(small_data, ["dept_id", "dept_name"])

        # 使用廣播連接
        broadcast_joined = large_df.join(
            broadcast(small_df), large_df.department_id == small_df.dept_id
        )

        # 執行連接
        result_count = broadcast_joined.count()
        assert result_count == 10000

        # 檢查結果包含兩個表的列
        columns = broadcast_joined.columns
        assert "username" in columns
        assert "dept_name" in columns

    def test_sort_merge_join(self, spark_session):
        """測試排序合並連接"""
        # 創建兩個相對較大的表
        table1_data = [(i, f"record_{i}", i % 1000) for i in range(5000)]
        table1 = spark_session.createDataFrame(table1_data, ["id", "data", "key"])

        table2_data = [(i, f"info_{i}", i * 2) for i in range(1000)]
        table2 = spark_session.createDataFrame(table2_data, ["key", "info", "value"])

        # 執行連接（默認會選擇適當的連接策略）
        joined_df = table1.join(table2, "key")

        # 檢查結果
        result_count = joined_df.count()
        assert result_count == 5000  # 每個key都能找到匹配

        # 檢查數據完整性
        sample_row = joined_df.first()
        assert sample_row.key is not None
        assert sample_row.data is not None
        assert sample_row.info is not None

    def test_bucket_join(self, spark_session, temp_dir):
        """測試桶連接"""
        # 創建測試數據
        orders_data = [(i, i % 100, f"order_{i}", i * 10) for i in range(1000)]
        orders_df = spark_session.createDataFrame(
            orders_data, ["order_id", "customer_id", "order_desc", "amount"]
        )

        customers_data = [(i, f"customer_{i}", f"city_{i % 20}") for i in range(100)]
        customers_df = spark_session.createDataFrame(
            customers_data, ["customer_id", "customer_name", "city"]
        )

        # 寫入桶表
        orders_path = os.path.join(temp_dir, "orders_bucketed")
        customers_path = os.path.join(temp_dir, "customers_bucketed")

        orders_df.write.mode("overwrite").bucketBy(10, "customer_id").saveAsTable(
            "bucketed_orders"
        )

        customers_df.write.mode("overwrite").bucketBy(10, "customer_id").saveAsTable(
            "bucketed_customers"
        )

        # 讀取桶表並連接
        bucketed_orders = spark_session.table("bucketed_orders")
        bucketed_customers = spark_session.table("bucketed_customers")

        # 執行桶連接
        bucket_joined = bucketed_orders.join(bucketed_customers, "customer_id")

        # 檢查結果
        result_count = bucket_joined.count()
        assert result_count == 1000

    def test_join_reordering(self, spark_session):
        """測試連接重排序"""
        # 創建三個表進行多表連接
        table_a = spark_session.createDataFrame(
            [(i, f"a_{i}") for i in range(100)], ["id", "data_a"]
        )

        table_b = spark_session.createDataFrame(
            [(i, f"b_{i}", i % 50) for i in range(100)], ["id", "data_b", "group_id"]
        )

        table_c = spark_session.createDataFrame(
            [(i, f"c_{i}") for i in range(50)], ["group_id", "data_c"]
        )

        # 多表連接
        result = table_a.join(table_b, "id").join(table_c, "group_id")

        # 檢查結果
        assert result.count() == 100

        # 檢查包含所有表的數據
        sample = result.first()
        assert sample.data_a is not None
        assert sample.data_b is not None
        assert sample.data_c is not None


class TestResourceManagement:
    """測試資源管理"""

    def test_executor_memory_usage(self, spark_session):
        """測試執行器內存使用"""
        # 創建內存密集型操作
        large_data = [
            (i, f"data_{i}" * 100, [j for j in range(100)]) for i in range(1000)
        ]
        df = spark_session.createDataFrame(large_data, ["id", "text", "numbers"])

        # 執行內存密集型操作
        result = df.filter(col("id") % 2 == 0).select("id", "text").distinct().count()

        assert result == 500  # 一半的記錄

    def test_task_parallelism(self, spark_session):
        """測試任務並行度"""
        # 創建測試數據
        data = [(i, i * 2, i * 3) for i in range(10000)]
        df = spark_session.createDataFrame(data, ["a", "b", "c"])

        # 設置不同的分區數來測試並行度
        partitioned_df = df.repartition(8)

        # 執行並行操作
        result = (
            partitioned_df.filter(col("a") > 5000)
            .withColumn("sum", col("a") + col("b") + col("c"))
            .count()
        )

        assert result < 10000  # 過濾後的結果
        assert partitioned_df.rdd.getNumPartitions() == 8

    def test_dynamic_allocation(self, spark_session):
        """測試動態資源分配（模擬）"""
        # 創建變化的工作負載
        small_data = [(i, i * 2) for i in range(100)]
        small_df = spark_session.createDataFrame(small_data, ["id", "value"])

        large_data = [(i, i * 2) for i in range(10000)]
        large_df = spark_session.createDataFrame(large_data, ["id", "value"])

        # 小工作負載
        small_result = small_df.count()
        assert small_result == 100

        # 大工作負載
        large_result = large_df.groupBy().sum("value").collect()[0][0]
        assert large_result > small_result


class TestQueryOptimization:
    """測試查詢優化"""

    def test_predicate_pushdown(self, spark_session, temp_dir):
        """測試謂詞下推"""
        # 創建測試數據
        data = [(i, f"category_{i % 10}", i * 100, i % 2 == 0) for i in range(10000)]
        df = spark_session.createDataFrame(data, ["id", "category", "value", "active"])

        # 寫入Parquet文件
        parquet_path = os.path.join(temp_dir, "test_data.parquet")
        df.write.mode("overwrite").parquet(parquet_path)

        # 讀取並應用過濾器
        read_df = spark_session.read.parquet(parquet_path)
        filtered_df = read_df.filter((col("active") == True) & (col("value") > 5000))

        # 執行查詢
        result_count = filtered_df.count()
        assert result_count < 10000  # 過濾後的結果

        # 檢查過濾條件
        all_active = all(row.active for row in filtered_df.collect())
        all_high_value = all(row.value > 5000 for row in filtered_df.collect())
        assert all_active
        assert all_high_value

    def test_column_pruning(self, spark_session):
        """測試列裁剪"""
        # 創建多列數據
        data = [
            (
                i,
                f"name_{i}",
                f"email_{i}@test.com",
                i * 100,
                i % 10,
                f"address_{i}",
                f"phone_{i}",
                f"comment_{i}",
            )
            for i in range(1000)
        ]

        columns = [
            "id",
            "name",
            "email",
            "salary",
            "department",
            "address",
            "phone",
            "comments",
        ]
        df = spark_session.createDataFrame(data, columns)

        # 只選擇需要的列
        selected_df = df.select("id", "name", "salary")

        # 檢查列數
        assert len(selected_df.columns) == 3
        assert "email" not in selected_df.columns
        assert "address" not in selected_df.columns

        # 執行操作
        result = selected_df.filter(col("salary") > 50000).count()
        assert result < 1000

    def test_constant_folding(self, spark_session):
        """測試常量折疊"""
        # 創建測試數據
        data = [(i, i * 2, i * 3) for i in range(100)]
        df = spark_session.createDataFrame(data, ["a", "b", "c"])

        # 使用常量表達式
        result_df = (
            df.withColumn("constant_calc", lit(10) + lit(20))
            .withColumn("mixed_calc", col("a") + lit(100))
            .filter(lit(True))
        )  # 常量過濾條件

        # 檢查結果
        assert result_df.count() == 100

        # 檢查常量計算結果
        first_row = result_df.first()
        assert first_row.constant_calc == 30  # 10 + 20
        assert first_row.mixed_calc == first_row.a + 100

    def test_join_reordering_cost_based(self, spark_session):
        """測試基於成本的連接重排序"""
        # 創建不同大小的表
        small_table = spark_session.createDataFrame(
            [(i, f"small_{i}") for i in range(10)], ["id", "data_small"]
        )

        medium_table = spark_session.createDataFrame(
            [(i, f"medium_{i}", i % 10) for i in range(100)],
            ["id", "data_medium", "small_id"],
        )

        large_table = spark_session.createDataFrame(
            [(i, f"large_{i}", i % 100) for i in range(1000)],
            ["id", "data_large", "medium_id"],
        )

        # 多表連接 - Spark應該優化連接順序
        result = large_table.join(
            medium_table, large_table.medium_id == medium_table.id
        ).join(small_table, medium_table.small_id == small_table.id)

        # 檢查結果
        count = result.count()
        assert count == 1000  # 所有大表記錄都應該有匹配

        # 檢查包含所有表的數據
        sample = result.first()
        assert sample.data_small is not None
        assert sample.data_medium is not None
        assert sample.data_large is not None


class TestPerformanceMonitoring:
    """測試性能監控"""

    def test_query_execution_time(self, spark_session):
        """測試查詢執行時間"""
        # 創建測試數據
        data = [(i, i * 2, f"data_{i}") for i in range(10000)]
        df = spark_session.createDataFrame(data, ["id", "value", "text"])

        # 測量執行時間
        start_time = time.time()

        result = df.filter(col("value") > 5000).groupBy("id").count().count()

        end_time = time.time()
        execution_time = end_time - start_time

        # 檢查結果和執行時間
        assert result > 0
        assert execution_time > 0
        # 不設置具體的時間閾值，因為它依賴於硬件

    def test_memory_usage_monitoring(self, spark_session):
        """測試內存使用監控"""
        # 創建內存密集型操作
        large_data = [
            (i, f"text_{i}" * 1000, [j for j in range(100)]) for i in range(1000)
        ]
        df = spark_session.createDataFrame(large_data, ["id", "large_text", "numbers"])

        # 緩存並觸發內存使用
        cached_df = df.cache()
        count = cached_df.count()

        # 檢查緩存狀態
        assert cached_df.is_cached
        assert count == 1000

        # 執行額外操作
        filtered_count = cached_df.filter(col("id") > 500).count()
        assert filtered_count < count

        # 清理緩存
        cached_df.unpersist()

    def test_task_metrics(self, spark_session):
        """測試任務指標"""
        # 創建測試數據
        data = [(i, i % 100, i * 1.5) for i in range(5000)]
        df = spark_session.createDataFrame(data, ["id", "group", "value"])

        # 執行複雜操作
        result = (
            df.groupBy("group")
            .agg({"value": "avg", "id": "count"})
            .filter(col("avg(value)") > 100)
            .count()
        )

        # 檢查結果
        assert result > 0
        assert result <= 100  # 最多100個組

    def test_explain_plan(self, spark_session):
        """測試執行計劃分析"""
        # 創建測試數據
        data = [(i, f"category_{i % 10}", i * 100) for i in range(1000)]
        df = spark_session.createDataFrame(data, ["id", "category", "value"])

        # 創建查詢
        query = (
            df.filter(col("value") > 50000).groupBy("category").count().orderBy("count")
        )

        # 檢查執行計劃（不會拋出異常即可）
        try:
            query.explain()
            query.explain(True)  # 詳細執行計劃
            assert True
        except Exception as e:
            pytest.fail(f"執行計劃分析失敗: {e}")

        # 執行查詢檢查結果
        result_count = query.count()
        assert result_count <= 10  # 最多10個類別
