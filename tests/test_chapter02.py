"""
第2章：RDD 基本操作的測試
"""

import pytest
from pyspark import SparkContext


class TestRDDBasics:
    """測試 RDD 基本操作"""

    def test_rdd_creation(self, spark_context):
        """測試 RDD 創建"""
        data = [1, 2, 3, 4, 5]
        rdd = spark_context.parallelize(data)

        assert rdd.collect() == [1, 2, 3, 4, 5]
        assert rdd.count() == 5

    def test_rdd_partitions(self, spark_context):
        """測試 RDD 分區"""
        data = [1, 2, 3, 4, 5, 6, 7, 8]
        rdd = spark_context.parallelize(data, 4)

        assert rdd.getNumPartitions() == 4

    def test_rdd_map(self, sample_rdd):
        """測試 RDD map 操作"""
        squared_rdd = sample_rdd.map(lambda x: x**2)
        result = squared_rdd.collect()

        expected = [1, 4, 9, 16, 25, 36, 49, 64, 81, 100]
        assert result == expected

    def test_rdd_filter(self, sample_rdd):
        """測試 RDD filter 操作"""
        even_rdd = sample_rdd.filter(lambda x: x % 2 == 0)
        result = even_rdd.collect()

        expected = [2, 4, 6, 8, 10]
        assert result == expected

    def test_rdd_reduce(self, sample_rdd):
        """測試 RDD reduce 操作"""
        sum_result = sample_rdd.reduce(lambda x, y: x + y)
        assert sum_result == 55  # 1+2+...+10 = 55

    def test_rdd_take(self, sample_rdd):
        """測試 RDD take 操作"""
        first_three = sample_rdd.take(3)
        assert first_three == [1, 2, 3]

    def test_rdd_top(self, sample_rdd):
        """測試 RDD top 操作"""
        top_three = sample_rdd.top(3)
        assert top_three == [10, 9, 8]

    def test_rdd_distinct(self, spark_context):
        """測試 RDD distinct 操作"""
        data = [1, 2, 2, 3, 3, 3, 4, 4, 4, 4]
        rdd = spark_context.parallelize(data)
        unique_rdd = rdd.distinct()

        result = sorted(unique_rdd.collect())
        assert result == [1, 2, 3, 4]

    def test_rdd_flatmap(self, spark_context):
        """測試 RDD flatMap 操作"""
        data = ["hello world", "spark is great"]
        rdd = spark_context.parallelize(data)
        words_rdd = rdd.flatMap(lambda x: x.split())

        result = words_rdd.collect()
        expected = ["hello", "world", "spark", "is", "great"]
        assert result == expected

    def test_rdd_union(self, spark_context):
        """測試 RDD union 操作"""
        rdd1 = spark_context.parallelize([1, 2, 3])
        rdd2 = spark_context.parallelize([4, 5, 6])

        union_rdd = rdd1.union(rdd2)
        result = union_rdd.collect()

        assert len(result) == 6
        assert set(result) == {1, 2, 3, 4, 5, 6}

    def test_rdd_intersection(self, spark_context):
        """測試 RDD intersection 操作"""
        rdd1 = spark_context.parallelize([1, 2, 3, 4, 5])
        rdd2 = spark_context.parallelize([3, 4, 5, 6, 7])

        intersection_rdd = rdd1.intersection(rdd2)
        result = sorted(intersection_rdd.collect())

        assert result == [3, 4, 5]

    def test_rdd_subtract(self, spark_context):
        """測試 RDD subtract 操作"""
        rdd1 = spark_context.parallelize([1, 2, 3, 4, 5])
        rdd2 = spark_context.parallelize([3, 4, 5])

        subtract_rdd = rdd1.subtract(rdd2)
        result = sorted(subtract_rdd.collect())

        assert result == [1, 2]

    def test_rdd_sample(self, sample_rdd):
        """測試 RDD sample 操作"""
        sampled_rdd = sample_rdd.sample(False, 0.5, seed=42)
        result = sampled_rdd.collect()

        # 由於是隨機採樣，我們只檢查結果是否為原始數據的子集
        assert len(result) <= 10
        for item in result:
            assert item in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    def test_rdd_cartesian(self, spark_context):
        """測試 RDD cartesian 操作"""
        rdd1 = spark_context.parallelize([1, 2])
        rdd2 = spark_context.parallelize([3, 4])

        cartesian_rdd = rdd1.cartesian(rdd2)
        result = cartesian_rdd.collect()

        expected = [(1, 3), (1, 4), (2, 3), (2, 4)]
        assert sorted(result) == sorted(expected)

    def test_rdd_coalesce(self, spark_context):
        """測試 RDD coalesce 操作"""
        rdd = spark_context.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 4)
        assert rdd.getNumPartitions() == 4

        coalesced_rdd = rdd.coalesce(2)
        assert coalesced_rdd.getNumPartitions() == 2
        assert coalesced_rdd.collect() == [1, 2, 3, 4, 5, 6, 7, 8]

    def test_rdd_repartition(self, spark_context):
        """測試 RDD repartition 操作"""
        rdd = spark_context.parallelize([1, 2, 3, 4, 5, 6, 7, 8], 2)
        assert rdd.getNumPartitions() == 2

        repartitioned_rdd = rdd.repartition(4)
        assert repartitioned_rdd.getNumPartitions() == 4
        assert sorted(repartitioned_rdd.collect()) == [1, 2, 3, 4, 5, 6, 7, 8]


class TestPairRDDOperations:
    """測試鍵值對 RDD 操作"""

    def test_pair_rdd_creation(self, spark_context):
        """測試鍵值對 RDD 創建"""
        data = [("a", 1), ("b", 2), ("c", 3)]
        pair_rdd = spark_context.parallelize(data)

        assert pair_rdd.collect() == [("a", 1), ("b", 2), ("c", 3)]

    def test_reduce_by_key(self, spark_context):
        """測試 reduceByKey 操作"""
        data = [("a", 1), ("b", 2), ("a", 3), ("b", 4)]
        pair_rdd = spark_context.parallelize(data)

        result_rdd = pair_rdd.reduceByKey(lambda x, y: x + y)
        result = dict(result_rdd.collect())

        assert result == {"a": 4, "b": 6}

    def test_group_by_key(self, spark_context):
        """測試 groupByKey 操作"""
        data = [("a", 1), ("b", 2), ("a", 3), ("b", 4)]
        pair_rdd = spark_context.parallelize(data)

        grouped_rdd = pair_rdd.groupByKey()
        result = grouped_rdd.mapValues(list).collect()
        result_dict = dict(result)

        assert sorted(result_dict["a"]) == [1, 3]
        assert sorted(result_dict["b"]) == [2, 4]

    def test_map_values(self, spark_context):
        """測試 mapValues 操作"""
        data = [("a", 1), ("b", 2), ("c", 3)]
        pair_rdd = spark_context.parallelize(data)

        result_rdd = pair_rdd.mapValues(lambda x: x * 2)
        result = result_rdd.collect()

        expected = [("a", 2), ("b", 4), ("c", 6)]
        assert result == expected

    def test_keys_values(self, spark_context):
        """測試 keys 和 values 操作"""
        data = [("a", 1), ("b", 2), ("c", 3)]
        pair_rdd = spark_context.parallelize(data)

        keys = pair_rdd.keys().collect()
        values = pair_rdd.values().collect()

        assert keys == ["a", "b", "c"]
        assert values == [1, 2, 3]

    def test_sort_by_key(self, spark_context):
        """測試 sortByKey 操作"""
        data = [("c", 3), ("a", 1), ("b", 2)]
        pair_rdd = spark_context.parallelize(data)

        sorted_rdd = pair_rdd.sortByKey()
        result = sorted_rdd.collect()

        expected = [("a", 1), ("b", 2), ("c", 3)]
        assert result == expected

    def test_join(self, spark_context):
        """測試 join 操作"""
        rdd1 = spark_context.parallelize([("a", 1), ("b", 2)])
        rdd2 = spark_context.parallelize([("a", 3), ("c", 4)])

        joined_rdd = rdd1.join(rdd2)
        result = joined_rdd.collect()

        assert result == [("a", (1, 3))]

    def test_left_outer_join(self, spark_context):
        """測試 leftOuterJoin 操作"""
        rdd1 = spark_context.parallelize([("a", 1), ("b", 2)])
        rdd2 = spark_context.parallelize([("a", 3), ("c", 4)])

        joined_rdd = rdd1.leftOuterJoin(rdd2)
        result = dict(joined_rdd.collect())

        assert result["a"] == (1, 3)
        assert result["b"] == (2, None)

    def test_cogroup(self, spark_context):
        """測試 cogroup 操作"""
        rdd1 = spark_context.parallelize([("a", 1), ("b", 2)])
        rdd2 = spark_context.parallelize([("a", 3), ("c", 4)])

        cogrouped_rdd = rdd1.cogroup(rdd2)
        result = cogrouped_rdd.mapValues(lambda x: (list(x[0]), list(x[1]))).collect()
        result_dict = dict(result)

        assert result_dict["a"] == ([1], [3])
        assert result_dict["b"] == ([2], [])
        assert result_dict["c"] == ([], [4])


class TestRDDActions:
    """測試 RDD Action 操作"""

    def test_collect(self, sample_rdd):
        """測試 collect 操作"""
        result = sample_rdd.collect()
        assert result == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    def test_count(self, sample_rdd):
        """測試 count 操作"""
        count = sample_rdd.count()
        assert count == 10

    def test_first(self, sample_rdd):
        """測試 first 操作"""
        first = sample_rdd.first()
        assert first == 1

    def test_foreach(self, sample_rdd):
        """測試 foreach 操作"""
        # foreach 沒有返回值，我們通過副作用來測試
        results = []

        def append_to_results(x):
            results.append(x)

        # 注意：foreach 在分散式環境中可能無法正常工作
        # 這裡我們測試它不會拋出異常
        try:
            sample_rdd.foreach(append_to_results)
            assert True
        except Exception as e:
            pytest.fail(f"foreach 操作失敗: {e}")

    def test_stats(self, sample_rdd):
        """測試 stats 操作"""
        stats = sample_rdd.stats()

        assert stats.count() == 10
        assert stats.mean() == 5.5
        assert stats.min() == 1
        assert stats.max() == 10
        assert stats.sum() == 55
