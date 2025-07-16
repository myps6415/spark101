#!/usr/bin/env python3
"""
第7章練習3：緩存和持久化策略
學習如何有效使用緩存和持久化來提高重複計算的性能
"""

import gc
import os
import random
import time
from datetime import datetime, timedelta

import psutil
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (array, asc, avg, col, collect_list, count,
                                   current_timestamp, desc, explode, expr, lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import rand
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import stddev, struct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import unix_timestamp, when
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               StringType, StructField, StructType,
                               TimestampType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("緩存和持久化策略練習")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        .getOrCreate()
    )

    print("=== 第7章練習3：緩存和持久化策略 ===")

    # 1. 緩存基礎概念
    print("\n1. 緩存基礎概念和監控:")

    # 1.1 內存使用監控函數
    print("\n1.1 內存監控工具:")

    def monitor_memory_usage():
        """監控系統內存使用情況"""
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()

        print(f"進程內存使用: {memory_info.rss / 1024 / 1024:.2f} MB")
        print(f"虛擬內存: {memory_info.vms / 1024 / 1024:.2f} MB")

        # 系統內存
        system_memory = psutil.virtual_memory()
        print(f"系統內存使用率: {system_memory.percent:.1f}%")
        print(f"可用內存: {system_memory.available / 1024 / 1024:.2f} MB")

        return memory_info.rss / 1024 / 1024

    def get_spark_storage_info():
        """獲取 Spark 存儲信息"""
        storage_status = spark.sparkContext.statusTracker().getExecutorInfos()

        total_memory = 0
        used_memory = 0

        for executor in storage_status:
            total_memory += executor.totalOnHeapStorageMemory
            used_memory += executor.memoryUsed

        print(f"Spark 總存儲內存: {total_memory / 1024 / 1024:.2f} MB")
        print(f"Spark 已用存儲內存: {used_memory / 1024 / 1024:.2f} MB")
        print(
            f"Spark 內存使用率: {(used_memory / total_memory * 100) if total_memory > 0 else 0:.1f}%"
        )

        return total_memory, used_memory

    # 初始內存狀態
    print("初始內存狀態:")
    initial_memory = monitor_memory_usage()
    initial_spark_total, initial_spark_used = get_spark_storage_info()

    # 1.2 創建測試數據
    print("\n1.2 創建計算密集型數據集:")

    def generate_complex_dataset(num_records=500000):
        """生成計算密集型數據集"""
        data = []
        for i in range(num_records):
            data.append(
                (
                    i,
                    f"user_{random.randint(1, 50000)}",
                    random.choice(["A", "B", "C", "D", "E"]),
                    random.randint(1, 100),
                    random.uniform(10, 10000),
                    random.choice(["US", "CN", "JP", "UK", "DE", "FR", "IT", "ES"]),
                    datetime.now() - timedelta(days=random.randint(1, 365)),
                    random.choice([True, False]),
                )
            )

        columns = [
            "id",
            "user_name",
            "category",
            "score",
            "amount",
            "country",
            "timestamp",
            "is_active",
        ]
        return spark.createDataFrame(data, columns)

    def create_complex_transformations(df):
        """創建複雜的數據轉換"""
        return (
            df.withColumn("computed_score", col("score") * col("amount") / 100)
            .withColumn(
                "risk_level",
                when(col("computed_score") > 500, "High")
                .when(col("computed_score") > 200, "Medium")
                .otherwise("Low"),
            )
            .withColumn("year", expr("year(timestamp)"))
            .withColumn("month", expr("month(timestamp)"))
            .filter(col("amount") > 50)
            .groupBy("category", "country", "risk_level", "year")
            .agg(
                count("*").alias("count"),
                avg("computed_score").alias("avg_score"),
                spark_sum("amount").alias("total_amount"),
                max("score").alias("max_score"),
                min("score").alias("min_score"),
                stddev("amount").alias("amount_stddev"),
            )
            .filter(col("count") > 5)
        )

    # 創建基礎數據集
    base_df = generate_complex_dataset(500000)
    complex_df = create_complex_transformations(base_df)

    print(f"基礎數據集記錄數: {base_df.count()}")
    print("複雜轉換已定義（Lazy Evaluation）")

    # 2. 存儲級別比較
    print("\n2. 不同存儲級別性能比較:")

    # 2.1 存儲級別測試
    print("\n2.1 存儲級別性能測試:")

    def cache_performance_test():
        """測試不同存儲級別的性能"""

        storage_levels = [
            ("無緩存", None),
            ("MEMORY_ONLY", StorageLevel.MEMORY_ONLY),
            ("MEMORY_AND_DISK", StorageLevel.MEMORY_AND_DISK),
            ("MEMORY_ONLY_SER", StorageLevel.MEMORY_ONLY_SER),
            ("MEMORY_AND_DISK_SER", StorageLevel.MEMORY_AND_DISK_SER),
            ("DISK_ONLY", StorageLevel.DISK_ONLY),
            ("MEMORY_ONLY_2", StorageLevel.MEMORY_ONLY_2),
            ("MEMORY_AND_DISK_2", StorageLevel.MEMORY_AND_DISK_2),
        ]

        results = []

        for name, storage_level in storage_levels:
            print(f"\n測試存儲級別: {name}")

            # 清理緩存
            spark.catalog.clearCache()
            gc.collect()

            # 獲取起始內存
            start_memory = monitor_memory_usage()
            start_spark_total, start_spark_used = get_spark_storage_info()

            # 應用緩存策略
            if storage_level:
                cached_df = complex_df.persist(storage_level)
            else:
                cached_df = complex_df

            # 首次執行（觸發緩存）
            start_time = time.time()
            first_result = cached_df.collect()
            first_execution_time = time.time() - start_time

            # 獲取緩存後內存
            cache_memory = monitor_memory_usage()
            cache_spark_total, cache_spark_used = get_spark_storage_info()

            # 第二次執行（使用緩存）
            start_time = time.time()
            second_result = cached_df.collect()
            second_execution_time = time.time() - start_time

            # 第三次執行（確保穩定性）
            start_time = time.time()
            third_result = cached_df.collect()
            third_execution_time = time.time() - start_time

            # 計算改善率
            improvement = (
                (
                    (first_execution_time - second_execution_time)
                    / first_execution_time
                    * 100
                )
                if first_execution_time > 0
                else 0
            )

            result_info = {
                "storage_level": name,
                "first_time": first_execution_time,
                "second_time": second_execution_time,
                "third_time": third_execution_time,
                "improvement_percent": improvement,
                "memory_increase": cache_memory - start_memory,
                "spark_memory_increase": cache_spark_used - start_spark_used,
                "result_count": len(first_result),
            }

            results.append(result_info)

            print(f"首次執行: {first_execution_time:.2f}s")
            print(f"第二次執行: {second_execution_time:.2f}s")
            print(f"第三次執行: {third_execution_time:.2f}s")
            print(f"性能改善: {improvement:.1f}%")
            print(f"內存增加: {cache_memory - start_memory:.2f} MB")
            print(f"結果記錄數: {len(first_result)}")

            # 清理當前緩存
            if storage_level:
                cached_df.unpersist()

        return results

    cache_results = cache_performance_test()

    # 2.2 存儲級別比較總結
    print("\n2.2 存儲級別性能總結:")

    def analyze_cache_results(results):
        """分析緩存性能結果"""

        print("存儲級別性能比較:")
        print("存儲級別\t\t首次(s)\t二次(s)\t改善率(%)\t內存增加(MB)")
        print("-" * 70)

        for result in results:
            print(
                f"{result['storage_level']:<20}\t{result['first_time']:.2f}\t{result['second_time']:.2f}\t{result['improvement_percent']:.1f}\t\t{result['memory_increase']:.2f}"
            )

        # 找出最佳存儲級別
        valid_results = [r for r in results if r["improvement_percent"] > 0]
        if valid_results:
            best_performance = max(
                valid_results, key=lambda x: x["improvement_percent"]
            )
            best_memory = min(valid_results, key=lambda x: x["memory_increase"])

            print(
                f"\n最佳性能改善: {best_performance['storage_level']} ({best_performance['improvement_percent']:.1f}%)"
            )
            print(
                f"最少內存使用: {best_memory['storage_level']} ({best_memory['memory_increase']:.2f} MB)"
            )

    analyze_cache_results(cache_results)

    # 3. 智能緩存策略
    print("\n3. 智能緩存策略:")

    # 3.1 基於使用頻率的緩存
    print("\n3.1 基於使用頻率的智能緩存:")

    class SmartCacheManager:
        """智能緩存管理器"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.cached_dataframes = {}
            self.usage_counts = {}
            self.cache_sizes = {}
            self.last_access = {}

        def cache_with_strategy(self, df, name, strategy="auto"):
            """根據策略緩存DataFrame"""

            # 估算數據大小
            sample_count = df.sample(0.01).count()
            estimated_size = sample_count * 100  # 估算總記錄數

            current_time = time.time()

            if strategy == "auto":
                # 自動選擇存儲級別
                if estimated_size < 10000:
                    storage_level = StorageLevel.MEMORY_ONLY
                elif estimated_size < 100000:
                    storage_level = StorageLevel.MEMORY_AND_DISK
                else:
                    storage_level = StorageLevel.MEMORY_AND_DISK_SER
            else:
                storage_level = strategy

            # 緩存數據
            cached_df = df.persist(storage_level)

            # 記錄緩存信息
            self.cached_dataframes[name] = cached_df
            self.usage_counts[name] = 1
            self.cache_sizes[name] = estimated_size
            self.last_access[name] = current_time

            print(f"緩存 '{name}' 使用存儲級別: {storage_level}")
            print(f"估算大小: {estimated_size} 記錄")

            return cached_df

        def get_cached_df(self, name):
            """獲取緩存的DataFrame"""
            if name in self.cached_dataframes:
                self.usage_counts[name] += 1
                self.last_access[name] = time.time()
                return self.cached_dataframes[name]
            return None

        def cleanup_cache(self, max_age_minutes=30, min_usage=2):
            """清理過期或低使用率的緩存"""
            current_time = time.time()
            to_remove = []

            for name in self.cached_dataframes:
                age_minutes = (current_time - self.last_access[name]) / 60
                usage_count = self.usage_counts[name]

                if age_minutes > max_age_minutes or usage_count < min_usage:
                    to_remove.append(name)

            for name in to_remove:
                print(
                    f"清理緩存: {name} (使用次數: {self.usage_counts[name]}, 年齡: {(current_time - self.last_access[name])/60:.1f}分鐘)"
                )
                self.cached_dataframes[name].unpersist()
                del self.cached_dataframes[name]
                del self.usage_counts[name]
                del self.cache_sizes[name]
                del self.last_access[name]

        def get_cache_statistics(self):
            """獲取緩存統計信息"""
            total_cached = len(self.cached_dataframes)
            total_size = sum(self.cache_sizes.values())
            total_usage = sum(self.usage_counts.values())

            return {
                "cached_dataframes": total_cached,
                "total_estimated_size": total_size,
                "total_usage_count": total_usage,
                "average_usage": total_usage / total_cached if total_cached > 0 else 0,
            }

    # 使用智能緩存管理器
    cache_manager = SmartCacheManager(spark)

    # 創建多個不同大小的數據集
    small_df = generate_complex_dataset(10000)
    medium_df = generate_complex_dataset(50000)
    large_df = generate_complex_dataset(200000)

    # 智能緩存
    cached_small = cache_manager.cache_with_strategy(
        create_complex_transformations(small_df), "small_analysis"
    )
    cached_medium = cache_manager.cache_with_strategy(
        create_complex_transformations(medium_df), "medium_analysis"
    )
    cached_large = cache_manager.cache_with_strategy(
        create_complex_transformations(large_df), "large_analysis"
    )

    # 模擬使用模式
    print("\n模擬數據使用模式:")
    for i in range(5):
        # 頻繁使用小數據集
        small_result = cache_manager.get_cached_df("small_analysis").count()
        print(f"第{i+1}次使用小數據集: {small_result} 記錄")

        if i % 2 == 0:
            # 偶爾使用中等數據集
            medium_result = cache_manager.get_cached_df("medium_analysis").count()
            print(f"第{i//2+1}次使用中等數據集: {medium_result} 記錄")

        time.sleep(1)  # 模擬時間間隔

    # 顯示緩存統計
    cache_stats = cache_manager.get_cache_statistics()
    print(f"\n緩存統計信息:")
    for key, value in cache_stats.items():
        print(f"{key}: {value}")

    # 4. 緩存生命週期管理
    print("\n4. 緩存生命週期管理:")

    # 4.1 動態緩存管理
    print("\n4.1 動態緩存管理:")

    def dynamic_cache_management():
        """實現動態緩存管理"""

        # 創建工作流程數據
        workflow_data = [
            ("step1", generate_complex_dataset(30000)),
            ("step2", generate_complex_dataset(40000)),
            ("step3", generate_complex_dataset(50000)),
        ]

        workflow_results = {}

        for step_name, df in workflow_data:
            print(f"\n處理工作流步驟: {step_name}")

            # 決定是否緩存
            if step_name in ["step1", "step3"]:  # 這些步驟會被重複使用
                print(f"緩存 {step_name} (預期重複使用)")
                cached_df = cache_manager.cache_with_strategy(
                    create_complex_transformations(df), step_name
                )
            else:
                print(f"不緩存 {step_name} (一次性使用)")
                cached_df = create_complex_transformations(df)

            # 執行計算
            start_time = time.time()
            result = cached_df.collect()
            execution_time = time.time() - start_time

            workflow_results[step_name] = {
                "result_count": len(result),
                "execution_time": execution_time,
            }

            print(
                f"{step_name} 執行時間: {execution_time:.2f}s, 結果: {len(result)} 記錄"
            )

        # 重複使用緩存的步驟
        print("\n重複執行緩存的步驟:")
        for step_name in ["step1", "step3"]:
            cached_df = cache_manager.get_cached_df(step_name)
            if cached_df:
                start_time = time.time()
                result = cached_df.collect()
                execution_time = time.time() - start_time
                print(f"{step_name} 重複執行時間: {execution_time:.2f}s")

        return workflow_results

    workflow_results = dynamic_cache_management()

    # 5. 序列化性能優化
    print("\n5. 序列化性能優化:")

    # 5.1 序列化格式比較
    print("\n5.1 序列化格式性能比較:")

    def test_serialization_performance():
        """測試不同序列化格式的性能"""

        # 創建複雜對象結構的數據
        complex_data_df = base_df.withColumn(
            "complex_struct",
            struct(
                col("user_name").alias("name"),
                col("category").alias("cat"),
                array(col("score"), col("amount")).alias("metrics"),
            ),
        ).select("id", "complex_struct", "country", "timestamp")

        serialization_tests = [
            ("Java序列化", StorageLevel.MEMORY_ONLY_SER),
            ("Kryo序列化", StorageLevel.MEMORY_ONLY_SER),  # Kryo已在SparkSession中配置
            ("無序列化", StorageLevel.MEMORY_ONLY),
        ]

        serialization_results = []

        for name, storage_level in serialization_tests:
            print(f"\n測試序列化方式: {name}")

            # 清理緩存
            spark.catalog.clearCache()
            gc.collect()

            start_memory = monitor_memory_usage()

            # 緩存數據
            cached_df = complex_data_df.persist(storage_level)

            # 觸發緩存
            start_time = time.time()
            count = cached_df.count()
            cache_time = time.time() - start_time

            cache_memory = monitor_memory_usage()

            # 測試讀取性能
            start_time = time.time()
            sample_result = cached_df.sample(0.1).collect()
            read_time = time.time() - start_time

            result_info = {
                "method": name,
                "cache_time": cache_time,
                "read_time": read_time,
                "memory_usage": cache_memory - start_memory,
                "record_count": count,
            }

            serialization_results.append(result_info)

            print(f"緩存時間: {cache_time:.2f}s")
            print(f"讀取時間: {read_time:.2f}s")
            print(f"內存使用: {cache_memory - start_memory:.2f} MB")

            cached_df.unpersist()

        return serialization_results

    serialization_results = test_serialization_performance()

    # 6. 內存使用監控和優化
    print("\n6. 內存使用監控和優化:")

    # 6.1 內存使用分析
    print("\n6.1 內存使用深度分析:")

    def memory_usage_analysis():
        """詳細分析內存使用模式"""

        # 創建不同類型的數據進行緩存
        datasets = {
            "數值密集": generate_complex_dataset(100000).select(
                "id", "score", "amount"
            ),
            "字符串密集": generate_complex_dataset(50000).select(
                "id", "user_name", "country", "category"
            ),
            "混合類型": generate_complex_dataset(75000),
            "嵌套結構": generate_complex_dataset(30000).withColumn(
                "nested", struct(col("user_name"), col("category"), col("amount"))
            ),
        }

        memory_analysis = []

        for data_type, df in datasets.items():
            print(f"\n分析數據類型: {data_type}")

            # 清理緩存
            spark.catalog.clearCache()
            gc.collect()

            before_memory = monitor_memory_usage()
            before_spark_total, before_spark_used = get_spark_storage_info()

            # 緩存數據
            cached_df = df.persist(StorageLevel.MEMORY_ONLY)
            count = cached_df.count()  # 觸發緩存

            after_memory = monitor_memory_usage()
            after_spark_total, after_spark_used = get_spark_storage_info()

            memory_per_record = (
                (after_memory - before_memory) / count * 1024 * 1024
            )  # bytes per record

            analysis_result = {
                "data_type": data_type,
                "record_count": count,
                "memory_increase_mb": after_memory - before_memory,
                "spark_memory_increase_mb": (after_spark_used - before_spark_used)
                / 1024
                / 1024,
                "bytes_per_record": memory_per_record,
            }

            memory_analysis.append(analysis_result)

            print(f"記錄數: {count}")
            print(f"內存增加: {after_memory - before_memory:.2f} MB")
            print(f"每記錄內存: {memory_per_record:.2f} bytes")

            cached_df.unpersist()

        return memory_analysis

    memory_analysis = memory_usage_analysis()

    # 7. 緩存策略最佳實踐
    print("\n7. 緩存策略最佳實踐:")

    # 7.1 緩存決策樹
    print("\n7.1 緩存決策樹:")

    def cache_decision_tree(df, usage_pattern="unknown"):
        """基於數據特徵和使用模式的緩存決策"""

        # 分析數據特徵
        sample_df = df.sample(0.01, seed=42)
        sample_count = sample_df.count()
        estimated_total = sample_count * 100

        # 計算數據複雜度（簡化指標）
        column_count = len(df.columns)

        decision_factors = {
            "estimated_size": estimated_total,
            "column_count": column_count,
            "usage_pattern": usage_pattern,
        }

        print(f"數據分析結果:")
        print(f"- 估算記錄數: {estimated_total}")
        print(f"- 列數: {column_count}")
        print(f"- 使用模式: {usage_pattern}")

        # 決策邏輯
        if usage_pattern == "one_time":
            recommendation = "不建議緩存"
            storage_level = None
        elif estimated_total < 10000:
            recommendation = "MEMORY_ONLY"
            storage_level = StorageLevel.MEMORY_ONLY
        elif estimated_total < 100000 and column_count < 10:
            recommendation = "MEMORY_AND_DISK"
            storage_level = StorageLevel.MEMORY_AND_DISK
        elif estimated_total < 500000:
            recommendation = "MEMORY_AND_DISK_SER"
            storage_level = StorageLevel.MEMORY_AND_DISK_SER
        else:
            recommendation = "DISK_ONLY或考慮不緩存"
            storage_level = StorageLevel.DISK_ONLY

        print(f"緩存建議: {recommendation}")

        return storage_level, decision_factors

    # 測試緩存決策樹
    test_cases = [
        (generate_complex_dataset(5000), "frequent"),
        (generate_complex_dataset(80000), "moderate"),
        (generate_complex_dataset(300000), "infrequent"),
        (generate_complex_dataset(1000), "one_time"),
    ]

    for i, (test_df, pattern) in enumerate(test_cases, 1):
        print(f"\n測試案例 {i}:")
        storage_level, factors = cache_decision_tree(test_df, pattern)

    # 8. 緩存性能基準測試
    print("\n8. 緩存性能基準測試:")

    # 8.1 綜合性能測試
    print("\n8.1 綜合緩存性能基準:")

    def comprehensive_cache_benchmark():
        """綜合緩存性能基準測試"""

        # 創建基準測試場景
        scenarios = [
            ("小數據頻繁讀取", generate_complex_dataset(20000), "frequent"),
            ("中數據中等讀取", generate_complex_dataset(100000), "moderate"),
            ("大數據偶爾讀取", generate_complex_dataset(300000), "infrequent"),
        ]

        benchmark_results = []

        for scenario_name, df, usage in scenarios:
            print(f"\n基準測試場景: {scenario_name}")

            # 獲取緩存建議
            recommended_storage, _ = cache_decision_tree(df, usage)

            if recommended_storage:
                # 測試緩存性能
                spark.catalog.clearCache()
                gc.collect()

                # 創建複雜轉換
                complex_transform = create_complex_transformations(df)

                # 無緩存性能
                start_time = time.time()
                no_cache_result = complex_transform.collect()
                no_cache_time = time.time() - start_time

                # 有緩存性能
                cached_transform = complex_transform.persist(recommended_storage)

                start_time = time.time()
                cached_result1 = cached_transform.collect()
                first_cache_time = time.time() - start_time

                start_time = time.time()
                cached_result2 = cached_transform.collect()
                second_cache_time = time.time() - start_time

                improvement = (
                    ((no_cache_time - second_cache_time) / no_cache_time * 100)
                    if no_cache_time > 0
                    else 0
                )

                result = {
                    "scenario": scenario_name,
                    "no_cache_time": no_cache_time,
                    "first_cache_time": first_cache_time,
                    "second_cache_time": second_cache_time,
                    "improvement_percent": improvement,
                    "storage_level": str(recommended_storage),
                }

                benchmark_results.append(result)

                print(f"無緩存時間: {no_cache_time:.2f}s")
                print(f"首次緩存時間: {first_cache_time:.2f}s")
                print(f"二次緩存時間: {second_cache_time:.2f}s")
                print(f"性能改善: {improvement:.1f}%")

                cached_transform.unpersist()
            else:
                print("建議不使用緩存")

        return benchmark_results

    benchmark_results = comprehensive_cache_benchmark()

    # 9. 緩存策略總結
    print("\n9. 緩存策略總結和建議:")

    def generate_cache_recommendations():
        """生成緩存策略建議"""

        # 基於測試結果的建議
        best_storage_level = None
        best_improvement = 0

        for result in cache_results:
            if result["improvement_percent"] > best_improvement:
                best_improvement = result["improvement_percent"]
                best_storage_level = result["storage_level"]

        recommendations = [
            f"對於重複使用的數據，推薦使用 {best_storage_level}",
            f"預期性能改善可達 {best_improvement:.1f}%",
            "小於10MB的數據集使用 MEMORY_ONLY",
            "大於100MB的數據集使用 MEMORY_AND_DISK_SER",
            "一次性使用的數據不建議緩存",
            "定期清理不再使用的緩存",
            "監控內存使用避免OOM錯誤",
        ]

        guidelines = {
            "緩存時機": "當數據會被重複使用2次以上時",
            "存儲級別選擇": "根據數據大小和內存容量",
            "生命週期管理": "及時清理過期緩存",
            "性能監控": "監控緩存命中率和內存使用",
            "序列化策略": "複雜對象使用Kryo序列化",
        }

        print("緩存策略建議:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")

        print("\n緩存管理指導原則:")
        for principle, description in guidelines.items():
            print(f"- {principle}: {description}")

        return recommendations, guidelines

    cache_recommendations, cache_guidelines = generate_cache_recommendations()

    # 10. 清理和總結
    print("\n10. 資源清理和總結:")

    # 清理所有緩存
    cache_manager.cleanup_cache(max_age_minutes=0, min_usage=0)
    spark.catalog.clearCache()

    # 最終內存狀態
    print("\n最終內存狀態:")
    final_memory = monitor_memory_usage()
    final_spark_total, final_spark_used = get_spark_storage_info()

    print(f"內存變化: {final_memory - initial_memory:.2f} MB")
    print(
        f"Spark內存變化: {(final_spark_used - initial_spark_used) / 1024 / 1024:.2f} MB"
    )

    # 生成性能報告
    performance_summary = {
        "最佳緩存策略": best_storage_level,
        "最大性能改善": f"{best_improvement:.1f}%",
        "內存效率最高": min(memory_analysis, key=lambda x: x["bytes_per_record"])[
            "data_type"
        ],
        "測試場景數": len(benchmark_results),
        "緩存建議數": len(cache_recommendations),
    }

    print("\n緩存性能測試總結:")
    for metric, value in performance_summary.items():
        print(f"- {metric}: {value}")

    # 清理資源
    spark.stop()
    print("\n緩存和持久化策略練習完成！")


if __name__ == "__main__":
    main()
