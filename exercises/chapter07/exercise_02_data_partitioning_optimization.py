#!/usr/bin/env python3
"""
第7章練習2：數據分區優化
學習如何優化數據分區策略，提高 Spark 應用的並行性和性能
"""

import random
import time
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (asc, avg, broadcast, col, collect_list,
                                   count, desc, explode, expr)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import rand
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import row_number, size, spark_partition_id, stddev
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType,
                               StructField, StructType)
from pyspark.sql.window import Window


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("數據分區優化練習")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

    print("=== 第7章練習2：數據分區優化 ===")

    # 1. 理解默認分區策略
    print("\n1. 分析默認分區策略:")

    # 1.1 分區分析函數
    print("\n1.1 分區分析工具:")

    def analyze_partitions(df, name="DataFrame"):
        """分析 DataFrame 的分區情況"""
        print(f"\n{name} 分區分析:")
        print("=" * 50)

        num_partitions = df.rdd.getNumPartitions()
        print(f"分區數量: {num_partitions}")

        # 每個分區的記錄數
        partition_counts = (
            df.withColumn("partition_id", spark_partition_id())
            .groupBy("partition_id")
            .count()
            .orderBy("partition_id")
        )

        print("每個分區的記錄數:")
        partition_counts.show()

        # 分區大小統計
        partition_stats = partition_counts.agg(
            avg("count").alias("avg_records_per_partition"),
            min("count").alias("min_records_per_partition"),
            max("count").alias("max_records_per_partition"),
            stddev("count").alias("stddev_records"),
        ).collect()[0]

        print(f"平均每分區記錄數: {partition_stats['avg_records_per_partition']:.2f}")
        print(f"最小分區記錄數: {partition_stats['min_records_per_partition']}")
        print(f"最大分區記錄數: {partition_stats['max_records_per_partition']}")
        print(f"標準差: {partition_stats['stddev_records']:.2f}")

        # 數據傾斜檢測
        if (
            partition_stats["max_records_per_partition"]
            > partition_stats["avg_records_per_partition"] * 2
        ):
            print("⚠️  檢測到數據傾斜!")
        else:
            print("✓ 數據分佈相對均勻")

        return partition_counts

    # 1.2 創建測試數據集
    print("\n1.2 創建測試數據集:")

    def create_large_dataset(num_records=100000):
        """創建大型測試數據集"""
        data = []
        for i in range(num_records):
            # 模擬數據傾斜：某些group_id有更多記錄
            if i % 10000 < 1000:
                group_id = 1  # 熱點數據
            elif i % 10000 < 2000:
                group_id = 2  # 次熱點數據
            else:
                group_id = random.randint(3, 100)  # 均勻分佈數據

            data.append(
                (
                    i,
                    f"user_{random.randint(1, 10000)}",
                    group_id,
                    random.choice(["A", "B", "C", "D", "E"]),
                    random.randint(18, 80),
                    random.uniform(100, 10000),
                    random.choice(["US", "CN", "JP", "UK", "DE"]),
                )
            )

        columns = [
            "id",
            "user_name",
            "group_id",
            "category",
            "age",
            "amount",
            "country",
        ]
        return spark.createDataFrame(data, columns)

    def create_small_dataset():
        """創建小型維度表"""
        data = []
        for i in range(1, 101):
            data.append(
                (
                    i,
                    f"Group {i}",
                    random.choice(["Premium", "Standard", "Basic"]),
                    random.uniform(0.1, 1.0),
                )
            )

        columns = ["group_id", "group_name", "tier", "discount_rate"]
        return spark.createDataFrame(data, columns)

    large_df = create_large_dataset(100000)
    small_df = create_small_dataset()

    print(f"大表記錄數: {large_df.count()}")
    print(f"小表記錄數: {small_df.count()}")

    # 分析默認分區
    analyze_partitions(large_df, "原始大表")
    analyze_partitions(small_df, "原始小表")

    # 2. 分區策略比較
    print("\n2. 分區策略比較:")

    # 2.1 repartition vs coalesce
    print("\n2.1 repartition vs coalesce 比較:")

    def compare_partitioning_methods():
        """比較不同分區方法的性能"""

        # 原始數據
        original = large_df

        # 不同分區策略
        strategies = {
            "原始": original,
            "repartition(20)": original.repartition(20),
            "repartition(50)": original.repartition(50),
            "coalesce(8)": original.coalesce(8),
            "repartition(group_id)": original.repartition("group_id"),
            "repartition(10, group_id)": original.repartition(10, "group_id"),
        }

        results = []

        for name, df in strategies.items():
            print(f"\n測試策略: {name}")

            # 記錄執行時間
            start_time = time.time()

            # 執行聚合操作
            result = (
                df.groupBy("group_id", "category")
                .agg(
                    count("*").alias("count"),
                    avg("amount").alias("avg_amount"),
                    spark_sum("amount").alias("total_amount"),
                )
                .collect()
            )

            execution_time = time.time() - start_time

            # 分析分區
            partition_info = analyze_partitions(df, name)

            results.append(
                {
                    "strategy": name,
                    "execution_time": execution_time,
                    "num_partitions": df.rdd.getNumPartitions(),
                    "result_count": len(result),
                }
            )

            print(f"執行時間: {execution_time:.2f} 秒")

        # 性能比較總結
        print("\n分區策略性能比較:")
        print("策略\t\t\t分區數\t執行時間(秒)")
        print("-" * 50)
        for result in results:
            print(
                f"{result['strategy']:<20}\t{result['num_partitions']}\t{result['execution_time']:.2f}"
            )

        return results

    partitioning_results = compare_partitioning_methods()

    # 3. JOIN 操作分區優化
    print("\n3. JOIN 操作分區優化:")

    # 3.1 不同 JOIN 分區策略
    print("\n3.1 JOIN 分區策略測試:")

    def test_join_partitioning():
        """測試不同分區策略對 JOIN 性能的影響"""

        join_strategies = [
            ("默認分區", large_df, small_df),
            ("大表按key分區", large_df.repartition("group_id"), small_df),
            (
                "兩表都按key分區",
                large_df.repartition("group_id"),
                small_df.repartition("group_id"),
            ),
            ("廣播小表", large_df, broadcast(small_df)),
            ("大表coalesce", large_df.coalesce(10), small_df),
        ]

        join_results = []

        for name, left_df, right_df in join_strategies:
            print(f"\n測試 JOIN 策略: {name}")

            start_time = time.time()

            # 執行 JOIN 操作
            joined_df = left_df.join(right_df, "group_id", "inner")

            # 執行聚合以觸發計算
            result = (
                joined_df.groupBy("tier", "category")
                .agg(
                    count("*").alias("count"),
                    avg("amount").alias("avg_amount"),
                    spark_sum(col("amount") * col("discount_rate")).alias(
                        "discounted_total"
                    ),
                )
                .collect()
            )

            execution_time = time.time() - start_time

            join_results.append(
                {
                    "strategy": name,
                    "execution_time": execution_time,
                    "result_count": len(result),
                }
            )

            print(f"執行時間: {execution_time:.2f} 秒")
            print(f"結果記錄數: {len(result)}")

        # JOIN 性能比較
        print("\nJOIN 性能比較:")
        print("策略\t\t\t執行時間(秒)\t結果數")
        print("-" * 45)
        for result in join_results:
            print(
                f"{result['strategy']:<20}\t{result['execution_time']:.2f}\t\t{result['result_count']}"
            )

        return join_results

    join_results = test_join_partitioning()

    # 4. 數據傾斜處理
    print("\n4. 數據傾斜處理:")

    # 4.1 檢測數據傾斜
    print("\n4.1 數據傾斜檢測:")

    def detect_data_skew(df, key_column):
        """檢測指定列的數據傾斜情況"""

        # 計算每個key的記錄數
        key_distribution = df.groupBy(key_column).count().orderBy(desc("count"))

        print(f"{key_column} 分佈情況 (Top 10):")
        key_distribution.show(10)

        # 統計分析
        stats = key_distribution.agg(
            avg("count").alias("avg_count"),
            min("count").alias("min_count"),
            max("count").alias("max_count"),
            stddev("count").alias("stddev_count"),
        ).collect()[0]

        print(f"平均記錄數: {stats['avg_count']:.2f}")
        print(f"最小記錄數: {stats['min_count']}")
        print(f"最大記錄數: {stats['max_count']}")
        print(f"標準差: {stats['stddev_count']:.2f}")

        # 計算傾斜比例
        skew_ratio = stats["max_count"] / stats["avg_count"]
        print(f"傾斜比例: {skew_ratio:.2f}")

        if skew_ratio > 3:
            print("⚠️  嚴重數據傾斜!")
        elif skew_ratio > 2:
            print("⚠️  中度數據傾斜")
        else:
            print("✓ 數據分佈相對均勻")

        return key_distribution, skew_ratio

    # 檢測 group_id 的傾斜情況
    group_distribution, skew_ratio = detect_data_skew(large_df, "group_id")

    # 4.2 處理數據傾斜
    print("\n4.2 數據傾斜處理策略:")

    def handle_data_skew():
        """實現多種數據傾斜處理策略"""

        # 策略1：加鹽分區
        print("\n策略1: 加鹽分區")

        def salt_partition(df, key_col, salt_range=10):
            """為傾斜的key添加隨機鹽值"""
            return df.withColumn("salt", (rand() * salt_range).cast("int")).withColumn(
                "salted_key", expr(f"concat({key_col}, '_', salt)")
            )

        # 對大表加鹽
        salted_large = salt_partition(large_df, "group_id", 5)

        # 對小表展開(與鹽值匹配)
        expanded_small = small_df.withColumn(
            "salt", explode(expr("sequence(0, 4)"))
        ).withColumn("salted_key", expr("concat(group_id, '_', salt)"))

        start_time = time.time()

        # 使用加鹽後的key進行JOIN
        salted_join = (
            salted_large.join(expanded_small, "salted_key", "inner")
            .groupBy("tier", "category")
            .agg(count("*").alias("count"), avg("amount").alias("avg_amount"))
        )

        salted_result = salted_join.collect()
        salted_time = time.time() - start_time

        print(f"加鹽JOIN執行時間: {salted_time:.2f} 秒")
        print(f"結果記錄數: {len(salted_result)}")

        # 策略2：分桶處理
        print("\n策略2: 分桶處理")

        # 識別熱點key
        hot_keys = (
            group_distribution.filter(col("count") > 1000)
            .select("group_id")
            .rdd.flatMap(lambda x: x)
            .collect()
        )
        print(f"熱點keys: {hot_keys}")

        # 分離熱點數據和正常數據
        hot_data = large_df.filter(col("group_id").isin(hot_keys))
        normal_data = large_df.filter(~col("group_id").isin(hot_keys))

        start_time = time.time()

        # 對正常數據使用普通JOIN
        normal_join = normal_data.join(small_df, "group_id", "inner")

        # 對熱點數據使用廣播JOIN
        hot_join = hot_data.join(broadcast(small_df), "group_id", "inner")

        # 合併結果
        combined_result = (
            normal_join.union(hot_join)
            .groupBy("tier", "category")
            .agg(count("*").alias("count"), avg("amount").alias("avg_amount"))
            .collect()
        )

        bucket_time = time.time() - start_time

        print(f"分桶處理執行時間: {bucket_time:.2f} 秒")
        print(f"結果記錄數: {len(combined_result)}")

        return salted_time, bucket_time

    skew_results = handle_data_skew()

    # 5. 動態分區調整
    print("\n5. 動態分區調整:")

    # 5.1 基於數據大小的動態分區
    print("\n5.1 基於數據大小的動態分區:")

    def dynamic_partitioning(df, target_partition_size_mb=128):
        """根據數據大小動態調整分區數"""

        # 估算數據大小（簡化估算）
        sample_size = df.sample(0.01).count()
        estimated_total_records = sample_size * 100

        # 假設每條記錄約1KB
        estimated_size_mb = estimated_total_records * 1024 / (1024 * 1024)

        # 計算理想分區數
        ideal_partitions = max(1, int(estimated_size_mb / target_partition_size_mb))

        print(f"估算數據大小: {estimated_size_mb:.2f} MB")
        print(f"目標分區大小: {target_partition_size_mb} MB")
        print(f"建議分區數: {ideal_partitions}")

        # 應用動態分區
        if ideal_partitions != df.rdd.getNumPartitions():
            if ideal_partitions > df.rdd.getNumPartitions():
                optimized_df = df.repartition(ideal_partitions)
            else:
                optimized_df = df.coalesce(ideal_partitions)
        else:
            optimized_df = df

        return optimized_df, ideal_partitions

    # 對大表應用動態分區
    optimized_large, suggested_partitions = dynamic_partitioning(large_df)

    print("\n動態分區前後比較:")
    analyze_partitions(large_df, "優化前")
    analyze_partitions(optimized_large, "優化後")

    # 6. 分區性能監控
    print("\n6. 分區性能監控:")

    # 6.1 分區級別性能分析
    print("\n6.1 分區級別性能分析:")

    def partition_performance_analysis(df):
        """分析各分區的性能特徵"""

        # 添加分區ID和性能指標
        partition_analysis = (
            df.withColumn("partition_id", spark_partition_id())
            .groupBy("partition_id")
            .agg(
                count("*").alias("record_count"),
                avg("amount").alias("avg_amount"),
                min("amount").alias("min_amount"),
                max("amount").alias("max_amount"),
                expr("approx_count_distinct(group_id)").alias("unique_groups"),
                expr("approx_count_distinct(user_name)").alias("unique_users"),
            )
            .withColumn(
                "load_factor",
                col("record_count")
                / expr(
                    "(SELECT AVG(record_count) FROM (SELECT partition_id, COUNT(*) as record_count FROM table GROUP BY partition_id))"
                ),
            )
            .orderBy("partition_id")
        )

        print("分區性能分析:")
        partition_analysis.show(20, truncate=False)

        return partition_analysis

    # 分析優化後的分區性能
    large_df.createOrReplaceTempView("table")
    partition_perf = partition_performance_analysis(optimized_large)

    # 7. 最佳實踐總結
    print("\n7. 分區優化最佳實踐:")

    def summarize_best_practices():
        """總結分區優化的最佳實踐"""

        best_practices = [
            "根據數據大小和集群資源確定分區數",
            "使用合適的分區鍵避免數據傾斜",
            "JOIN前確保相關表使用相同分區策略",
            "小表使用廣播JOIN避免shuffle",
            "監控分區數據分佈和性能指標",
            "對傾斜數據使用加鹽或分桶策略",
            "定期調整分區策略適應數據變化",
        ]

        partitioning_guidelines = {
            "分區數設定": "CPU核心數的2-4倍",
            "分區大小": "100-200MB per partition",
            "傾斜檢測": "最大分區 > 平均分區 * 3",
            "JOIN優化": "大表按JOIN key分區",
            "小表處理": "< 200MB使用廣播JOIN",
        }

        print("分區優化最佳實踐:")
        for i, practice in enumerate(best_practices, 1):
            print(f"{i}. {practice}")

        print("\n分區參數指導原則:")
        for guideline, value in partitioning_guidelines.items():
            print(f"- {guideline}: {value}")

        return best_practices, partitioning_guidelines

    practices, guidelines = summarize_best_practices()

    # 8. 分區策略建議
    print("\n8. 分區策略建議:")

    def generate_partitioning_recommendations():
        """基於分析結果生成分區策略建議"""

        recommendations = []

        # 基於數據傾斜情況
        if skew_ratio > 3:
            recommendations.append("數據嚴重傾斜，建議使用加鹽分區或分桶處理")
        elif skew_ratio > 2:
            recommendations.append("數據中度傾斜，考慮重新設計分區鍵")

        # 基於數據大小
        total_records = large_df.count()
        if total_records > 1000000:
            recommendations.append("大數據集，建議增加分區數以提高並行度")

        # 基於JOIN模式
        recommendations.append("頻繁JOIN操作，建議預分區和使用廣播變量")

        # 基於性能測試結果
        best_strategy = min(partitioning_results, key=lambda x: x["execution_time"])
        recommendations.append(f"性能測試顯示 '{best_strategy['strategy']}' 表現最佳")

        print("個性化分區策略建議:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")

        return recommendations

    recommendations = generate_partitioning_recommendations()

    # 9. 分區監控儀表板
    print("\n9. 分區監控指標:")

    def create_partition_dashboard():
        """創建分區監控儀表板"""

        dashboard_metrics = {
            "總分區數": optimized_large.rdd.getNumPartitions(),
            "數據傾斜比例": f"{skew_ratio:.2f}",
            "平均分區大小": f"{large_df.count() / optimized_large.rdd.getNumPartitions():.0f} 記錄",
            "JOIN性能提升": f"{((max([r['execution_time'] for r in join_results]) - min([r['execution_time'] for r in join_results])) / max([r['execution_time'] for r in join_results]) * 100):.1f}%",
            "建議分區數": suggested_partitions,
            "熱點分區": "檢測到",
        }

        print("分區監控儀表板:")
        print("=" * 40)
        for metric, value in dashboard_metrics.items():
            print(f"{metric:<15}: {value}")

        return dashboard_metrics

    dashboard = create_partition_dashboard()

    # 清理資源
    spark.stop()
    print("\n數據分區優化練習完成！")


if __name__ == "__main__":
    main()
