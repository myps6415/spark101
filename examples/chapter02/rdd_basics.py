#!/usr/bin/env python3
"""
第2章：Spark Core 基本操作 - RDD 基礎
學習 RDD 的創建和基本操作
"""

from pyspark import SparkConf, SparkContext


def main():
    # 創建 SparkContext
    conf = SparkConf().setAppName("RDD Basics").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    print("🎯 RDD 基礎操作示範")
    print("=" * 30)

    # 1. 從集合創建 RDD
    numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    numbers_rdd = sc.parallelize(numbers)

    print(f"原始數據: {numbers}")
    print(f"RDD 分區數: {numbers_rdd.getNumPartitions()}")

    # 2. Transformation 操作
    print("\n🔄 Transformation 操作:")

    # 過濾偶數
    even_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)
    print(f"偶數: {even_rdd.collect()}")

    # 映射操作 - 平方
    squared_rdd = numbers_rdd.map(lambda x: x**2)
    print(f"平方: {squared_rdd.collect()}")

    # 扁平化映射
    words = ["hello world", "spark is awesome", "big data processing"]
    words_rdd = sc.parallelize(words)
    flat_words_rdd = words_rdd.flatMap(lambda x: x.split())
    print(f"分詞結果: {flat_words_rdd.collect()}")

    # 3. Action 操作
    print("\n⚡ Action 操作:")

    # 計數
    count = numbers_rdd.count()
    print(f"元素總數: {count}")

    # 求和
    total = numbers_rdd.reduce(lambda x, y: x + y)
    print(f"總和: {total}")

    # 取前 N 個元素
    first_three = numbers_rdd.take(3)
    print(f"前三個元素: {first_three}")

    # 統計
    stats = numbers_rdd.stats()
    print(f"統計信息: 平均值={stats.mean():.2f}, 標準差={stats.stdev():.2f}")

    # 4. 鍵值對 RDD
    print("\n🔑 鍵值對 RDD 操作:")

    # 創建鍵值對
    pairs_rdd = numbers_rdd.map(lambda x: (x % 3, x))
    print(f"鍵值對: {pairs_rdd.collect()}")

    # 按鍵分組
    grouped_rdd = pairs_rdd.groupByKey()
    grouped_result = grouped_rdd.mapValues(list).collect()
    print(f"按鍵分組: {grouped_result}")

    # 按鍵求和
    sum_by_key = pairs_rdd.reduceByKey(lambda x, y: x + y)
    print(f"按鍵求和: {sum_by_key.collect()}")

    # 停止 SparkContext
    sc.stop()
    print("\n✅ RDD 操作示範完成")


if __name__ == "__main__":
    main()
