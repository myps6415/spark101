#!/usr/bin/env python3
"""
第2章練習1：基本 RDD 操作
數字序列處理練習
"""

from pyspark import SparkConf, SparkContext


def main():
    # 創建 SparkContext
    conf = SparkConf().setAppName("RDD基本操作練習").setMaster("local[*]")
    sc = SparkContext(conf=conf)

    print("=== 第2章練習1：基本 RDD 操作 ===")

    # 創建數字 RDD
    numbers = list(range(1, 101))
    numbers_rdd = sc.parallelize(numbers)

    print(f"原始數據範圍: 1 到 100，共 {numbers_rdd.count()} 個數字")
    print(f"分區數: {numbers_rdd.getNumPartitions()}")

    # 1. 找出所有偶數
    even_numbers = numbers_rdd.filter(lambda x: x % 2 == 0)
    print(f"\n1. 偶數總數: {even_numbers.count()}")
    print(f"前10個偶數: {even_numbers.take(10)}")

    # 2. 計算偶數的平方
    even_squares = even_numbers.map(lambda x: x**2)
    print(f"\n2. 前10個偶數平方: {even_squares.take(10)}")

    # 3. 計算偶數平方的總和
    total_sum = even_squares.reduce(lambda x, y: x + y)
    print(f"\n3. 偶數平方總和: {total_sum}")

    # 4. 找出最大的3個偶數平方值
    top_three = even_squares.top(3)
    print(f"\n4. 最大的3個偶數平方值: {top_three}")

    # 額外分析
    print("\n=== 額外分析 ===")

    # 5. 統計分析
    even_squares_list = even_squares.collect()
    count = len(even_squares_list)
    mean = sum(even_squares_list) / count
    print(f"\n5. 統計分析:")
    print(f"   平均值: {mean:.2f}")
    print(f"   最小值: {min(even_squares_list)}")
    print(f"   最大值: {max(even_squares_list)}")

    # 6. 按範圍分組
    range_groups = even_squares.map(
        lambda x: ("Small" if x < 1000 else "Medium" if x < 5000 else "Large", 1)
    ).reduceByKey(lambda a, b: a + b)

    print(f"\n6. 按大小分組:")
    for group, count in range_groups.collect():
        print(f"   {group}: {count} 個")

    # 7. 計算累積分佈
    sorted_squares = even_squares.sortBy(lambda x: x)
    total_count = sorted_squares.count()

    print(f"\n7. 分位數:")
    percentiles = [25, 50, 75, 90]
    for p in percentiles:
        index = int((p / 100.0) * total_count) - 1
        value = sorted_squares.take(index + 1)[index]
        print(f"   第{p}百分位數: {value}")

    # 8. 性能測試
    print(f"\n8. 性能測試:")
    import time

    # 測試不同操作的性能
    start_time = time.time()
    numbers_rdd.filter(lambda x: x % 2 == 0).map(lambda x: x**2).reduce(
        lambda x, y: x + y
    )
    end_time = time.time()
    print(f"   鏈式操作執行時間: {end_time - start_time:.4f} 秒")

    # 測試緩存效果
    cached_rdd = even_squares.cache()

    start_time = time.time()
    cached_rdd.count()
    first_time = time.time() - start_time

    start_time = time.time()
    cached_rdd.count()
    second_time = time.time() - start_time

    print(f"   首次訪問緩存 RDD: {first_time:.4f} 秒")
    print(f"   第二次訪問緩存 RDD: {second_time:.4f} 秒")
    print(f"   性能提升: {first_time/second_time:.2f}x")

    # 停止 SparkContext
    sc.stop()
    print("\n練習完成！")


if __name__ == "__main__":
    main()
