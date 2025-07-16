#!/usr/bin/env python3
"""
第2章練習3：鍵值對 RDD 操作
Key-Value RDD 轉換和聚合練習
"""

import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("鍵值對RDD操作練習")
        .master("local[*]")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("=== 第2章練習3：鍵值對 RDD 操作 ===")

    # 1. 創建銷售數據
    print("\n1. 創建銷售數據:")

    # 產品銷售記錄 (product_id, (sales_amount, date, region))
    sales_data = [
        ("P001", (1200.0, "2024-01-15", "North")),
        ("P002", (450.0, "2024-01-16", "South")),
        ("P001", (800.0, "2024-01-17", "East")),
        ("P003", (2200.0, "2024-01-18", "North")),
        ("P002", (650.0, "2024-01-19", "West")),
        ("P001", (950.0, "2024-01-20", "South")),
        ("P004", (1800.0, "2024-01-21", "East")),
        ("P003", (1100.0, "2024-01-22", "West")),
        ("P002", (580.0, "2024-01-23", "North")),
        ("P004", (2100.0, "2024-01-24", "South")),
        ("P001", (720.0, "2024-01-25", "East")),
        ("P003", (1650.0, "2024-01-26", "North")),
        ("P002", (420.0, "2024-01-27", "West")),
        ("P004", (1950.0, "2024-01-28", "South")),
        ("P001", (880.0, "2024-01-29", "North")),
    ]

    # 創建鍵值對 RDD
    sales_rdd = sc.parallelize(sales_data)

    print(f"銷售記錄總數: {sales_rdd.count()}")
    print("前5筆銷售記錄:")
    for record in sales_rdd.take(5):
        product_id, (amount, date, region) = record
        print(f"產品 {product_id}: ${amount} ({region}, {date})")

    # 2. 基本鍵值對操作
    print("\n2. 基本鍵值對操作:")

    # 2.1 按產品分組銷售額
    print("\n2.1 按產品統計總銷售額:")

    # 提取 (product_id, sales_amount)
    product_sales = sales_rdd.map(lambda x: (x[0], x[1][0]))

    # 按產品聚合銷售額
    total_sales_by_product = product_sales.reduceByKey(lambda a, b: a + b)

    print("各產品總銷售額:")
    for product, total in total_sales_by_product.sortBy(
        lambda x: x[1], ascending=False
    ).collect():
        print(f"{product}: ${total:,.2f}")

    # 2.2 計算每個產品的銷售次數
    print("\n2.2 各產品銷售次數:")

    product_counts = sales_rdd.map(lambda x: (x[0], 1)).reduceByKey(lambda a, b: a + b)

    for product, count in product_counts.sortByKey().collect():
        print(f"{product}: {count} 次")

    # 3. 高級聚合操作
    print("\n3. 高級聚合操作:")

    # 3.1 每個產品的詳細統計
    print("\n3.1 每個產品的詳細統計:")

    def calculate_stats(values):
        amounts = [v[0] for v in values]  # 提取銷售額
        return {
            "count": len(amounts),
            "total": sum(amounts),
            "avg": sum(amounts) / len(amounts),
            "min": min(amounts),
            "max": max(amounts),
        }

    # 按產品分組並計算統計
    product_details = (
        sales_rdd.map(lambda x: (x[0], [x[1]]))
        .reduceByKey(lambda a, b: a + b)
        .mapValues(calculate_stats)
    )

    for product, stats in product_details.sortByKey().collect():
        print(f"{product}:")
        print(f"  - 銷售次數: {stats['count']}")
        print(f"  - 總銷售額: ${stats['total']:,.2f}")
        print(f"  - 平均銷售額: ${stats['avg']:,.2f}")
        print(f"  - 最小銷售額: ${stats['min']:,.2f}")
        print(f"  - 最大銷售額: ${stats['max']:,.2f}")

    # 4. 按地區分析
    print("\n4. 按地區分析:")

    # 4.1 地區銷售統計
    print("\n4.1 地區銷售統計:")

    # 提取 (region, sales_amount)
    region_sales = sales_rdd.map(lambda x: (x[1][2], x[1][0]))

    region_totals = region_sales.reduceByKey(lambda a, b: a + b)
    region_counts = region_sales.map(lambda x: (x[0], 1)).reduceByKey(
        lambda a, b: a + b
    )

    # 合併地區統計
    region_stats = region_totals.join(region_counts).mapValues(
        lambda x: {"total": x[0], "count": x[1], "avg": x[0] / x[1]}
    )

    print("各地區銷售統計:")
    for region, stats in region_stats.sortBy(
        lambda x: x[1]["total"], ascending=False
    ).collect():
        print(f"{region}:")
        print(f"  - 總銷售額: ${stats['total']:,.2f}")
        print(f"  - 銷售次數: {stats['count']}")
        print(f"  - 平均銷售額: ${stats['avg']:,.2f}")

    # 5. 複雜的分組操作
    print("\n5. 複雜的分組操作:")

    # 5.1 產品-地區組合分析
    print("\n5.1 產品-地區組合分析:")

    # 創建複合鍵 (product_id, region)
    product_region_sales = sales_rdd.map(lambda x: ((x[0], x[1][2]), x[1][0]))

    product_region_totals = product_region_sales.reduceByKey(lambda a, b: a + b)

    print("產品-地區銷售額:")
    for (product, region), total in product_region_totals.sortBy(
        lambda x: x[1], ascending=False
    ).collect():
        print(f"{product} in {region}: ${total:,.2f}")

    # 6. Join 操作
    print("\n6. Join 操作:")

    # 創建產品信息 RDD
    product_info = [
        ("P001", ("Laptop", "Electronics", 25000)),
        ("P002", ("Phone", "Electronics", 18000)),
        ("P003", ("Book", "Education", 500)),
        ("P004", ("Headphones", "Electronics", 3000)),
    ]

    product_info_rdd = sc.parallelize(product_info)

    print("\n產品信息:")
    for product_id, (name, category, price) in product_info_rdd.collect():
        print(f"{product_id}: {name} ({category}) - ${price}")

    # 6.1 內連接 - 銷售額與產品信息
    print("\n6.1 銷售額與產品信息內連接:")

    sales_with_info = total_sales_by_product.join(product_info_rdd)

    for product_id, (total_sales, (name, category, price)) in sales_with_info.collect():
        print(f"{product_id} ({name}): 總銷售額 ${total_sales:,.2f}, 單價 ${price}")

    # 6.2 左連接 - 包含所有銷售記錄
    print("\n6.2 左外連接:")

    left_join_result = total_sales_by_product.leftOuterJoin(product_info_rdd)

    for product_id, (total_sales, product_info) in left_join_result.collect():
        if product_info:
            name, category, price = product_info
            print(f"{product_id} ({name}): ${total_sales:,.2f}")
        else:
            print(f"{product_id} (未知產品): ${total_sales:,.2f}")

    # 7. 分區操作
    print("\n7. 分區操作:")

    # 7.1 查看當前分區
    print(f"\n7.1 當前 RDD 分區數: {sales_rdd.getNumPartitions()}")

    # 7.2 按鍵分區
    partitioned_sales = sales_rdd.partitionBy(4)
    print(f"重新分區後分區數: {partitioned_sales.getNumPartitions()}")

    # 7.3 每個分區的統計
    def partition_stats(index, iterator):
        records = list(iterator)
        return [(index, len(records))]

    partition_counts = partitioned_sales.mapPartitionsWithIndex(
        partition_stats
    ).collect()
    print("每個分區的記錄數:")
    for partition_id, count in partition_counts:
        print(f"分區 {partition_id}: {count} 條記錄")

    # 8. 累加器應用
    print("\n8. 累加器應用:")

    # 使用累加器統計不同金額範圍的銷售
    high_value_sales = sc.accumulator(0)
    medium_value_sales = sc.accumulator(0)
    low_value_sales = sc.accumulator(0)

    def categorize_sales(record):
        product_id, (amount, date, region) = record
        if amount >= 1500:
            high_value_sales.add(1)
        elif amount >= 800:
            medium_value_sales.add(1)
        else:
            low_value_sales.add(1)
        return record

    # 觸發累加器計算
    sales_rdd.map(categorize_sales).count()

    print("銷售額分類統計:")
    print(f"高額銷售 (≥$1500): {high_value_sales.value}")
    print(f"中額銷售 ($800-$1499): {medium_value_sales.value}")
    print(f"低額銷售 (<$800): {low_value_sales.value}")

    # 9. 廣播變量應用
    print("\n9. 廣播變量應用:")

    # 創建地區代碼映射
    region_codes = {"North": "N", "South": "S", "East": "E", "West": "W"}
    broadcast_region_codes = sc.broadcast(region_codes)

    # 使用廣播變量轉換地區名稱
    sales_with_codes = sales_rdd.map(
        lambda x: (
            x[0],
            (x[1][0], x[1][1], broadcast_region_codes.value.get(x[1][2], x[1][2])),
        )
    )

    print("使用地區代碼的銷售記錄 (前5筆):")
    for record in sales_with_codes.take(5):
        product_id, (amount, date, region_code) = record
        print(f"{product_id}: ${amount} in {region_code} on {date}")

    # 10. 複雜的業務邏輯
    print("\n10. 複雜的業務邏輯:")

    # 10.1 找出每個地區銷售額最高的產品
    print("\n10.1 每個地區銷售額最高的產品:")

    # 創建 (region, (product_id, sales_amount))
    region_product_sales = sales_rdd.map(
        lambda x: (x[1][2], (x[0], x[1][0]))
    ).reduceByKey(lambda a, b: a if a[1] > b[1] else b)

    for region, (product, amount) in region_product_sales.collect():
        print(f"{region}: {product} (${amount:,.2f})")

    # 10.2 計算每個產品的銷售趨勢
    print("\n10.2 產品銷售時間分析:")

    # 按產品和日期分組
    product_daily_sales = sales_rdd.map(
        lambda x: ((x[0], x[1][1]), x[1][0])
    ).reduceByKey(lambda a, b: a + b)

    # 按產品分組日期銷售
    product_timeline = (
        product_daily_sales.map(lambda x: (x[0][0], (x[0][1], x[1])))
        .groupByKey()
        .mapValues(lambda dates: sorted(list(dates)))
    )

    for product, timeline in product_timeline.take(2):  # 只顯示前2個產品
        print(f"{product} 銷售時間線:")
        for date, amount in timeline:
            print(f"  {date}: ${amount:,.2f}")

    # 11. 性能優化示例
    print("\n11. 性能優化示例:")

    # 11.1 緩存常用的 RDD
    frequent_rdd = total_sales_by_product.cache()

    # 多次使用緩存的 RDD
    top_product = frequent_rdd.max(key=lambda x: x[1])
    total_revenue = frequent_rdd.map(lambda x: x[1]).sum()

    print(f"最佳銷售產品: {top_product[0]} (${top_product[1]:,.2f})")
    print(f"總營收: ${total_revenue:,.2f}")

    # 11.2 合併操作減少 shuffle
    combined_stats = (
        sales_rdd.map(lambda x: (x[0], (x[1][0], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: {"total": x[0], "count": x[1], "avg": x[0] / x[1]})
    )

    print("\n優化後的產品統計:")
    for product, stats in combined_stats.sortByKey().collect():
        print(
            f"{product}: 總額 ${stats['total']:,.2f}, 次數 {stats['count']}, 平均 ${stats['avg']:,.2f}"
        )

    # 清理廣播變量
    broadcast_region_codes.destroy()

    # 清理資源
    spark.stop()
    print("\n鍵值對 RDD 操作練習完成！")


if __name__ == "__main__":
    main()
