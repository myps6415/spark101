#!/usr/bin/env python3
"""
第3章：DataFrame 和 Dataset API - DataFrame 基礎操作
學習 DataFrame 的創建、Schema 定義和基本操作
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, lower, regexp_replace, upper, when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("DataFrame Basics")
        .master("local[*]")
        .getOrCreate()
    )

    print("📊 DataFrame 基礎操作示範")
    print("=" * 40)

    # 1. 創建 DataFrame 的不同方式
    print("\n1️⃣ 創建 DataFrame")

    # 方式1: 從列表創建
    data = [
        ("Alice", 25, "Engineer", 75000.0),
        ("Bob", 30, "Manager", 85000.0),
        ("Charlie", 35, "Designer", 65000.0),
        ("Diana", 28, "Analyst", 70000.0),
        ("Eve", 32, "Engineer", 80000.0),
    ]
    columns = ["name", "age", "job", "salary"]
    df = spark.createDataFrame(data, columns)

    print("從列表創建的 DataFrame:")
    df.show()

    # 方式2: 使用 Schema 定義
    print("\n2️⃣ 使用 Schema 定義")
    schema = StructType(
        [
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("job", StringType(), True),
            StructField("salary", DoubleType(), True),
        ]
    )

    df_with_schema = spark.createDataFrame(data, schema)
    print("DataFrame 結構:")
    df_with_schema.printSchema()

    # 3. 基本資訊查看
    print("\n3️⃣ DataFrame 基本資訊")
    print(f"行數: {df.count()}")
    print(f"列數: {len(df.columns)}")
    print(f"列名: {df.columns}")

    # 統計摘要
    print("\n數值列統計摘要:")
    df.describe().show()

    # 4. 選擇和過濾操作
    print("\n4️⃣ 選擇和過濾操作")

    # 選擇特定列
    print("選擇 name 和 salary 列:")
    df.select("name", "salary").show()

    # 使用 col 函數
    print("使用 col 函數選擇:")
    df.select(col("name"), col("salary")).show()

    # 過濾操作
    print("年齡大於 30 的員工:")
    df.filter(col("age") > 30).show()

    print("工程師職位的員工:")
    df.filter(col("job") == "Engineer").show()

    # 多條件過濾
    print("年齡大於 25 且薪資大於 70000 的員工:")
    df.filter((col("age") > 25) & (col("salary") > 70000)).show()

    # 5. 添加和修改列
    print("\n5️⃣ 添加和修改列")

    # 添加新列
    df_with_bonus = df.withColumn("bonus", col("salary") * 0.1)
    print("添加獎金列:")
    df_with_bonus.show()

    # 修改現有列
    df_upper_name = df.withColumn("name", upper(col("name")))
    print("姓名轉大寫:")
    df_upper_name.show()

    # 條件列
    df_with_level = df.withColumn(
        "level",
        when(col("age") < 30, "Junior")
        .when(col("age") < 35, "Senior")
        .otherwise("Expert"),
    )
    print("添加級別列:")
    df_with_level.show()

    # 6. 排序操作
    print("\n6️⃣ 排序操作")

    # 按年齡排序
    print("按年齡升序:")
    df.orderBy(col("age")).show()

    # 按薪資降序
    print("按薪資降序:")
    df.orderBy(col("salary").desc()).show()

    # 多列排序
    print("按職位升序，薪資降序:")
    df.orderBy(col("job").asc(), col("salary").desc()).show()

    # 7. 分組和聚合操作
    print("\n7️⃣ 分組和聚合操作")

    # 按職位分組統計
    print("按職位分組統計:")
    df.groupBy("job").count().show()

    # 按職位計算平均薪資
    print("按職位計算平均薪資:")
    df.groupBy("job").avg("salary").show()

    # 多個聚合函數
    from pyspark.sql.functions import avg, max, min
    from pyspark.sql.functions import sum as spark_sum

    print("按職位的詳細統計:")
    df.groupBy("job").agg(
        avg("salary").alias("avg_salary"),
        max("salary").alias("max_salary"),
        min("salary").alias("min_salary"),
        spark_sum("salary").alias("total_salary"),
    ).show()

    # 8. 字串操作
    print("\n8️⃣ 字串操作")

    # 大小寫轉換
    df_string_ops = df.select(
        col("name"),
        upper(col("name")).alias("upper_name"),
        lower(col("name")).alias("lower_name"),
    )
    print("字串大小寫轉換:")
    df_string_ops.show()

    # 9. 處理空值
    print("\n9️⃣ 處理空值")

    # 創建包含空值的數據
    data_with_nulls = [
        ("Alice", 25, "Engineer", 75000.0),
        ("Bob", None, "Manager", 85000.0),
        ("Charlie", 35, None, 65000.0),
        ("Diana", 28, "Analyst", None),
    ]

    df_nulls = spark.createDataFrame(data_with_nulls, columns)
    print("包含空值的 DataFrame:")
    df_nulls.show()

    # 刪除包含空值的行
    print("刪除包含空值的行:")
    df_nulls.dropna().show()

    # 填充空值
    print("填充空值:")
    df_filled = df_nulls.fillna({"age": 0, "job": "Unknown", "salary": 0.0})
    df_filled.show()

    # 10. 讀寫 CSV 文件
    print("\n🔟 讀寫 CSV 文件")

    # 寫入 CSV
    output_path = "/tmp/employees.csv"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
    print(f"DataFrame 已寫入 {output_path}")

    # 讀取 CSV
    if os.path.exists(output_path):
        df_read = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(output_path)
        )
        print("從 CSV 讀取的 DataFrame:")
        df_read.show()

    # 停止 SparkSession
    spark.stop()
    print("\n✅ DataFrame 基礎操作示範完成")


if __name__ == "__main__":
    main()
