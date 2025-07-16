#!/usr/bin/env python3
"""
第1章：Spark 基礎概念 - Hello Spark
這是你的第一個 Spark 程式
"""

from pyspark.sql import SparkSession


def main():
    # 創建 SparkSession
    spark = SparkSession.builder.appName("HelloSpark").master("local[*]").getOrCreate()

    print("🎉 歡迎來到 Spark 101！")
    print(f"Spark 版本: {spark.version}")
    print(f"Spark 應用程式名稱: {spark.sparkContext.appName}")

    # 創建簡單的 DataFrame
    data = [
        ("Alice", 25, "Engineer"),
        ("Bob", 30, "Manager"),
        ("Charlie", 35, "Designer"),
        ("Diana", 28, "Analyst"),
    ]

    columns = ["name", "age", "job"]
    df = spark.createDataFrame(data, columns)

    print("\n📊 創建的 DataFrame:")
    df.show()

    print("\n📋 DataFrame 結構:")
    df.printSchema()

    # 簡單的操作
    print("\n🔍 年齡大於 28 的人:")
    df.filter(df.age > 28).show()

    # 停止 SparkSession
    spark.stop()
    print("\n✅ Spark 應用程式結束")


if __name__ == "__main__":
    main()
