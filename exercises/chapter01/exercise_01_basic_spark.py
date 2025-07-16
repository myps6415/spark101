#!/usr/bin/env python3
"""
第1章練習1：創建你的第一個 Spark 應用
學生成績分析練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("學生成績分析").master("local[*]").getOrCreate()
    )

    print("=== 第1章練習1：學生成績分析 ===")

    # 學生數據
    students_data = [
        ("Alice", 20, "Computer Science", 85.5),
        ("Bob", 19, "Mathematics", 92.0),
        ("Charlie", 21, "Physics", 78.5),
        ("Diana", 20, "Chemistry", 88.0),
        ("Eve", 22, "Biology", 90.5),
        ("Frank", 19, "Computer Science", 95.0),
        ("Grace", 20, "Mathematics", 82.5),
        ("Henry", 21, "Physics", 87.0),
        ("Ivy", 19, "Chemistry", 91.5),
        ("Jack", 20, "Biology", 84.0),
    ]

    columns = ["name", "age", "department", "score"]
    students_df = spark.createDataFrame(students_data, columns)

    # 1. 顯示 DataFrame
    print("\n1. 學生數據表格:")
    students_df.show()

    # 2. 顯示 Schema
    print("\n2. DataFrame 的 Schema:")
    students_df.printSchema()

    # 3. 計算平均成績
    avg_score = students_df.agg(avg("score")).collect()[0][0]
    print(f"\n3. 平均成績: {avg_score:.2f}")

    # 4. 找出成績最高的學生
    max_score = students_df.agg(spark_max("score")).collect()[0][0]
    top_student = students_df.filter(col("score") == max_score).collect()[0]
    print(f"\n4. 成績最高的學生: {top_student.name}, 成績: {top_student.score}")

    # 額外分析
    print("\n=== 額外分析 ===")

    # 按科系統計
    print("\n5. 按科系統計:")
    dept_stats = (
        students_df.groupBy("department")
        .agg(
            avg("score").alias("avg_score"),
            spark_max("score").alias("max_score"),
            col("department").alias("dept"),
        )
        .orderBy("avg_score", ascending=False)
    )
    dept_stats.show()

    # 成績等級分析
    print("\n6. 成績等級分布:")
    students_with_grade = students_df.withColumn(
        "grade",
        when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .otherwise("D"),
    )

    grade_distribution = students_with_grade.groupBy("grade").count().orderBy("grade")
    grade_distribution.show()

    # 清理資源
    spark.stop()
    print("\n練習完成！")


if __name__ == "__main__":
    main()
