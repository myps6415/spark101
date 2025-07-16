#!/usr/bin/env python3
"""
第1章練習2：DataFrame 基本操作
複雜數據分析練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import row_number, when
from pyspark.sql.window import Window


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("DataFrame基本操作練習")
        .master("local[*]")
        .getOrCreate()
    )

    print("=== 第1章練習2：DataFrame 基本操作 ===")

    # 使用更大的學生數據集
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
        ("Kate", 21, "Computer Science", 89.0),
        ("Leo", 19, "Mathematics", 76.5),
        ("Mia", 22, "Physics", 93.5),
        ("Nick", 20, "Chemistry", 81.0),
        ("Olivia", 21, "Biology", 86.5),
    ]

    columns = ["name", "age", "department", "score"]
    students_df = spark.createDataFrame(students_data, columns)

    print("\n原始數據:")
    students_df.show()

    # 1. 篩選出成績大於80分的學生
    print("\n1. 成績大於80分的學生:")
    high_score_students = students_df.filter(col("score") > 80)
    high_score_students.show()

    # 2. 按科系分組，計算每個科系的平均成績
    print("\n2. 每個科系的平均成績:")
    dept_stats = (
        students_df.groupBy("department")
        .agg(avg("score").alias("avg_score"), count("*").alias("student_count"))
        .orderBy("avg_score", ascending=False)
    )
    dept_stats.show()

    # 3. 添加一個成績等級欄位
    print("\n3. 添加成績等級:")
    students_with_grade = students_df.withColumn(
        "grade",
        when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .otherwise("D"),
    )
    students_with_grade.select("name", "department", "score", "grade").show()

    # 4. 統計每個等級的學生人數
    print("\n4. 每個等級的學生人數:")
    grade_stats = students_with_grade.groupBy("grade").count().orderBy("grade")
    grade_stats.show()

    # 5. 找出每個科系成績最高的學生
    print("\n5. 每個科系成績最高的學生:")
    window_spec = Window.partitionBy("department").orderBy(col("score").desc())
    top_students_by_dept = (
        students_df.withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") == 1)
        .drop("rank")
    )
    top_students_by_dept.show()

    # 額外分析：年齡和成績的關係
    print("\n=== 額外分析 ===")
    print("\n6. 年齡分組分析:")
    age_analysis = (
        students_df.withColumn(
            "age_group",
            when(col("age") <= 19, "Young")
            .when(col("age") <= 21, "Middle")
            .otherwise("Senior"),
        )
        .groupBy("age_group")
        .agg(avg("score").alias("avg_score"), count("*").alias("count"))
    )
    age_analysis.show()

    # 7. 多條件篩選
    print("\n7. Computer Science 科系且成績大於85的學生:")
    cs_high_performers = students_df.filter(
        (col("department") == "Computer Science") & (col("score") > 85)
    )
    cs_high_performers.show()

    # 清理資源
    spark.stop()
    print("\n練習完成！")


if __name__ == "__main__":
    main()
