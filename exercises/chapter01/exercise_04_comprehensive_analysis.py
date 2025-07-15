#!/usr/bin/env python3
"""
第1章練習4：綜合分析
整合前面練習的綜合數據分析專案
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, max as spark_max, min as spark_min, \
    sum as spark_sum, desc, asc, round as spark_round, stddev, percentile_approx, \
    row_number, rank, dense_rank, lag, lead
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("綜合分析練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第1章練習4：綜合分析 ===")
    
    # 1. 創建全面的學生數據集
    print("\n1. 創建綜合學生數據集:")
    
    comprehensive_data = [
        ("Alice Chen", 20, "Computer Science", 85.5, "Taipei", "Fall", 2023, 3.2),
        ("Bob Wang", 19, "Mathematics", 92.0, "Taichung", "Fall", 2023, 3.7),
        ("Charlie Liu", 21, "Physics", 78.5, "Kaohsiung", "Spring", 2024, 2.8),
        ("Diana Zhang", 20, "Chemistry", 88.0, "Taipei", "Fall", 2023, 3.4),
        ("Eve Brown", 22, "Biology", 90.5, "Tainan", "Spring", 2024, 3.6),
        ("Frank Davis", 19, "Computer Science", 95.0, "Taipei", "Fall", 2023, 3.9),
        ("Grace Wilson", 20, "Mathematics", 82.5, "Taichung", "Spring", 2024, 3.1),
        ("Henry Johnson", 21, "Physics", 87.0, "Kaohsiung", "Fall", 2023, 3.3),
        ("Ivy Lee", 19, "Chemistry", 91.5, "Taipei", "Spring", 2024, 3.7),
        ("Jack Smith", 20, "Biology", 84.0, "Tainan", "Fall", 2023, 3.2),
        ("Kate Brown", 21, "Computer Science", 89.0, "Taipei", "Spring", 2024, 3.5),
        ("Leo Davis", 19, "Mathematics", 76.5, "Taichung", "Fall", 2023, 2.9),
        ("Mia Wilson", 22, "Physics", 93.5, "Kaohsiung", "Spring", 2024, 3.8),
        ("Nick Taylor", 20, "Chemistry", 81.0, "Taipei", "Fall", 2023, 3.0),
        ("Olivia Johnson", 21, "Biology", 86.5, "Tainan", "Spring", 2024, 3.3),
        ("Paul Chen", 18, "Computer Science", 94.0, "Taipei", "Fall", 2023, 3.8),
        ("Quinn Wang", 23, "Mathematics", 79.0, "Taichung", "Spring", 2024, 2.9),
        ("Rita Liu", 20, "Physics", 88.5, "Kaohsiung", "Fall", 2023, 3.4),
        ("Sam Zhang", 19, "Chemistry", 92.5, "Taipei", "Spring", 2024, 3.7),
        ("Tina Brown", 22, "Biology", 85.5, "Tainan", "Fall", 2023, 3.2)
    ]
    
    columns = ["name", "age", "department", "score", "city", "semester", "year", "gpa"]
    students_df = spark.createDataFrame(comprehensive_data, columns)
    
    print("綜合學生數據:")
    students_df.show(truncate=False)
    
    print(f"總學生數: {students_df.count()}")
    print(f"涵蓋科系數: {students_df.select('department').distinct().count()}")
    print(f"涵蓋城市數: {students_df.select('city').distinct().count()}")
    
    # 2. 基礎統計分析
    print("\n2. 基礎統計分析:")
    
    # 2.1 整體成績統計
    print("\n2.1 整體成績統計:")
    overall_stats = students_df.agg(
        count("*").alias("total_students"),
        avg("score").alias("avg_score"),
        spark_max("score").alias("max_score"),
        spark_min("score").alias("min_score"),
        stddev("score").alias("stddev_score"),
        percentile_approx("score", 0.5).alias("median_score"),
        percentile_approx("score", 0.25).alias("q1_score"),
        percentile_approx("score", 0.75).alias("q3_score")
    )
    
    stats_row = overall_stats.collect()[0]
    print(f"學生總數: {stats_row['total_students']}")
    print(f"平均成績: {stats_row['avg_score']:.2f}")
    print(f"最高成績: {stats_row['max_score']:.1f}")
    print(f"最低成績: {stats_row['min_score']:.1f}")
    print(f"標準差: {stats_row['stddev_score']:.2f}")
    print(f"中位數: {stats_row['median_score']:.1f}")
    print(f"第一四分位數: {stats_row['q1_score']:.1f}")
    print(f"第三四分位數: {stats_row['q3_score']:.1f}")
    
    # 2.2 年齡分布分析
    print("\n2.2 年齡分布分析:")
    age_distribution = students_df.groupBy("age").agg(
        count("*").alias("student_count"),
        avg("score").alias("avg_score")
    ).orderBy("age")
    
    age_distribution.show()
    
    # 3. 按科系分析
    print("\n3. 按科系分析:")
    
    department_analysis = students_df.groupBy("department").agg(
        count("*").alias("student_count"),
        avg("score").alias("avg_score"),
        spark_max("score").alias("max_score"),
        spark_min("score").alias("min_score"),
        avg("gpa").alias("avg_gpa")
    ).orderBy(desc("avg_score"))
    
    print("科系成績排名:")
    department_analysis.show()
    
    # 找出每個科系的最優秀學生
    print("\n每個科系的最優秀學生:")
    window_dept = Window.partitionBy("department").orderBy(desc("score"))
    
    top_students_by_dept = students_df.withColumn(
        "rank_in_dept", 
        row_number().over(window_dept)
    ).filter(col("rank_in_dept") == 1).drop("rank_in_dept")
    
    top_students_by_dept.select("name", "department", "score", "gpa").show()
    
    # 4. 地理分布分析
    print("\n4. 地理分布分析:")
    
    city_analysis = students_df.groupBy("city").agg(
        count("*").alias("student_count"),
        avg("score").alias("avg_score"),
        avg("gpa").alias("avg_gpa")
    ).orderBy(desc("student_count"))
    
    print("城市學生分布:")
    city_analysis.show()
    
    # 城市和科系的交叉分析
    print("\n城市-科系交叉分析:")
    city_dept_analysis = students_df.groupBy("city", "department").agg(
        count("*").alias("student_count"),
        avg("score").alias("avg_score")
    ).orderBy("city", "department")
    
    city_dept_analysis.show(truncate=False)
    
    # 5. 學期表現分析
    print("\n5. 學期表現分析:")
    
    semester_analysis = students_df.groupBy("semester", "year").agg(
        count("*").alias("student_count"),
        avg("score").alias("avg_score"),
        avg("gpa").alias("avg_gpa")
    ).orderBy("year", "semester")
    
    print("學期表現對比:")
    semester_analysis.show()
    
    # 6. 成績等級分析
    print("\n6. 成績等級分析:")
    
    # 添加成績等級
    students_with_grade = students_df.withColumn(
        "grade",
        when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .when(col("score") >= 60, "D")
        .otherwise("F")
    )
    
    grade_distribution = students_with_grade.groupBy("grade").agg(
        count("*").alias("student_count"),
        (count("*") * 100.0 / students_df.count()).alias("percentage")
    ).orderBy("grade")
    
    print("成績等級分布:")
    grade_distribution.withColumn("percentage", spark_round("percentage", 1)).show()
    
    # 按科系的成績等級分布
    print("\n各科系成績等級分布:")
    grade_by_dept = students_with_grade.groupBy("department", "grade").count() \
        .orderBy("department", "grade")
    
    grade_by_dept.show()
    
    # 7. 進階分析 - 排名和百分位
    print("\n7. 進階分析 - 排名和百分位:")
    
    # 全校排名
    window_overall = Window.orderBy(desc("score"))
    
    students_with_rank = students_df.withColumn(
        "overall_rank", 
        row_number().over(window_overall)
    ).withColumn(
        "dense_rank",
        dense_rank().over(window_overall)
    )
    
    print("前10名學生:")
    students_with_rank.filter(col("overall_rank") <= 10) \
        .select("overall_rank", "name", "department", "score", "gpa") \
        .show()
    
    # 8. 相關性分析
    print("\n8. 相關性分析:")
    
    # 年齡與成績的關係
    age_score_corr = students_df.stat.corr("age", "score")
    print(f"年齡與成績相關係數: {age_score_corr:.3f}")
    
    # GPA與成績的關係
    gpa_score_corr = students_df.stat.corr("gpa", "score")
    print(f"GPA與成績相關係數: {gpa_score_corr:.3f}")
    
    # 年齡與GPA的關係
    age_gpa_corr = students_df.stat.corr("age", "gpa")
    print(f"年齡與GPA相關係數: {age_gpa_corr:.3f}")
    
    # 9. 異常值檢測
    print("\n9. 異常值檢測:")
    
    # 使用IQR方法檢測異常值
    q1 = stats_row['q1_score']
    q3 = stats_row['q3_score']
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    outliers = students_df.filter(
        (col("score") < lower_bound) | (col("score") > upper_bound)
    )
    
    print(f"異常值範圍: < {lower_bound:.1f} 或 > {upper_bound:.1f}")
    print(f"檢測到 {outliers.count()} 個異常值:")
    outliers.select("name", "department", "score").show()
    
    # 10. 綜合報告
    print("\n10. 綜合分析報告:")
    
    # 最優秀科系
    best_dept = department_analysis.first()
    print(f"最優秀科系: {best_dept['department']} (平均成績: {best_dept['avg_score']:.2f})")
    
    # 最大城市
    biggest_city = city_analysis.first()
    print(f"學生最多城市: {biggest_city['city']} ({biggest_city['student_count']} 人)")
    
    # 成績分布特徵
    a_grade_pct = grade_distribution.filter(col("grade") == "A").collect()[0]["percentage"]
    print(f"A等成績比例: {a_grade_pct:.1f}%")
    
    # 年齡分布
    avg_age = students_df.agg(avg("age")).collect()[0][0]
    print(f"平均年齡: {avg_age:.1f} 歲")
    
    # 11. 趨勢分析
    print("\n11. 趨勢分析:")
    
    # 使用窗口函數分析成績趨勢
    window_trend = Window.partitionBy("department").orderBy("name")
    
    trend_analysis = students_df.withColumn(
        "prev_score",
        lag("score", 1).over(window_trend)
    ).withColumn(
        "score_diff",
        col("score") - col("prev_score")
    )
    
    # 顯示有前一個比較基準的記錄
    print("科系內成績變化趨勢 (按姓名排序):")
    trend_analysis.filter(col("prev_score").isNotNull()) \
        .select("name", "department", "score", "prev_score", "score_diff") \
        .show()
    
    # 12. 保存分析結果
    print("\n12. 保存分析結果:")
    
    # 創建最終分析結果
    final_analysis = students_with_grade.join(
        students_with_rank.select("name", "overall_rank"),
        "name"
    ).select(
        "name", "age", "department", "city", "semester", "year",
        "score", "gpa", "grade", "overall_rank"
    ).orderBy("overall_rank")
    
    print("最終綜合分析結果:")
    final_analysis.show(truncate=False)
    
    # 統計摘要
    print("\n=== 分析摘要 ===")
    summary_stats = {
        "總學生數": students_df.count(),
        "平均成績": f"{stats_row['avg_score']:.2f}",
        "成績標準差": f"{stats_row['stddev_score']:.2f}",
        "最優科系": best_dept['department'],
        "學生最多城市": biggest_city['city'],
        "A等比例": f"{a_grade_pct:.1f}%",
        "檢測異常值": outliers.count(),
        "年齡成績相關性": f"{age_score_corr:.3f}"
    }
    
    for key, value in summary_stats.items():
        print(f"- {key}: {value}")
    
    # 清理資源
    spark.stop()
    print("\n綜合分析練習完成！")

if __name__ == "__main__":
    main()