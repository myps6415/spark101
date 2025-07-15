#!/usr/bin/env python3
"""
第1章練習3：數據清洗和轉換
數據品質處理和標準化練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, regexp_replace, trim, upper, lower, \
    split, regexp_extract, coalesce, lit, avg, stddev, abs as spark_abs
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("數據清洗和轉換練習") \
        .master("local[*]") \
        .getOrCreate()
    
    print("=== 第1章練習3：數據清洗和轉換 ===")
    
    # 1. 創建含有髒數據的學生數據集
    print("\n1. 創建含有髒數據的學生數據集:")
    
    dirty_data = [
        ("  Alice Chen  ", "20", "Computer Science", "85.5", "alice@email.com"),
        ("Bob Wang", "", "Mathematics", "92.0", "BOB@EMAIL.COM"),
        ("Charlie Liu", "21", "physics", "78.5", "charlie.liu@email"),
        ("", "20", "Chemistry", "88.0", "diana@email.com"),
        ("Eve Brown", "22", "Biology", "", "eve@email.com"),
        ("Frank Davis", "abc", "Computer Science", "95.0", "frank@email.com"),
        ("Grace Wilson", "20", "MATHEMATICS", "82.5", "grace@INVALID"),
        ("Henry  Johnson", "21", "", "87.0", "henry@email.com"),
        ("Ivy   Lee", "19", "Chemistry", "-15.5", "ivy@email.com"),
        ("Jack Smith", "20", "Biology", "999.0", ""),
        ("Kate Brown", "25", "Computer Science", "89.0", "kate@email.com"),
        (None, "19", "Mathematics", "76.5", "leo@email.com"),
        ("Mia Davis", "18", "Physics", "93.5", "mia@email.com"),
        ("Nick Wilson", "20", "Chemistry", "81.0", "nick@email.com"),
        ("Olivia Taylor", "21", "Biology", "86.5", "olivia@email.com")
    ]
    
    columns = ["name", "age", "department", "score", "email"]
    dirty_df = spark.createDataFrame(dirty_data, columns)
    
    print("原始髒數據:")
    dirty_df.show(truncate=False)
    
    print(f"原始記錄數: {dirty_df.count()}")
    
    # 2. 檢查數據品質問題
    print("\n2. 數據品質問題分析:")
    
    # 檢查空值和缺失值
    print("\n2.1 空值統計:")
    for column in dirty_df.columns:
        null_count = dirty_df.filter(
            col(column).isNull() | 
            (col(column) == "") | 
            (col(column) == " ")
        ).count()
        print(f"{column}: {null_count} 個空值")
    
    # 檢查數據類型問題
    print("\n2.2 數據類型問題:")
    # 檢查年齡是否為數字
    non_numeric_age = dirty_df.filter(~col("age").rlike("^[0-9]+$")).count()
    print(f"非數字年齡: {non_numeric_age} 筆")
    
    # 檢查成績範圍
    try:
        invalid_scores = dirty_df.filter(
            (col("score").cast("double") < 0) | 
            (col("score").cast("double") > 100)
        ).count()
        print(f"無效成績範圍: {invalid_scores} 筆")
    except:
        print("成績欄位包含非數字值")
    
    # 3. 數據清洗步驟
    print("\n3. 開始數據清洗:")
    
    # 3.1 清理姓名欄位
    print("\n3.1 清理姓名欄位:")
    cleaned_step1 = dirty_df.withColumn(
        "name_cleaned",
        trim(regexp_replace(col("name"), "\\s+", " "))  # 移除多餘空格
    )
    
    # 處理空姓名
    cleaned_step1 = cleaned_step1.withColumn(
        "name_cleaned",
        when(
            (col("name_cleaned").isNull()) | 
            (col("name_cleaned") == "") | 
            (col("name_cleaned") == " "),
            "Unknown Student"
        ).otherwise(col("name_cleaned"))
    )
    
    print("姓名清理後:")
    cleaned_step1.select("name", "name_cleaned").show(truncate=False)
    
    # 3.2 清理年齡欄位
    print("\n3.2 清理年齡欄位:")
    
    # 計算有效年齡的平均值用於填補
    valid_ages = dirty_df.filter(col("age").rlike("^[0-9]+$")).select(col("age").cast("int"))
    avg_age = valid_ages.agg(avg("age")).collect()[0][0]
    print(f"有效年齡平均值: {avg_age:.1f}")
    
    cleaned_step2 = cleaned_step1.withColumn(
        "age_cleaned",
        when(
            col("age").rlike("^[0-9]+$"), 
            col("age").cast("int")
        ).otherwise(int(avg_age))  # 用平均值填補無效年齡
    )
    
    print("年齡清理後:")
    cleaned_step2.select("age", "age_cleaned").show()
    
    # 3.3 標準化科系名稱
    print("\n3.3 標準化科系名稱:")
    
    department_mapping = {
        "computer science": "Computer Science",
        "mathematics": "Mathematics", 
        "physics": "Physics",
        "chemistry": "Chemistry",
        "biology": "Biology"
    }
    
    cleaned_step3 = cleaned_step2.withColumn("department_cleaned", lower(trim(col("department"))))
    
    # 應用標準化映射
    for old_name, new_name in department_mapping.items():
        cleaned_step3 = cleaned_step3.withColumn(
            "department_cleaned",
            when(col("department_cleaned") == old_name, new_name)
            .otherwise(col("department_cleaned"))
        )
    
    # 處理空科系
    cleaned_step3 = cleaned_step3.withColumn(
        "department_cleaned",
        when(
            (col("department_cleaned").isNull()) | 
            (col("department_cleaned") == ""),
            "Unknown Department"
        ).otherwise(col("department_cleaned"))
    )
    
    print("科系標準化後:")
    cleaned_step3.select("department", "department_cleaned").distinct().show()
    
    # 3.4 清理成績欄位
    print("\n3.4 清理成績欄位:")
    
    # 計算有效成績的統計值
    valid_scores = dirty_df.filter(
        col("score").rlike("^[0-9]+\\.?[0-9]*$")
    ).select(col("score").cast("double").alias("score_num"))
    
    score_stats = valid_scores.agg(
        avg("score_num").alias("avg_score"),
        stddev("score_num").alias("std_score")
    ).collect()[0]
    
    avg_score = score_stats["avg_score"]
    std_score = score_stats["std_score"]
    
    print(f"有效成績統計 - 平均值: {avg_score:.2f}, 標準差: {std_score:.2f}")
    
    # 清理成績：處理無效值和異常值
    cleaned_step4 = cleaned_step3.withColumn(
        "score_cleaned",
        when(
            col("score").rlike("^[0-9]+\\.?[0-9]*$"),
            col("score").cast("double")
        ).otherwise(avg_score)  # 用平均值填補無效成績
    )
    
    # 處理異常值（超出合理範圍）
    cleaned_step4 = cleaned_step4.withColumn(
        "score_cleaned",
        when(
            (col("score_cleaned") < 0) | (col("score_cleaned") > 100),
            avg_score
        ).otherwise(col("score_cleaned"))
    )
    
    print("成績清理後:")
    cleaned_step4.select("score", "score_cleaned").show()
    
    # 3.5 清理郵箱欄位
    print("\n3.5 清理郵箱欄位:")
    
    # 郵箱格式驗證和清理
    email_pattern = "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
    
    cleaned_final = cleaned_step4.withColumn(
        "email_cleaned",
        when(
            col("email").rlike(email_pattern),
            lower(trim(col("email")))  # 標準化為小寫
        ).otherwise("invalid@email.com")  # 無效郵箱用預設值
    )
    
    print("郵箱清理後:")
    cleaned_final.select("email", "email_cleaned").show(truncate=False)
    
    # 4. 最終清理結果
    print("\n4. 最終清理結果:")
    
    final_cleaned = cleaned_final.select(
        col("name_cleaned").alias("name"),
        col("age_cleaned").alias("age"),
        col("department_cleaned").alias("department"),
        col("score_cleaned").alias("score"),
        col("email_cleaned").alias("email")
    )
    
    print("清理後的數據:")
    final_cleaned.show(truncate=False)
    
    # 5. 數據品質驗證
    print("\n5. 數據品質驗證:")
    
    # 驗證清理效果
    quality_check = {
        "總記錄數": final_cleaned.count(),
        "空姓名": final_cleaned.filter(col("name") == "Unknown Student").count(),
        "無效年齡": final_cleaned.filter((col("age") < 18) | (col("age") > 30)).count(),
        "未知科系": final_cleaned.filter(col("department") == "Unknown Department").count(),
        "無效成績": final_cleaned.filter((col("score") < 0) | (col("score") > 100)).count(),
        "無效郵箱": final_cleaned.filter(col("email") == "invalid@email.com").count()
    }
    
    print("數據品質統計:")
    for metric, count in quality_check.items():
        print(f"- {metric}: {count}")
    
    # 6. 額外的數據轉換
    print("\n6. 額外的數據轉換:")
    
    # 6.1 添加成績等級
    final_with_grade = final_cleaned.withColumn(
        "grade",
        when(col("score") >= 90, "A")
        .when(col("score") >= 80, "B")
        .when(col("score") >= 70, "C")
        .when(col("score") >= 60, "D")
        .otherwise("F")
    )
    
    # 6.2 添加年齡組
    final_with_groups = final_with_grade.withColumn(
        "age_group",
        when(col("age") <= 19, "Freshman")
        .when(col("age") <= 21, "Sophomore")
        .otherwise("Senior")
    )
    
    # 6.3 添加郵箱域名
    final_complete = final_with_groups.withColumn(
        "email_domain",
        regexp_extract(col("email"), "@([^.]+)", 1)
    )
    
    print("完整的清理和轉換結果:")
    final_complete.show(truncate=False)
    
    # 7. 清理效果統計
    print("\n7. 清理效果統計:")
    
    before_after = {
        "清理前記錄數": dirty_df.count(),
        "清理後記錄數": final_complete.count(),
        "數據保留率": f"{(final_complete.count() / dirty_df.count() * 100):.1f}%"
    }
    
    print("清理效果:")
    for metric, value in before_after.items():
        print(f"- {metric}: {value}")
    
    # 按科系統計清理後的數據
    print("\n科系分布統計:")
    final_complete.groupBy("department").count().orderBy("count", ascending=False).show()
    
    print("\n成績等級分布:")
    final_complete.groupBy("grade").count().orderBy("grade").show()
    
    # 清理資源
    spark.stop()
    print("\n數據清洗練習完成！")

if __name__ == "__main__":
    main()