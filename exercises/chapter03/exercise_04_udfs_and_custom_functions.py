#!/usr/bin/env python3
"""
第3章練習4：自定義函數和 UDF
用戶自定義函數和複雜業務邏輯實現練習
"""

import json
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (array, avg, broadcast, col, count, lit,
                                   map_from_arrays)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import pandas_udf, struct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import udf, when
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               MapType, StringType, StructField, StructType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("UDF和自定義函數練習")
        .master("local[*]")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    print("=== 第3章練習4：自定義函數和 UDF ===")

    # 1. 創建測試數據
    print("\n1. 創建測試數據:")

    # 客戶交易數據
    transaction_data = [
        (
            "CUST001",
            "Alice Johnson",
            "alice@email.com",
            "2024-01-15",
            1250.00,
            "electronics,books",
            "premium",
            85,
        ),
        (
            "CUST002",
            "Bob Chen",
            "bob@email.com",
            "2024-01-16",
            890.50,
            "clothing,accessories",
            "gold",
            72,
        ),
        (
            "CUST003",
            "Charlie Wu",
            "charlie.wu@email.com",
            "2024-01-17",
            2100.00,
            "electronics",
            "premium",
            91,
        ),
        (
            "CUST004",
            "Diana Lin",
            "diana_lin@email.com",
            "2024-01-18",
            567.25,
            "books,toys",
            "silver",
            68,
        ),
        (
            "CUST005",
            "Eve Brown",
            "eve.brown@email.com",
            "2024-01-19",
            1456.75,
            "clothing",
            "gold",
            78,
        ),
        (
            "CUST006",
            "Frank Davis",
            "frank@email.com",
            "2024-01-20",
            789.00,
            "electronics,home",
            "silver",
            82,
        ),
        (
            "CUST007",
            "Grace Wilson",
            "grace.wilson@email.com",
            "2024-01-21",
            345.50,
            "books",
            "bronze",
            58,
        ),
        (
            "CUST008",
            "Henry Johnson",
            "henry@email.com",
            "2024-01-22",
            1890.25,
            "electronics,clothing",
            "premium",
            88,
        ),
        (
            "CUST009",
            "Ivy Lee",
            "ivy.lee@email.com",
            "2024-01-23",
            623.75,
            "toys,books",
            "silver",
            75,
        ),
        (
            "CUST010",
            "Jack Smith",
            "jack.smith@email.com",
            "2024-01-24",
            1234.00,
            "home,electronics",
            "gold",
            80,
        ),
    ]

    transaction_columns = [
        "customer_id",
        "name",
        "email",
        "transaction_date",
        "amount",
        "categories",
        "tier",
        "satisfaction_score",
    ]
    df = spark.createDataFrame(transaction_data, transaction_columns)

    print("原始交易數據:")
    df.show(truncate=False)

    # 2. 基本 UDF 函數
    print("\n2. 基本 UDF 函數:")

    # 2.1 字符串處理 UDF
    print("\n2.1 字符串處理 UDF:")

    # 標準化姓名
    def standardize_name(name):
        if name is None:
            return "Unknown"
        # 移除多餘空格，每個單詞首字母大寫
        return " ".join(word.capitalize() for word in name.strip().split())

    standardize_name_udf = udf(standardize_name, StringType())

    # 驗證郵箱格式
    def validate_email(email):
        if email is None:
            return False
        pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        return bool(re.match(pattern, email))

    validate_email_udf = udf(validate_email, BooleanType())

    # 應用字符串處理 UDF
    df_string_processed = df.withColumn(
        "name_standardized", standardize_name_udf(col("name"))
    ).withColumn("email_valid", validate_email_udf(col("email")))

    print("字符串處理結果:")
    df_string_processed.select(
        "name", "name_standardized", "email", "email_valid"
    ).show()

    # 2.2 數值計算 UDF
    print("\n2.2 數值計算 UDF:")

    # 計算客戶價值分數
    def calculate_customer_value(amount, satisfaction, tier):
        if amount is None or satisfaction is None or tier is None:
            return 0.0

        # 基礎分數 = 金額權重 + 滿意度權重 + 等級權重
        amount_score = min(amount / 1000 * 30, 50)  # 最高50分
        satisfaction_score = satisfaction * 0.3  # 最高30分

        tier_scores = {"premium": 20, "gold": 15, "silver": 10, "bronze": 5}
        tier_score = tier_scores.get(tier.lower(), 0)

        return round(amount_score + satisfaction_score + tier_score, 2)

    calculate_value_udf = udf(calculate_customer_value, DoubleType())

    # 等級升級建議
    def suggest_tier_upgrade(current_tier, amount, satisfaction):
        if current_tier is None:
            return "unknown"

        current = current_tier.lower()

        if current == "bronze" and amount > 500 and satisfaction > 70:
            return "silver"
        elif current == "silver" and amount > 1000 and satisfaction > 80:
            return "gold"
        elif current == "gold" and amount > 1500 and satisfaction > 85:
            return "premium"
        else:
            return current

    suggest_upgrade_udf = udf(suggest_tier_upgrade, StringType())

    # 應用數值計算 UDF
    df_value_processed = df_string_processed.withColumn(
        "customer_value_score",
        calculate_value_udf(col("amount"), col("satisfaction_score"), col("tier")),
    ).withColumn(
        "suggested_tier",
        suggest_upgrade_udf(col("tier"), col("amount"), col("satisfaction_score")),
    )

    print("數值計算結果:")
    df_value_processed.select(
        "customer_id",
        "amount",
        "satisfaction_score",
        "tier",
        "customer_value_score",
        "suggested_tier",
    ).show()

    # 3. 複雜業務邏輯 UDF
    print("\n3. 複雜業務邏輯 UDF:")

    # 3.1 分類分析 UDF
    print("\n3.1 分類分析 UDF:")

    # 解析和分析購買分類
    def analyze_categories(categories_str):
        if categories_str is None:
            return {"count": 0, "types": [], "diversity_score": 0}

        categories = [cat.strip() for cat in categories_str.split(",")]
        category_weights = {
            "electronics": 3,
            "clothing": 2,
            "books": 1,
            "toys": 1,
            "home": 2,
            "accessories": 1,
        }

        diversity_score = sum(category_weights.get(cat, 0) for cat in categories)

        return {
            "count": len(categories),
            "types": categories,
            "diversity_score": diversity_score,
        }

    # 返回複雜結構的 UDF
    category_schema = StructType(
        [
            StructField("count", IntegerType(), True),
            StructField("types", ArrayType(StringType()), True),
            StructField("diversity_score", IntegerType(), True),
        ]
    )

    analyze_categories_udf = udf(analyze_categories, category_schema)

    # 應用分類分析 UDF
    df_category_analyzed = df_value_processed.withColumn(
        "category_analysis", analyze_categories_udf(col("categories"))
    )

    print("分類分析結果:")
    df_category_analyzed.select("customer_id", "categories", "category_analysis").show(
        truncate=False
    )

    # 3.2 風險評估 UDF
    print("\n3.2 客戶風險評估 UDF:")

    # 綜合風險評估
    def assess_customer_risk(tier, amount, satisfaction, email_valid, categories_str):
        risk_score = 0
        risk_factors = []

        # 等級風險
        tier_risks = {"bronze": 3, "silver": 2, "gold": 1, "premium": 0}
        tier_risk = tier_risks.get(tier.lower() if tier else "bronze", 3)
        risk_score += tier_risk

        # 交易金額風險
        if amount is None or amount < 100:
            risk_score += 3
            risk_factors.append("low_transaction_amount")
        elif amount > 2000:
            risk_score += 1
            risk_factors.append("high_transaction_amount")

        # 滿意度風險
        if satisfaction is None or satisfaction < 60:
            risk_score += 3
            risk_factors.append("low_satisfaction")
        elif satisfaction < 75:
            risk_score += 1
            risk_factors.append("medium_satisfaction")

        # 郵箱風險
        if not email_valid:
            risk_score += 2
            risk_factors.append("invalid_email")

        # 分類多樣性風險
        if categories_str is None or len(categories_str.split(",")) == 1:
            risk_score += 1
            risk_factors.append("low_category_diversity")

        # 風險等級
        if risk_score >= 8:
            risk_level = "HIGH"
        elif risk_score >= 5:
            risk_level = "MEDIUM"
        elif risk_score >= 2:
            risk_level = "LOW"
        else:
            risk_level = "MINIMAL"

        return {
            "risk_score": risk_score,
            "risk_level": risk_level,
            "risk_factors": risk_factors,
        }

    risk_schema = StructType(
        [
            StructField("risk_score", IntegerType(), True),
            StructField("risk_level", StringType(), True),
            StructField("risk_factors", ArrayType(StringType()), True),
        ]
    )

    assess_risk_udf = udf(assess_customer_risk, risk_schema)

    # 應用風險評估 UDF
    df_risk_assessed = df_category_analyzed.withColumn(
        "risk_assessment",
        assess_risk_udf(
            col("tier"),
            col("amount"),
            col("satisfaction_score"),
            col("email_valid"),
            col("categories"),
        ),
    )

    print("風險評估結果:")
    df_risk_assessed.select(
        "customer_id", "tier", "amount", "satisfaction_score", "risk_assessment"
    ).show(truncate=False)

    # 4. Pandas UDF (向量化 UDF)
    print("\n4. Pandas UDF (向量化操作):")

    # 4.1 批量統計計算
    @pandas_udf(DoubleType())
    def calculate_z_score(amounts: pd.Series) -> pd.Series:
        """計算金額的 Z-score 標準化"""
        mean_amount = amounts.mean()
        std_amount = amounts.std()
        if std_amount == 0:
            return pd.Series([0.0] * len(amounts))
        return (amounts - mean_amount) / std_amount

    # 4.2 複雜聚合統計
    @pandas_udf(
        StructType(
            [
                StructField("percentile_25", DoubleType(), True),
                StructField("percentile_50", DoubleType(), True),
                StructField("percentile_75", DoubleType(), True),
                StructField("iqr", DoubleType(), True),
            ]
        )
    )
    def calculate_percentiles(amounts: pd.Series) -> pd.DataFrame:
        """計算百分位統計"""
        p25 = amounts.quantile(0.25)
        p50 = amounts.quantile(0.50)
        p75 = amounts.quantile(0.75)
        iqr = p75 - p25

        return pd.DataFrame(
            {
                "percentile_25": [p25] * len(amounts),
                "percentile_50": [p50] * len(amounts),
                "percentile_75": [p75] * len(amounts),
                "iqr": [iqr] * len(amounts),
            }
        )

    # 應用 Pandas UDF
    df_pandas_processed = df_risk_assessed.withColumn(
        "amount_z_score", calculate_z_score(col("amount"))
    )

    print("Pandas UDF 處理結果:")
    df_pandas_processed.select("customer_id", "amount", "amount_z_score").show()

    # 5. 條件邏輯和決策樹 UDF
    print("\n5. 條件邏輯和決策樹 UDF:")

    # 營銷活動推薦
    def recommend_marketing_campaign(
        tier, amount, satisfaction, categories_str, risk_level
    ):
        recommendations = []

        # 基於等級的推薦
        if tier and tier.lower() == "premium":
            recommendations.append("VIP_EXCLUSIVE_OFFERS")
        elif tier and tier.lower() == "gold":
            recommendations.append("GOLD_MEMBER_DISCOUNTS")

        # 基於金額的推薦
        if amount and amount > 1500:
            recommendations.append("HIGH_VALUE_REWARDS")
        elif amount and amount < 500:
            recommendations.append("SPENDING_INCENTIVES")

        # 基於滿意度的推薦
        if satisfaction and satisfaction < 70:
            recommendations.append("SATISFACTION_IMPROVEMENT")
        elif satisfaction and satisfaction > 85:
            recommendations.append("LOYALTY_REWARDS")

        # 基於分類的推薦
        if categories_str:
            categories = [cat.strip().lower() for cat in categories_str.split(",")]
            if "electronics" in categories:
                recommendations.append("TECH_DEALS")
            if "clothing" in categories:
                recommendations.append("FASHION_ALERTS")
            if "books" in categories:
                recommendations.append("READING_CLUB")

        # 基於風險的推薦
        if risk_level == "HIGH":
            recommendations.append("RETENTION_CAMPAIGN")
        elif risk_level == "MINIMAL":
            recommendations.append("EXPANSION_OPPORTUNITIES")

        return recommendations if recommendations else ["GENERAL_NEWSLETTER"]

    recommend_campaign_udf = udf(recommend_marketing_campaign, ArrayType(StringType()))

    # 客戶生命週期階段判斷
    def determine_lifecycle_stage(tier, amount, satisfaction, categories_count):
        # 新客戶
        if tier and tier.lower() == "bronze" and amount < 500:
            return "NEW_CUSTOMER"

        # 成長期客戶
        if tier and tier.lower() in ["silver", "gold"] and satisfaction > 75:
            return "GROWING_CUSTOMER"

        # 成熟客戶
        if tier and tier.lower() in ["gold", "premium"] and amount > 1000:
            return "MATURE_CUSTOMER"

        # 風險客戶
        if satisfaction < 65 or (amount < 300 and tier and tier.lower() != "bronze"):
            return "AT_RISK_CUSTOMER"

        # 流失風險
        if satisfaction < 55:
            return "CHURN_RISK"

        return "REGULAR_CUSTOMER"

    lifecycle_stage_udf = udf(determine_lifecycle_stage, StringType())

    # 應用決策樹 UDF
    df_final = df_pandas_processed.withColumn(
        "marketing_recommendations",
        recommend_campaign_udf(
            col("tier"),
            col("amount"),
            col("satisfaction_score"),
            col("categories"),
            col("risk_assessment.risk_level"),
        ),
    ).withColumn(
        "lifecycle_stage",
        lifecycle_stage_udf(
            col("tier"),
            col("amount"),
            col("satisfaction_score"),
            col("category_analysis.count"),
        ),
    )

    print("營銷推薦和生命週期分析:")
    df_final.select(
        "customer_id", "tier", "lifecycle_stage", "marketing_recommendations"
    ).show(truncate=False)

    # 6. 性能比較
    print("\n6. UDF 性能比較:")

    # 6.1 UDF vs 內建函數性能測試
    print("\n6.1 簡單分類處理性能比較:")

    # 使用 UDF
    def simple_categorize_udf(amount):
        if amount is None:
            return "unknown"
        elif amount > 1500:
            return "high"
        elif amount > 800:
            return "medium"
        else:
            return "low"

    categorize_udf = udf(simple_categorize_udf, StringType())

    # 使用內建函數
    df_builtin = df.withColumn(
        "amount_category_builtin",
        when(col("amount") > 1500, "high")
        .when(col("amount") > 800, "medium")
        .otherwise("low"),
    )

    df_udf = df.withColumn("amount_category_udf", categorize_udf(col("amount")))

    print("內建函數結果:")
    df_builtin.select("customer_id", "amount", "amount_category_builtin").show(5)

    print("UDF 結果:")
    df_udf.select("customer_id", "amount", "amount_category_udf").show(5)

    # 7. UDF 注册為 SQL 函數
    print("\n7. 註冊 UDF 為 SQL 函數:")

    # 註冊 UDF
    spark.udf.register("standardize_name", standardize_name_udf)
    spark.udf.register("validate_email", validate_email_udf)
    spark.udf.register("calculate_customer_value", calculate_value_udf)

    # 創建臨時視圖
    df.createOrReplaceTempView("transactions")

    # 使用 SQL 調用 UDF
    sql_result = spark.sql(
        """
        SELECT 
            customer_id,
            standardize_name(name) as clean_name,
            validate_email(email) as email_valid,
            calculate_customer_value(amount, satisfaction_score, tier) as value_score
        FROM transactions
        WHERE amount > 800
    """
    )

    print("SQL UDF 調用結果:")
    sql_result.show()

    # 8. 錯誤處理和健壯性
    print("\n8. UDF 錯誤處理:")

    # 健壯的 UDF 示例
    def robust_calculation(value1, value2, operation):
        try:
            if value1 is None or value2 is None:
                return None

            if operation == "divide":
                return float(value1) / float(value2) if value2 != 0 else None
            elif operation == "multiply":
                return float(value1) * float(value2)
            elif operation == "add":
                return float(value1) + float(value2)
            else:
                return None
        except (ValueError, TypeError, ZeroDivisionError):
            return None

    robust_calc_udf = udf(robust_calculation, DoubleType())

    # 測試錯誤處理
    test_data = [
        ("T001", 100, 10, "divide"),
        ("T002", 100, 0, "divide"),  # 除零錯誤
        ("T003", None, 10, "multiply"),  # 空值
        ("T004", 100, 5, "invalid"),  # 無效操作
    ]

    test_df = spark.createDataFrame(test_data, ["id", "val1", "val2", "op"])
    test_result = test_df.withColumn(
        "result", robust_calc_udf(col("val1"), col("val2"), col("op"))
    )

    print("錯誤處理測試:")
    test_result.show()

    # 9. UDF 最佳實踐總結
    print("\n9. UDF 最佳實踐總結:")

    # 統計各種 UDF 的使用情況
    udf_summary = {
        "字符串處理 UDF": 2,
        "數值計算 UDF": 2,
        "複雜結構 UDF": 2,
        "Pandas UDF": 2,
        "決策邏輯 UDF": 2,
        "SQL 註冊 UDF": 3,
        "錯誤處理 UDF": 1,
    }

    print("UDF 使用統計:")
    for category, count in udf_summary.items():
        print(f"- {category}: {count} 個")

    best_practices = [
        "優先使用內建函數，性能更好",
        "UDF 中添加空值和錯誤處理",
        "使用 Pandas UDF 進行向量化操作",
        "複雜邏輯可以返回結構化數據",
        "註冊常用 UDF 為 SQL 函數",
        "避免在 UDF 中進行 I/O 操作",
        "測試 UDF 的邊界情況",
    ]

    print("\nUDF 最佳實踐:")
    for i, practice in enumerate(best_practices, 1):
        print(f"{i}. {practice}")

    # 10. 最終結果展示
    print("\n10. 最終綜合結果:")

    final_summary = df_final.select(
        "customer_id",
        "name_standardized",
        "tier",
        "customer_value_score",
        "suggested_tier",
        "lifecycle_stage",
        "risk_assessment.risk_level",
        "category_analysis.diversity_score",
    )

    print("客戶綜合分析結果:")
    final_summary.show(truncate=False)

    # 統計摘要
    print("\n處理統計:")
    print(f"- 總客戶數: {df.count()}")
    print(
        f"- 有效郵箱比例: {df_string_processed.filter(col('email_valid') == True).count() / df.count() * 100:.1f}%"
    )
    print(
        f"- 高風險客戶: {df_risk_assessed.filter(col('risk_assessment.risk_level') == 'HIGH').count()}"
    )
    print(
        f"- 建議升級客戶: {df_value_processed.filter(col('suggested_tier') != col('tier')).count()}"
    )

    # 清理資源
    spark.stop()
    print("\nUDF 和自定義函數練習完成！")


if __name__ == "__main__":
    main()
