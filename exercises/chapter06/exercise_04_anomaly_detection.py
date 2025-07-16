#!/usr/bin/env python3
"""
第6章練習4：異常檢測
使用 Spark MLlib 進行異常檢測和離群點分析
"""

import random
from datetime import datetime, timedelta

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, OneVsRest
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator)
from pyspark.ml.feature import PCA, StandardScaler, VectorAssembler
from pyspark.ml.stat import Correlation
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (asc, avg, col, count, desc, expr, isnan,
                                   isnull)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import percentile_approx
from pyspark.sql.functions import pow as spark_pow
from pyspark.sql.functions import rand
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import row_number, sqrt, stddev
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               StringType, StructField, StructType)
from pyspark.sql.window import Window


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("異常檢測練習")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    print("=== 第6章練習4：異常檢測 ===")

    # 1. 創建正常和異常數據
    print("\n1. 創建包含異常的數據集:")

    # 1.1 生成正常交易數據
    print("\n1.1 生成正常交易數據:")

    def generate_normal_transactions():
        transactions = []
        for i in range(1, 1001):  # 1000筆正常交易
            # 正常交易特征
            amount = random.uniform(10, 500)  # 正常金額範圍
            transaction_hour = random.randint(8, 22)  # 正常時間
            merchant_category = random.choice([1, 2, 3, 4, 5])  # 常見商家類型

            # 基於時間調整金額
            if 12 <= transaction_hour <= 14:  # 午餐時間
                if merchant_category == 1:  # 餐廳
                    amount = random.uniform(15, 80)
            elif 18 <= transaction_hour <= 20:  # 晚餐時間
                if merchant_category == 1:
                    amount = random.uniform(25, 120)

            # 位置相關特征
            location_risk_score = random.uniform(0.1, 0.3)  # 低風險位置
            distance_from_home = random.uniform(0, 50)  # 正常距離

            # 用戶行為特征
            frequency_last_hour = random.randint(0, 3)  # 正常頻率
            avg_amount_last_30_days = random.uniform(50, 300)

            # 設備和渠道特征
            is_online = random.choice([True, False])
            device_risk_score = random.uniform(0.1, 0.2)

            transactions.append(
                (
                    i,
                    amount,
                    transaction_hour,
                    merchant_category,
                    location_risk_score,
                    distance_from_home,
                    frequency_last_hour,
                    avg_amount_last_30_days,
                    is_online,
                    device_risk_score,
                    0,  # 0 = 正常
                )
            )

        return transactions

    # 1.2 生成異常交易數據
    print("\n1.2 生成異常交易數據:")

    def generate_anomalous_transactions():
        anomalies = []
        for i in range(1001, 1201):  # 200筆異常交易
            anomaly_type = random.choice(["amount", "time", "location", "frequency"])

            if anomaly_type == "amount":  # 金額異常
                amount = random.uniform(1000, 5000)  # 異常高金額
                transaction_hour = random.randint(8, 22)
                location_risk_score = random.uniform(0.1, 0.3)
                frequency_last_hour = random.randint(0, 3)
                distance_from_home = random.uniform(0, 50)

            elif anomaly_type == "time":  # 時間異常
                amount = random.uniform(50, 300)
                transaction_hour = random.choice([2, 3, 4, 5])  # 深夜交易
                location_risk_score = random.uniform(0.1, 0.3)
                frequency_last_hour = random.randint(0, 3)
                distance_from_home = random.uniform(0, 50)

            elif anomaly_type == "location":  # 位置異常
                amount = random.uniform(50, 500)
                transaction_hour = random.randint(8, 22)
                location_risk_score = random.uniform(0.7, 0.9)  # 高風險位置
                frequency_last_hour = random.randint(0, 3)
                distance_from_home = random.uniform(200, 1000)  # 異常距離

            else:  # 頻率異常
                amount = random.uniform(50, 300)
                transaction_hour = random.randint(8, 22)
                location_risk_score = random.uniform(0.1, 0.3)
                frequency_last_hour = random.randint(8, 15)  # 異常高頻
                distance_from_home = random.uniform(0, 50)

            merchant_category = random.choice([1, 2, 3, 4, 5])
            avg_amount_last_30_days = random.uniform(50, 300)
            is_online = random.choice([True, False])
            device_risk_score = random.uniform(0.1, 0.8)

            anomalies.append(
                (
                    i,
                    amount,
                    transaction_hour,
                    merchant_category,
                    location_risk_score,
                    distance_from_home,
                    frequency_last_hour,
                    avg_amount_last_30_days,
                    is_online,
                    device_risk_score,
                    1,  # 1 = 異常
                )
            )

        return anomalies

    # 合併正常和異常數據
    normal_transactions = generate_normal_transactions()
    anomalous_transactions = generate_anomalous_transactions()
    all_transactions = normal_transactions + anomalous_transactions

    transaction_columns = [
        "transaction_id",
        "amount",
        "transaction_hour",
        "merchant_category",
        "location_risk_score",
        "distance_from_home",
        "frequency_last_hour",
        "avg_amount_last_30_days",
        "is_online",
        "device_risk_score",
        "is_fraud",
    ]

    transactions_df = spark.createDataFrame(all_transactions, transaction_columns)

    print(f"總交易數: {transactions_df.count()}")
    print(f"正常交易: {transactions_df.filter(col('is_fraud') == 0).count()}")
    print(f"異常交易: {transactions_df.filter(col('is_fraud') == 1).count()}")

    print("交易數據示例:")
    transactions_df.show(10)

    # 2. 統計方法異常檢測
    print("\n2. 統計方法異常檢測:")

    # 2.1 Z-Score 方法
    print("\n2.1 Z-Score 異常檢測:")

    # 計算金額的 Z-Score
    amount_stats = transactions_df.agg(
        avg("amount").alias("mean_amount"), stddev("amount").alias("std_amount")
    ).collect()[0]

    mean_amount = amount_stats["mean_amount"]
    std_amount = amount_stats["std_amount"]

    z_score_df = transactions_df.withColumn(
        "z_score", abs((col("amount") - mean_amount) / std_amount)
    ).withColumn(
        "is_outlier_zscore", when(col("z_score") > 3, 1).otherwise(0)  # 3-sigma 規則
    )

    print("Z-Score 異常檢測結果:")
    z_score_results = z_score_df.groupBy("is_outlier_zscore").count()
    z_score_results.show()

    # Z-Score 準確性評估
    z_score_accuracy = z_score_df.select(
        avg(when(col("is_fraud") == col("is_outlier_zscore"), 1).otherwise(0)).alias(
            "accuracy"
        )
    ).collect()[0]["accuracy"]

    print(f"Z-Score 方法準確率: {z_score_accuracy:.4f}")

    # 2.2 IQR 方法
    print("\n2.2 IQR (四分位距) 異常檢測:")

    # 計算 IQR
    quartiles = transactions_df.select(
        percentile_approx("amount", 0.25).alias("q1"),
        percentile_approx("amount", 0.75).alias("q3"),
    ).collect()[0]

    q1, q3 = quartiles["q1"], quartiles["q3"]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    iqr_df = transactions_df.withColumn(
        "is_outlier_iqr",
        when(
            (col("amount") < lower_bound) | (col("amount") > upper_bound), 1
        ).otherwise(0),
    )

    print(f"IQR 異常邊界: [{lower_bound:.2f}, {upper_bound:.2f}]")
    print("IQR 異常檢測結果:")
    iqr_results = iqr_df.groupBy("is_outlier_iqr").count()
    iqr_results.show()

    # IQR 準確性評估
    iqr_accuracy = iqr_df.select(
        avg(when(col("is_fraud") == col("is_outlier_iqr"), 1).otherwise(0)).alias(
            "accuracy"
        )
    ).collect()[0]["accuracy"]

    print(f"IQR 方法準確率: {iqr_accuracy:.4f}")

    # 3. 基於距離的異常檢測
    print("\n3. 基於距離的異常檢測:")

    # 3.1 數據預處理
    print("\n3.1 數據預處理:")

    # 準備特征
    feature_columns = [
        "amount",
        "transaction_hour",
        "merchant_category",
        "location_risk_score",
        "distance_from_home",
        "frequency_last_hour",
        "avg_amount_last_30_days",
        "device_risk_score",
    ]

    # 處理布爾類型特征
    processed_df = transactions_df.withColumn(
        "is_online_numeric", when(col("is_online"), 1.0).otherwise(0.0)
    )

    feature_columns_final = feature_columns + ["is_online_numeric"]

    # 創建特征向量
    assembler = VectorAssembler(inputCols=feature_columns_final, outputCol="features")
    feature_df = assembler.transform(processed_df)

    # 標準化特征
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(feature_df)
    scaled_df = scaler_model.transform(feature_df)

    print("特征預處理完成")

    # 3.2 基於聚類的異常檢測
    print("\n3.2 基於聚類的異常檢測:")

    # 使用 K-Means 進行聚類
    kmeans = KMeans(featuresCol="scaled_features", k=5, seed=42)
    kmeans_model = kmeans.fit(scaled_df)
    clustered_df = kmeans_model.transform(scaled_df)

    # 計算每個點到其聚類中心的距離
    def calculate_distance_to_center(features, center_idx, centers):
        center = centers[center_idx]
        distance = float(np.sqrt(np.sum((np.array(features) - center) ** 2)))
        return distance

    # 獲取聚類中心
    centers = kmeans_model.clusterCenters()

    # 廣播聚類中心
    broadcast_centers = spark.sparkContext.broadcast(centers)

    # 定義距離計算UDF
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType

    def distance_udf(features, prediction):
        centers = broadcast_centers.value
        center = centers[prediction]
        features_array = features.toArray()
        distance = float(np.sqrt(np.sum((features_array - center) ** 2)))
        return distance

    distance_to_center_udf = udf(distance_udf, DoubleType())

    distance_df = clustered_df.withColumn(
        "distance_to_center",
        distance_to_center_udf(col("scaled_features"), col("prediction")),
    )

    # 基於距離確定異常
    distance_threshold = distance_df.agg(
        percentile_approx("distance_to_center", 0.95)
    ).collect()[0][0]

    cluster_anomaly_df = distance_df.withColumn(
        "is_outlier_cluster",
        when(col("distance_to_center") > distance_threshold, 1).otherwise(0),
    )

    print(f"聚類異常檢測閾值: {distance_threshold:.4f}")
    print("聚類異常檢測結果:")
    cluster_results = cluster_anomaly_df.groupBy("is_outlier_cluster").count()
    cluster_results.show()

    # 聚類方法準確性評估
    cluster_accuracy = cluster_anomaly_df.select(
        avg(when(col("is_fraud") == col("is_outlier_cluster"), 1).otherwise(0)).alias(
            "accuracy"
        )
    ).collect()[0]["accuracy"]

    print(f"聚類方法準確率: {cluster_accuracy:.4f}")

    # 4. 機器學習異常檢測
    print("\n4. 機器學習異常檢測:")

    # 4.1 監督學習方法
    print("\n4.1 監督學習異常檢測:")

    # 分割訓練和測試數據
    train_df, test_df = scaled_df.randomSplit([0.7, 0.3], seed=42)

    print(f"訓練數據: {train_df.count()}")
    print(f"測試數據: {test_df.count()}")

    # 邏輯回歸分類器
    lr = LogisticRegression(featuresCol="scaled_features", labelCol="is_fraud")
    lr_model = lr.fit(train_df)

    # 預測
    lr_predictions = lr_model.transform(test_df)

    # 評估
    binary_evaluator = BinaryClassificationEvaluator(
        labelCol="is_fraud", metricName="areaUnderROC"
    )
    auc = binary_evaluator.evaluate(lr_predictions)

    multiclass_evaluator = MulticlassClassificationEvaluator(
        labelCol="is_fraud", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = multiclass_evaluator.evaluate(lr_predictions)

    print(f"邏輯回歸 AUC: {auc:.4f}")
    print(f"邏輯回歸準確率: {accuracy:.4f}")

    # 顯示混淆矩陣
    confusion_matrix = lr_predictions.groupBy("is_fraud", "prediction").count()
    print("混淆矩陣:")
    confusion_matrix.show()

    # 5. 集成異常檢測方法
    print("\n5. 集成異常檢測方法:")

    # 5.1 多方法投票
    print("\n5.1 多方法集成:")

    # 合併所有方法的結果
    ensemble_df = (
        cluster_anomaly_df.join(
            z_score_df.select("transaction_id", "is_outlier_zscore"), "transaction_id"
        )
        .join(iqr_df.select("transaction_id", "is_outlier_iqr"), "transaction_id")
        .join(
            lr_predictions.select("transaction_id", "prediction").withColumnRenamed(
                "prediction", "lr_prediction"
            ),
            "transaction_id",
        )
    )

    # 計算集成分數
    ensemble_score_df = ensemble_df.withColumn(
        "ensemble_score",
        col("is_outlier_cluster")
        + col("is_outlier_zscore")
        + col("is_outlier_iqr")
        + col("lr_prediction"),
    ).withColumn(
        "is_outlier_ensemble",
        when(col("ensemble_score") >= 2, 1).otherwise(0),  # 多數投票
    )

    print("集成方法結果:")
    ensemble_results = ensemble_score_df.groupBy("is_outlier_ensemble").count()
    ensemble_results.show()

    # 集成方法準確性評估
    ensemble_accuracy = ensemble_score_df.select(
        avg(when(col("is_fraud") == col("is_outlier_ensemble"), 1).otherwise(0)).alias(
            "accuracy"
        )
    ).collect()[0]["accuracy"]

    print(f"集成方法準確率: {ensemble_accuracy:.4f}")

    # 6. 實時異常檢測
    print("\n6. 實時異常檢測系統:")

    # 6.1 異常分數計算
    print("\n6.1 異常分數計算:")

    # 定義異常分數計算函數
    def calculate_anomaly_score(row):
        score = 0

        # 金額異常分數
        if row["amount"] > 1000:
            score += 0.3
        elif row["amount"] > 500:
            score += 0.1

        # 時間異常分數
        if row["transaction_hour"] < 6 or row["transaction_hour"] > 23:
            score += 0.2

        # 位置風險分數
        score += row["location_risk_score"] * 0.2

        # 頻率異常分數
        if row["frequency_last_hour"] > 5:
            score += 0.2

        # 距離異常分數
        if row["distance_from_home"] > 100:
            score += 0.1

        return min(score, 1.0)  # 限制在0-1之間

    # 應用異常分數計算
    from pyspark.sql.functions import udf

    anomaly_score_udf = udf(lambda row: calculate_anomaly_score(row), DoubleType())

    realtime_df = transactions_df.withColumn(
        "anomaly_score",
        when(col("amount") > 1000, 0.3).otherwise(0)
        + when(
            (col("transaction_hour") < 6) | (col("transaction_hour") > 23), 0.2
        ).otherwise(0)
        + col("location_risk_score") * 0.2
        + when(col("frequency_last_hour") > 5, 0.2).otherwise(0)
        + when(col("distance_from_home") > 100, 0.1).otherwise(0),
    ).withColumn(
        "risk_level",
        when(col("anomaly_score") > 0.7, "HIGH")
        .when(col("anomaly_score") > 0.4, "MEDIUM")
        .otherwise("LOW"),
    )

    print("實時異常分數分布:")
    realtime_df.groupBy("risk_level").count().orderBy("risk_level").show()

    # 6.2 實時監控規則
    print("\n6.2 實時監控規則:")

    # 定義多種監控規則
    monitoring_rules = realtime_df.withColumn(
        "rule_triggers",
        when(col("amount") > 2000, array(lit("large_amount"))).otherwise(array())
        + when(
            (col("transaction_hour") < 6) | (col("transaction_hour") > 23),
            array(lit("unusual_time")),
        ).otherwise(array())
        + when(
            col("location_risk_score") > 0.7, array(lit("high_risk_location"))
        ).otherwise(array())
        + when(col("frequency_last_hour") > 8, array(lit("high_frequency"))).otherwise(
            array()
        )
        + when(
            col("distance_from_home") > 500, array(lit("unusual_location"))
        ).otherwise(array()),
    ).withColumn(
        "alert_level",
        when(size(col("rule_triggers")) >= 3, "CRITICAL")
        .when(size(col("rule_triggers")) >= 2, "HIGH")
        .when(size(col("rule_triggers")) >= 1, "MEDIUM")
        .otherwise("LOW"),
    )

    print("監控規則觸發統計:")
    monitoring_rules.groupBy("alert_level").count().orderBy("alert_level").show()

    # 7. 性能評估和比較
    print("\n7. 異常檢測方法性能比較:")

    # 7.1 方法性能總結
    method_performance = {
        "Z-Score": z_score_accuracy,
        "IQR": iqr_accuracy,
        "聚類距離": cluster_accuracy,
        "邏輯回歸": accuracy,
        "集成方法": ensemble_accuracy,
    }

    print("各方法準確率比較:")
    for method, acc in sorted(
        method_performance.items(), key=lambda x: x[1], reverse=True
    ):
        print(f"- {method}: {acc:.4f}")

    # 7.2 精確率和召回率分析
    print("\n7.2 精確率和召回率分析:")

    # 計算最佳方法的詳細指標
    best_method_df = ensemble_score_df

    # 真正例 (TP)、假正例 (FP)、真負例 (TN)、假負例 (FN)
    tp = best_method_df.filter(
        (col("is_fraud") == 1) & (col("is_outlier_ensemble") == 1)
    ).count()
    fp = best_method_df.filter(
        (col("is_fraud") == 0) & (col("is_outlier_ensemble") == 1)
    ).count()
    tn = best_method_df.filter(
        (col("is_fraud") == 0) & (col("is_outlier_ensemble") == 0)
    ).count()
    fn = best_method_df.filter(
        (col("is_fraud") == 1) & (col("is_outlier_ensemble") == 0)
    ).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1_score = (
        2 * (precision * recall) / (precision + recall)
        if (precision + recall) > 0
        else 0
    )

    print(f"精確率 (Precision): {precision:.4f}")
    print(f"召回率 (Recall): {recall:.4f}")
    print(f"F1 分數: {f1_score:.4f}")
    print(f"真正例: {tp}, 假正例: {fp}, 真負例: {tn}, 假負例: {fn}")

    # 8. 異常類型分析
    print("\n8. 異常類型分析:")

    # 8.1 異常模式識別
    print("\n8.1 異常模式分析:")

    fraud_analysis = transactions_df.filter(col("is_fraud") == 1).withColumn(
        "anomaly_type",
        when(col("amount") > 1000, "Large Amount")
        .when(
            (col("transaction_hour") < 6) | (col("transaction_hour") > 23),
            "Unusual Time",
        )
        .when(col("location_risk_score") > 0.6, "High Risk Location")
        .when(col("frequency_last_hour") > 6, "High Frequency")
        .otherwise("Other"),
    )

    print("異常類型分布:")
    fraud_analysis.groupBy("anomaly_type").count().orderBy(desc("count")).show()

    # 8.2 異常嚴重程度分析
    severity_analysis = realtime_df.filter(col("anomaly_score") > 0.3).withColumn(
        "severity_category",
        when(col("anomaly_score") > 0.8, "Critical")
        .when(col("anomaly_score") > 0.6, "High")
        .when(col("anomaly_score") > 0.4, "Medium")
        .otherwise("Low"),
    )

    print("異常嚴重程度分布:")
    severity_analysis.groupBy("severity_category").agg(
        count("*").alias("count"), avg("amount").alias("avg_amount")
    ).orderBy("severity_category").show()

    # 9. 異常檢測系統部署
    print("\n9. 異常檢測系統部署:")

    # 9.1 模型持久化
    print("\n9.1 模型持久化:")

    # 保存模型
    model_path = "/tmp/anomaly_detection_models"
    try:
        # 保存各個模型組件
        scaler_model.write().overwrite().save(f"{model_path}/scaler")
        kmeans_model.write().overwrite().save(f"{model_path}/kmeans")
        lr_model.write().overwrite().save(f"{model_path}/logistic_regression")
        print("異常檢測模型已保存")
    except Exception as e:
        print(f"模型保存失敗: {e}")

    # 9.2 實時預測流程
    print("\n9.2 實時預測流程示例:")

    # 模擬新交易
    new_transactions = [
        (2001, 3000, 3, 1, 0.8, 300, 12, 150, True, 0.7),  # 高風險交易
        (2002, 85, 14, 2, 0.2, 15, 1, 200, False, 0.1),  # 正常交易
    ]

    new_trans_df = spark.createDataFrame(
        new_transactions,
        [
            "transaction_id",
            "amount",
            "transaction_hour",
            "merchant_category",
            "location_risk_score",
            "distance_from_home",
            "frequency_last_hour",
            "avg_amount_last_30_days",
            "is_online",
            "device_risk_score",
        ],
    )

    # 應用相同的預處理
    new_processed = new_trans_df.withColumn(
        "is_online_numeric", when(col("is_online"), 1.0).otherwise(0.0)
    )

    new_features = assembler.transform(new_processed)
    new_scaled = scaler_model.transform(new_features)

    # 預測異常
    new_clusters = kmeans_model.transform(new_scaled)
    new_distances = new_clusters.withColumn(
        "distance_to_center",
        distance_to_center_udf(col("scaled_features"), col("prediction")),
    )

    new_predictions = lr_model.transform(new_scaled)

    # 綜合判斷
    final_predictions = new_distances.join(
        new_predictions.select("transaction_id", "probability", "prediction"),
        "transaction_id",
    ).withColumn(
        "is_anomaly",
        when(
            (col("distance_to_center") > distance_threshold) | (col("prediction") == 1),
            True,
        ).otherwise(False),
    )

    print("新交易異常檢測結果:")
    final_predictions.select(
        "transaction_id",
        "amount",
        "transaction_hour",
        "distance_to_center",
        "prediction",
        "is_anomaly",
    ).show()

    # 10. 系統監控和維護
    print("\n10. 異常檢測系統總結:")

    # 10.1 系統性能總結
    system_metrics = {
        "總交易數": transactions_df.count(),
        "異常交易數": transactions_df.filter(col("is_fraud") == 1).count(),
        "最佳方法": "集成方法",
        "最佳準確率": f"{max(method_performance.values()):.4f}",
        "檢測精確率": f"{precision:.4f}",
        "檢測召回率": f"{recall:.4f}",
        "F1分數": f"{f1_score:.4f}",
    }

    print("系統性能指標:")
    for metric, value in system_metrics.items():
        print(f"- {metric}: {value}")

    # 10.2 系統改進建議
    improvement_suggestions = [
        "增加更多特征工程以提高檢測準確性",
        "實施在線學習以適應新的異常模式",
        "建立反饋機制以持續改進模型",
        "增加無監督異常檢測方法",
        "實施分層異常檢測策略",
        "建立異常檢測結果的可解釋性",
        "設計自適應閾值調整機制",
    ]

    print("\n系統改進建議:")
    for i, suggestion in enumerate(improvement_suggestions, 1):
        print(f"{i}. {suggestion}")

    # 10.3 運營建議
    operational_recommendations = [
        "建立24/7實時監控系統",
        "設置多級別告警機制",
        "定期重新訓練和更新模型",
        "建立人工審核流程",
        "實施模型性能監控",
        "建立異常案例分析流程",
    ]

    print("\n運營建議:")
    for i, recommendation in enumerate(operational_recommendations, 1):
        print(f"{i}. {recommendation}")

    # 清理資源
    spark.stop()
    print("\n異常檢測練習完成！")


if __name__ == "__main__":
    main()
