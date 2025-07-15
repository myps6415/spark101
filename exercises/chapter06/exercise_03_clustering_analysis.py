#!/usr/bin/env python3
"""
第6章練習3：聚類分析
使用 Spark MLlib 進行客戶分群和聚類分析
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum, max as spark_max, \
    min as spark_min, stddev, collect_list, size, explode, split, \
    rand, round as spark_round, desc, asc, row_number, dense_rank, \
    sqrt, pow as spark_pow, abs as spark_abs
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture
from pyspark.ml.feature import VectorAssembler, StandardScaler, PCA, StringIndexer
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.stat import Correlation

import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
import random

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("聚類分析練習") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("=== 第6章練習3：聚類分析 ===")
    
    # 1. 創建客戶行為數據
    print("\n1. 創建客戶行為數據:")
    
    # 1.1 創建客戶基本信息
    print("\n1.1 創建客戶基本信息:")
    
    def generate_customer_data():
        customers = []
        for i in range(1, 501):  # 500個客戶
            age = random.randint(18, 70)
            income = random.randint(25000, 150000)
            
            # 基於年齡調整收入
            if age < 25:
                income = random.randint(25000, 50000)
            elif age < 35:
                income = random.randint(35000, 80000)
            elif age < 50:
                income = random.randint(50000, 120000)
            else:
                income = random.randint(40000, 100000)
            
            education = random.choice(["High School", "Bachelor", "Master", "PhD"])
            occupation = random.choice(["Student", "Engineer", "Manager", "Teacher", "Doctor", "Artist", "Other"])
            marital_status = random.choice(["Single", "Married", "Divorced"])
            
            customers.append((i, age, income, education, occupation, marital_status))
        
        return customers
    
    customer_data = generate_customer_data()
    customer_columns = ["customer_id", "age", "income", "education", "occupation", "marital_status"]
    customers_df = spark.createDataFrame(customer_data, customer_columns)
    
    # 1.2 創建購買行為數據
    print("\n1.2 創建購買行為數據:")
    
    def generate_purchase_behavior():
        behaviors = []
        for customer_id in range(1, 501):
            # 基於客戶特征生成行為模式
            customer = next(c for c in customer_data if c[0] == customer_id)
            age, income = customer[1], customer[2]
            
            # 基於年齡和收入生成行為特征
            if income > 80000:  # 高收入群體
                monthly_purchases = random.randint(8, 20)
                avg_order_value = random.randint(200, 800)
                premium_purchases = random.randint(2, 8)
            elif income > 50000:  # 中等收入群體
                monthly_purchases = random.randint(4, 12)
                avg_order_value = random.randint(80, 300)
                premium_purchases = random.randint(0, 3)
            else:  # 低收入群體
                monthly_purchases = random.randint(2, 8)
                avg_order_value = random.randint(30, 150)
                premium_purchases = random.randint(0, 1)
            
            # 添加一些隨機變化
            monthly_purchases += random.randint(-2, 3)
            avg_order_value += random.randint(-50, 100)
            
            total_spent = monthly_purchases * avg_order_value * random.uniform(0.8, 1.2)
            days_since_last_purchase = random.randint(1, 90)
            
            # 購買類別偏好
            electronics_ratio = random.uniform(0, 0.5)
            clothing_ratio = random.uniform(0, 0.4)
            books_ratio = random.uniform(0, 0.3)
            home_ratio = 1 - electronics_ratio - clothing_ratio - books_ratio
            
            # 購買渠道偏好
            online_ratio = random.uniform(0.3, 0.9)
            
            behaviors.append((
                customer_id,
                max(0, monthly_purchases),
                max(10, avg_order_value),
                max(0, total_spent),
                days_since_last_purchase,
                max(0, premium_purchases),
                round(electronics_ratio, 3),
                round(clothing_ratio, 3),
                round(books_ratio, 3),
                round(home_ratio, 3),
                round(online_ratio, 3)
            ))
        
        return behaviors
    
    behavior_data = generate_purchase_behavior()
    behavior_columns = [
        "customer_id", "monthly_purchases", "avg_order_value", "total_spent",
        "days_since_last_purchase", "premium_purchases", "electronics_ratio",
        "clothing_ratio", "books_ratio", "home_ratio", "online_ratio"
    ]
    behavior_df = spark.createDataFrame(behavior_data, behavior_columns)
    
    # 合併數據
    customer_full_df = customers_df.join(behavior_df, "customer_id")
    
    print(f"客戶數據總數: {customer_full_df.count()}")
    print("客戶數據示例:")
    customer_full_df.show(5, truncate=False)
    
    # 2. 數據探索和預處理
    print("\n2. 數據探索和預處理:")
    
    # 2.1 數據統計分析
    print("\n2.1 數據統計分析:")
    
    # 基本統計信息
    numeric_columns = ["age", "income", "monthly_purchases", "avg_order_value", "total_spent"]
    
    stats_df = customer_full_df.select(*numeric_columns).describe()
    print("數值型特征統計:")
    stats_df.show()
    
    # 相關性分析
    print("\n2.2 特征相關性分析:")
    
    # 準備數值特征
    feature_columns = [
        "age", "income", "monthly_purchases", "avg_order_value", 
        "total_spent", "days_since_last_purchase", "premium_purchases"
    ]
    
    # 創建特征向量
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    feature_df = assembler.transform(customer_full_df)
    
    # 計算相關性矩陣
    correlation_matrix = Correlation.corr(feature_df, "features").head()[0]
    print("相關性矩陣:")
    print(correlation_matrix.toArray())
    
    # 3. K-Means 聚類
    print("\n3. K-Means 聚類分析:")
    
    # 3.1 特征標準化
    print("\n3.1 特征標準化:")
    
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaler_model = scaler.fit(feature_df)
    scaled_df = scaler_model.transform(feature_df)
    
    print("特征標準化完成")
    
    # 3.2 確定最優聚類數
    print("\n3.2 確定最優聚類數 (肘部法則):")
    
    # 計算不同K值的成本
    costs = []
    K_values = range(2, 11)
    
    for k in K_values:
        kmeans = KMeans(featuresCol="scaled_features", k=k, seed=42)
        model = kmeans.fit(scaled_df)
        cost = model.summary.trainingCost
        costs.append(cost)
        print(f"K={k}, Cost={cost:.2f}")
    
    # 找到肘部點（簡化版本）
    best_k = 5  # 基於成本下降趨勢選擇
    print(f"選擇 K={best_k}")
    
    # 3.3 K-Means 模型訓練
    print("\n3.3 K-Means 模型訓練:")
    
    kmeans = KMeans(featuresCol="scaled_features", k=best_k, seed=42)
    kmeans_model = kmeans.fit(scaled_df)
    
    # 預測聚類
    clustered_df = kmeans_model.transform(scaled_df)
    
    print("聚類結果:")
    clustered_df.groupBy("prediction").count().orderBy("prediction").show()
    
    # 3.4 聚類評估
    print("\n3.4 聚類評估:")
    
    # Silhouette 分數
    evaluator = ClusteringEvaluator(featuresCol="scaled_features")
    silhouette = evaluator.evaluate(clustered_df)
    print(f"Silhouette 分數: {silhouette:.4f}")
    
    # Within Set Sum of Squared Errors
    wssse = kmeans_model.summary.trainingCost
    print(f"WSSSE: {wssse:.2f}")
    
    # 4. 聚類分析和解釋
    print("\n4. 聚類分析和解釋:")
    
    # 4.1 聚類中心分析
    print("\n4.1 聚類中心分析:")
    
    centers = kmeans_model.clusterCenters()
    print("聚類中心:")
    for i, center in enumerate(centers):
        print(f"簇 {i}: {center}")
    
    # 4.2 各聚類特征分析
    print("\n4.2 各聚類特征分析:")
    
    cluster_analysis = clustered_df.groupBy("prediction").agg(
        count("*").alias("cluster_size"),
        avg("age").alias("avg_age"),
        avg("income").alias("avg_income"),
        avg("monthly_purchases").alias("avg_monthly_purchases"),
        avg("avg_order_value").alias("avg_order_value"),
        avg("total_spent").alias("avg_total_spent"),
        avg("premium_purchases").alias("avg_premium_purchases")
    ).orderBy("prediction")
    
    print("聚類特征分析:")
    cluster_analysis.show()
    
    # 4.3 聚類標籤和命名
    print("\n4.3 聚類標籤和解釋:")
    
    # 基於特征給聚類命名
    cluster_labels = clustered_df.withColumn(
        "cluster_label",
        when(col("prediction") == 0, "Budget Conscious")
        .when(col("prediction") == 1, "Premium Shoppers") 
        .when(col("prediction") == 2, "Regular Customers")
        .when(col("prediction") == 3, "High Value Customers")
        .otherwise("Occasional Buyers")
    )
    
    # 按標籤統計
    label_stats = cluster_labels.groupBy("cluster_label").agg(
        count("*").alias("customer_count"),
        avg("income").alias("avg_income"),
        avg("total_spent").alias("avg_spending")
    ).orderBy(desc("avg_spending"))
    
    print("聚類標籤統計:")
    label_stats.show()
    
    # 5. 高斯混合模型聚類
    print("\n5. 高斯混合模型聚類:")
    
    # 5.1 GMM 模型訓練
    print("\n5.1 GMM 模型訓練:")
    
    gmm = GaussianMixture(featuresCol="scaled_features", k=best_k, seed=42)
    gmm_model = gmm.fit(scaled_df)
    
    # GMM 預測
    gmm_clustered_df = gmm_model.transform(scaled_df)
    
    print("GMM 聚類結果:")
    gmm_clustered_df.groupBy("prediction").count().orderBy("prediction").show()
    
    # 5.2 GMM 評估
    print("\n5.2 GMM 評估:")
    
    gmm_silhouette = evaluator.evaluate(gmm_clustered_df)
    print(f"GMM Silhouette 分數: {gmm_silhouette:.4f}")
    
    # 對數似然
    log_likelihood = gmm_model.summary.logLikelihood
    print(f"對數似然: {log_likelihood:.2f}")
    
    # 6. 二分K-Means聚類
    print("\n6. 二分K-Means聚類:")
    
    # 6.1 二分K-Means模型
    print("\n6.1 二分K-Means模型訓練:")
    
    bisecting_kmeans = BisectingKMeans(featuresCol="scaled_features", k=best_k, seed=42)
    bisecting_model = bisecting_kmeans.fit(scaled_df)
    
    bisecting_clustered_df = bisecting_model.transform(scaled_df)
    
    print("二分K-Means聚類結果:")
    bisecting_clustered_df.groupBy("prediction").count().orderBy("prediction").show()
    
    # 6.2 比較不同聚類算法
    print("\n6.2 聚類算法比較:")
    
    bisecting_silhouette = evaluator.evaluate(bisecting_clustered_df)
    
    algorithm_comparison = {
        "K-Means": silhouette,
        "GMM": gmm_silhouette,
        "Bisecting K-Means": bisecting_silhouette
    }
    
    print("算法性能比較:")
    for algo, score in algorithm_comparison.items():
        print(f"{algo}: {score:.4f}")
    
    # 7. 業務洞察和應用
    print("\n7. 業務洞察和應用:")
    
    # 7.1 客戶價值分析
    print("\n7.1 客戶價值分析:")
    
    # 使用最佳聚類結果進行分析
    best_clustered_df = cluster_labels
    
    # 計算客戶生命週期價值
    clv_analysis = best_clustered_df.withColumn(
        "estimated_clv",
        col("monthly_purchases") * col("avg_order_value") * 12 * 2  # 假設2年生命週期
    ).groupBy("cluster_label").agg(
        count("*").alias("customer_count"),
        avg("estimated_clv").alias("avg_clv"),
        spark_sum("estimated_clv").alias("total_clv"),
        avg("days_since_last_purchase").alias("avg_recency")
    ).withColumn(
        "clv_percentage", 
        col("total_clv") / spark_sum("total_clv").over(Window.partitionBy()) * 100
    )
    
    print("客戶生命週期價值分析:")
    clv_analysis.orderBy(desc("avg_clv")).show()
    
    # 7.2 營銷策略建議
    print("\n7.2 營銷策略建議:")
    
    # 基於聚類的營銷策略
    marketing_strategies = best_clustered_df.withColumn(
        "marketing_strategy",
        when(col("cluster_label") == "Premium Shoppers", "VIP服務和專屬優惠")
        .when(col("cluster_label") == "High Value Customers", "個性化推薦和忠誠度計劃")
        .when(col("cluster_label") == "Regular Customers", "交叉銷售和促銷活動")
        .when(col("cluster_label") == "Budget Conscious", "價格敏感促銷和基礎產品")
        .otherwise("重新激活和優惠券")
    ).withColumn(
        "communication_channel",
        when(col("online_ratio") > 0.7, "Digital Marketing")
        .when(col("online_ratio") > 0.5, "Omnichannel")
        .otherwise("Traditional Marketing")
    )
    
    strategy_summary = marketing_strategies.groupBy("cluster_label", "marketing_strategy").count()
    print("營銷策略分配:")
    strategy_summary.show(truncate=False)
    
    # 8. 聚類穩定性分析
    print("\n8. 聚類穩定性分析:")
    
    # 8.1 多次聚類結果比較
    print("\n8.1 聚類穩定性測試:")
    
    stability_scores = []
    for seed in [42, 123, 456, 789, 999]:
        kmeans_stability = KMeans(featuresCol="scaled_features", k=best_k, seed=seed)
        model_stability = kmeans_stability.fit(scaled_df)
        clustered_stability = model_stability.transform(scaled_df)
        
        silhouette_stability = evaluator.evaluate(clustered_stability)
        stability_scores.append(silhouette_stability)
    
    avg_stability = sum(stability_scores) / len(stability_scores)
    std_stability = np.std(stability_scores)
    
    print(f"穩定性測試結果:")
    print(f"平均 Silhouette 分數: {avg_stability:.4f}")
    print(f"標準差: {std_stability:.4f}")
    print(f"穩定性評級: {'高' if std_stability < 0.05 else '中' if std_stability < 0.1 else '低'}")
    
    # 9. 降維可視化準備
    print("\n9. 降維可視化準備:")
    
    # 9.1 PCA 降維
    print("\n9.1 PCA 降維:")
    
    # PCA 降維到2D用於可視化
    pca = PCA(k=2, inputCol="scaled_features", outputCol="pca_features")
    pca_model = pca.fit(scaled_df)
    pca_df = pca_model.transform(scaled_df)
    
    # 獲取PCA結果
    pca_result = pca_df.select("customer_id", "pca_features", "prediction").collect()
    
    print(f"PCA 解釋方差比例: {pca_model.explainedVariance}")
    print("PCA 降維完成，準備可視化數據")
    
    # 10. 聚類結果應用
    print("\n10. 聚類結果應用:")
    
    # 10.1 個性化推薦
    print("\n10.1 基於聚類的個性化推薦:")
    
    # 為每個聚類生成推薦策略
    recommendation_rules = best_clustered_df.withColumn(
        "product_recommendation",
        when(col("cluster_label") == "Premium Shoppers", 
             array(lit("高端電子產品"), lit("奢侈品"), lit("專業設備")))
        .when(col("cluster_label") == "High Value Customers",
             array(lit("智能家居"), lit("高品質服裝"), lit("專業書籍")))
        .when(col("cluster_label") == "Regular Customers",
             array(lit("日用品"), lit("休閒服裝"), lit("娛樂產品")))
        .when(col("cluster_label") == "Budget Conscious",
             array(lit("基礎電子產品"), lit("平價服裝"), lit("實用家居")))
        .otherwise(array(lit("促銷商品"), lit("新手優惠")))
    ).withColumn(
        "promotion_type",
        when(col("cluster_label") == "Premium Shoppers", "專屬VIP折扣")
        .when(col("cluster_label") == "High Value Customers", "忠誠度積分翻倍")
        .when(col("cluster_label") == "Regular Customers", "買二送一優惠")
        .when(col("cluster_label") == "Budget Conscious", "大幅度折扣")
        .otherwise("新用戶專享優惠")
    )
    
    print("個性化推薦示例:")
    recommendation_rules.select("customer_id", "cluster_label", "product_recommendation", "promotion_type") \
        .limit(10).show(truncate=False)
    
    # 10.2 風險評估
    print("\n10.2 客戶流失風險評估:")
    
    churn_risk = best_clustered_df.withColumn(
        "churn_risk_score",
        when(col("days_since_last_purchase") > 60, 0.8)
        .when(col("days_since_last_purchase") > 30, 0.5)
        .when(col("monthly_purchases") < 2, 0.6)
        .otherwise(0.2)
    ).withColumn(
        "churn_risk_level",
        when(col("churn_risk_score") > 0.7, "High")
        .when(col("churn_risk_score") > 0.4, "Medium") 
        .otherwise("Low")
    )
    
    churn_summary = churn_risk.groupBy("cluster_label", "churn_risk_level").count()
    print("流失風險分析:")
    churn_summary.show()
    
    # 11. 聚類模型部署建議
    print("\n11. 聚類模型部署建議:")
    
    # 11.1 模型保存和載入
    print("\n11.1 模型持久化:")
    
    # 保存模型（示例路徑）
    model_path = "/tmp/customer_clustering_model"
    try:
        kmeans_model.write().overwrite().save(model_path)
        print(f"模型已保存到: {model_path}")
    except Exception as e:
        print(f"模型保存失敗: {e}")
    
    # 11.2 實時聚類預測
    print("\n11.2 實時聚類預測流程:")
    
    # 模擬新客戶數據
    new_customers = [
        (501, 25, 45000, 5, 120, 600, 15, 2, 0.3, 0.4, 0.1, 0.2, 0.8),
        (502, 45, 95000, 12, 250, 3000, 8, 6, 0.5, 0.2, 0.2, 0.1, 0.6)
    ]
    
    new_customer_df = spark.createDataFrame(new_customers, 
        ["customer_id"] + feature_columns + ["electronics_ratio", "clothing_ratio", "books_ratio", "home_ratio", "online_ratio"])
    
    # 應用相同的預處理流程
    new_features_df = assembler.transform(new_customer_df)
    new_scaled_df = scaler_model.transform(new_features_df)
    
    # 預測聚類
    new_predictions = kmeans_model.transform(new_scaled_df)
    
    print("新客戶聚類預測:")
    new_predictions.select("customer_id", "prediction").show()
    
    # 12. 總結和建議
    print("\n12. 聚類分析總結:")
    
    # 分析結果總結
    analysis_summary = {
        "客戶總數": customer_full_df.count(),
        "聚類數量": best_k,
        "最佳算法": "K-Means",
        "Silhouette分數": f"{silhouette:.4f}",
        "主要客戶群體": "Premium Shoppers, High Value Customers",
        "高風險客戶比例": f"{churn_risk.filter(col('churn_risk_level') == 'High').count() / customer_full_df.count() * 100:.1f}%"
    }
    
    print("聚類分析結果總結:")
    for key, value in analysis_summary.items():
        print(f"- {key}: {value}")
    
    # 業務建議
    business_recommendations = [
        "針對Premium Shoppers群體開發高端產品線",
        "為High Value Customers建立忠誠度計劃",
        "對Budget Conscious群體提供更多促銷活動",
        "建立流失預警系統監控高風險客戶",
        "實施個性化營銷提升客戶體驗",
        "定期重新評估聚類模型以適應客戶行為變化"
    ]
    
    print("\n業務建議:")
    for i, recommendation in enumerate(business_recommendations, 1):
        print(f"{i}. {recommendation}")
    
    # 技術建議
    technical_recommendations = [
        "實施實時聚類預測管道",
        "建立A/B測試框架驗證聚類效果",
        "整合更多數據源豐富客戶畫像",
        "考慮使用深度學習方法改進聚類",
        "建立聚類模型監控和自動更新機制",
        "開發聚類結果可視化儀表板"
    ]
    
    print("\n技術建議:")
    for i, recommendation in enumerate(technical_recommendations, 1):
        print(f"{i}. {recommendation}")
    
    # 清理資源
    spark.stop()
    print("\n聚類分析練習完成！")

if __name__ == "__main__":
    main()