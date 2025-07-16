#!/usr/bin/env python3
"""
第8章 練習4：綜合數據平台構建
=================================

在這個綜合練習中，你將構建一個完整的數據平台，
整合前面所學的所有技術：批處理、流處理、機器學習、性能調優。

學習目標：
- 設計和實現端到端的數據處理管道
- 整合批處理和流處理
- 實現實時機器學習預測
- 應用性能調優最佳實踐
- 構建監控和告警系統
"""

import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

# 添加項目根目錄到 Python 路徑
project_root = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
sys.path.insert(0, project_root)

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   RegressionEvaluator)
from pyspark.ml.feature import StandardScaler, StringIndexer, VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import SparkSession
from pyspark.sql.functions import (array_contains, avg, broadcast, coalesce,
                                   col, collect_list, count, current_timestamp,
                                   date_format, dense_rank, explode, isnan,
                                   isnull, lag, lead, lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import (ntile, rank, regexp_extract, row_number,
                                   size, split, struct)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, udf, when, window
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               MapType, StringType, StructField, StructType,
                               TimestampType)
from pyspark.sql.window import Window
from pyspark.streaming import StreamingContext


class ComprehensiveDataPlatform:
    """綜合數據平台類"""

    def __init__(self, app_name: str = "ComprehensiveDataPlatform"):
        """初始化數據平台"""
        self.spark = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )

        self.spark.sparkContext.setLogLevel("WARN")
        print(f"🚀 數據平台啟動完成 - Spark {self.spark.version}")

    def create_sample_datasets(self) -> Dict[str, Any]:
        """創建示例數據集"""
        print("📊 創建示例數據集...")

        # 1. 用戶數據
        users_data = [
            (1, "Alice", 25, "Premium", "alice@example.com", "2023-01-15", "USA"),
            (2, "Bob", 30, "Standard", "bob@example.com", "2023-02-10", "UK"),
            (
                3,
                "Charlie",
                35,
                "Premium",
                "charlie@example.com",
                "2023-01-20",
                "Canada",
            ),
            (4, "Diana", 28, "Standard", "diana@example.com", "2023-03-05", "USA"),
            (5, "Eve", 32, "VIP", "eve@example.com", "2023-01-25", "Germany"),
            (6, "Frank", 27, "Standard", "frank@example.com", "2023-02-15", "France"),
            (7, "Grace", 29, "Premium", "grace@example.com", "2023-01-30", "Japan"),
            (8, "Henry", 31, "VIP", "henry@example.com", "2023-02-20", "Australia"),
        ]

        users_df = self.spark.createDataFrame(
            users_data,
            [
                "user_id",
                "name",
                "age",
                "subscription_tier",
                "email",
                "registration_date",
                "country",
            ],
        )

        # 2. 產品數據
        products_data = [
            (101, "Laptop Pro", "Electronics", 1299.99, "2023-01-01", True),
            (102, "Wireless Mouse", "Electronics", 29.99, "2023-01-01", True),
            (103, "Programming Book", "Books", 49.99, "2023-01-15", True),
            (104, "Coffee Mug", "Home", 15.99, "2023-02-01", True),
            (105, "Mechanical Keyboard", "Electronics", 149.99, "2023-01-10", True),
            (106, "Desk Lamp", "Home", 79.99, "2023-01-20", True),
            (107, "Python Course", "Education", 199.99, "2023-02-15", True),
            (108, "Notebook", "Office", 12.99, "2023-01-05", True),
        ]

        products_df = self.spark.createDataFrame(
            products_data,
            [
                "product_id",
                "product_name",
                "category",
                "price",
                "launch_date",
                "is_active",
            ],
        )

        # 3. 交易數據
        transactions_data = []
        for i in range(1, 1001):  # 1000筆交易
            user_id = (i % 8) + 1
            product_id = (i % 8) + 101
            quantity = 1 if i % 3 == 0 else 2
            base_price = [1299.99, 29.99, 49.99, 15.99, 149.99, 79.99, 199.99, 12.99][
                product_id - 101
            ]
            amount = base_price * quantity

            # 創建時間序列
            days_ago = i % 30
            transaction_time = datetime.now() - timedelta(
                days=days_ago, hours=i % 24, minutes=i % 60
            )

            transactions_data.append(
                (
                    i,
                    user_id,
                    product_id,
                    quantity,
                    amount,
                    transaction_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "completed" if i % 10 != 0 else "failed",
                )
            )

        transactions_df = self.spark.createDataFrame(
            transactions_data,
            [
                "transaction_id",
                "user_id",
                "product_id",
                "quantity",
                "amount",
                "transaction_time",
                "status",
            ],
        )

        # 4. 用戶行為數據（流式數據模擬）
        behaviors_data = []
        for i in range(1, 5001):  # 5000條行為記錄
            user_id = (i % 8) + 1
            actions = ["view", "click", "add_to_cart", "purchase", "search", "logout"]
            action = actions[i % len(actions)]

            # 模擬會話
            session_id = f"session_{(i // 10) + 1}"
            page_url = f"/page/{(i % 20) + 1}"

            timestamp = datetime.now() - timedelta(hours=i % 168)  # 7天內的數據

            behaviors_data.append(
                (
                    i,
                    user_id,
                    session_id,
                    action,
                    page_url,
                    timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    f"device_{(i % 3) + 1}",  # device_1, device_2, device_3
                )
            )

        behaviors_df = self.spark.createDataFrame(
            behaviors_data,
            [
                "event_id",
                "user_id",
                "session_id",
                "action",
                "page_url",
                "timestamp",
                "device_type",
            ],
        )

        return {
            "users": users_df,
            "products": products_df,
            "transactions": transactions_df,
            "behaviors": behaviors_df,
        }

    def batch_processing_pipeline(self, datasets: Dict[str, Any]) -> Dict[str, Any]:
        """批處理數據管道"""
        print("🔄 執行批處理數據管道...")

        users_df = datasets["users"]
        products_df = datasets["products"]
        transactions_df = datasets["transactions"]
        behaviors_df = datasets["behaviors"]

        # 1. 數據清理和驗證
        print("  🧹 數據清理...")

        # 清理交易數據
        clean_transactions = transactions_df.filter(
            (col("amount") > 0) & (col("quantity") > 0) & (col("status").isNotNull())
        ).withColumn(
            "transaction_time",
            to_timestamp(col("transaction_time"), "yyyy-MM-dd HH:mm:ss"),
        )

        # 清理行為數據
        clean_behaviors = behaviors_df.filter(col("action").isNotNull()).withColumn(
            "timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )

        # 2. 數據聚合分析
        print("  📊 數據聚合分析...")

        # 用戶交易摘要
        user_transaction_summary = clean_transactions.groupBy("user_id").agg(
            count("transaction_id").alias("total_transactions"),
            spark_sum("amount").alias("total_spent"),
            avg("amount").alias("avg_transaction_amount"),
            spark_max("transaction_time").alias("last_transaction_date"),
            countDistinct("product_id").alias("unique_products_purchased"),
        )

        # 產品銷售分析
        product_sales_summary = (
            clean_transactions.join(products_df, "product_id")
            .groupBy("product_id", "product_name", "category")
            .agg(
                count("transaction_id").alias("total_sales"),
                spark_sum("amount").alias("total_revenue"),
                spark_sum("quantity").alias("total_quantity_sold"),
            )
        )

        # 用戶行為分析
        user_behavior_summary = clean_behaviors.groupBy("user_id").agg(
            count("event_id").alias("total_events"),
            countDistinct("session_id").alias("total_sessions"),
            countDistinct("action").alias("unique_actions"),
            spark_sum(when(col("action") == "purchase", 1).otherwise(0)).alias(
                "purchase_events"
            ),
        )

        # 3. 高級分析
        print("  🎯 高級分析...")

        # 客戶生命週期價值分析
        clv_analysis = (
            user_transaction_summary.join(users_df, "user_id")
            .withColumn(
                "clv_segment",
                when(col("total_spent") > 1000, "High Value")
                .when(col("total_spent") > 500, "Medium Value")
                .otherwise("Low Value"),
            )
            .withColumn(
                "engagement_score",
                (
                    col("total_transactions") * 0.3
                    + col("unique_products_purchased") * 0.7
                ),
            )
        )

        # 產品推薦矩陣
        product_recommendation_matrix = (
            clean_transactions.join(clean_transactions.alias("t2"), "user_id")
            .filter(col("product_id") != col("t2.product_id"))
            .groupBy("product_id", "t2.product_id")
            .agg(count("*").alias("co_purchase_count"))
            .filter(col("co_purchase_count") > 2)
        )

        return {
            "user_transaction_summary": user_transaction_summary,
            "product_sales_summary": product_sales_summary,
            "user_behavior_summary": user_behavior_summary,
            "clv_analysis": clv_analysis,
            "product_recommendation_matrix": product_recommendation_matrix,
            "clean_transactions": clean_transactions,
            "clean_behaviors": clean_behaviors,
        }

    def machine_learning_pipeline(
        self, processed_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """機器學習管道"""
        print("🤖 執行機器學習管道...")

        # 1. 特徵工程
        print("  🔧 特徵工程...")

        clv_data = processed_data["clv_analysis"]

        # 準備機器學習特徵
        ml_features = clv_data.select(
            "user_id",
            "age",
            "total_transactions",
            "total_spent",
            "avg_transaction_amount",
            "unique_products_purchased",
            "engagement_score",
            when(col("subscription_tier") == "VIP", 1)
            .when(col("subscription_tier") == "Premium", 0.7)
            .when(col("subscription_tier") == "Standard", 0.3)
            .otherwise(0)
            .alias("tier_score"),
            when(col("clv_segment") == "High Value", 1)
            .otherwise(0)
            .alias("is_high_value"),
        ).na.drop()

        # 特徵向量化
        feature_cols = [
            "age",
            "total_transactions",
            "total_spent",
            "avg_transaction_amount",
            "unique_products_purchased",
            "engagement_score",
            "tier_score",
        ]

        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

        # 標準化
        scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

        # 2. 分類模型：預測高價值客戶
        print("  📈 訓練分類模型...")

        # 準備分類數據
        classification_data = assembler.transform(ml_features)
        scaled_data = scaler.fit(classification_data).transform(classification_data)

        # 分割數據
        train_data, test_data = scaled_data.randomSplit([0.8, 0.2], seed=42)

        # 訓練隨機森林分類器
        rf_classifier = RandomForestClassifier(
            featuresCol="scaled_features",
            labelCol="is_high_value",
            numTrees=10,
            maxDepth=5,
            seed=42,
        )

        rf_model = rf_classifier.fit(train_data)

        # 預測
        predictions = rf_model.transform(test_data)

        # 評估
        evaluator = BinaryClassificationEvaluator(
            labelCol="is_high_value", rawPredictionCol="rawPrediction"
        )
        auc = evaluator.evaluate(predictions)

        # 3. 聚類分析：客戶分群
        print("  🎯 客戶聚類分析...")

        kmeans = KMeans(featuresCol="scaled_features", k=4, seed=42)

        kmeans_model = kmeans.fit(scaled_data)
        clustered_data = kmeans_model.transform(scaled_data)

        # 分析聚類結果
        cluster_analysis = (
            clustered_data.groupBy("prediction")
            .agg(
                count("*").alias("cluster_size"),
                avg("total_spent").alias("avg_spent"),
                avg("engagement_score").alias("avg_engagement"),
                avg("age").alias("avg_age"),
            )
            .orderBy("prediction")
        )

        return {
            "classification_model": rf_model,
            "classification_predictions": predictions,
            "classification_auc": auc,
            "clustering_model": kmeans_model,
            "clustered_data": clustered_data,
            "cluster_analysis": cluster_analysis,
            "ml_features": scaled_data,
        }

    def streaming_pipeline(self, datasets: Dict[str, Any]) -> None:
        """流處理管道（模擬）"""
        print("🌊 模擬流處理管道...")

        # 注意：這是一個模擬的流處理示例
        # 在實際環境中，你會從 Kafka、Kinesis 等流數據源讀取

        behaviors_df = datasets["behaviors"]

        # 模擬流處理：實時用戶行為分析
        print("  📡 實時用戶行為分析...")

        # 按時間窗口聚合
        windowed_behaviors = (
            behaviors_df.withColumn(
                "hour", date_format(col("timestamp"), "yyyy-MM-dd HH")
            )
            .groupBy("hour", "action")
            .agg(count("*").alias("action_count"))
            .orderBy("hour", "action")
        )

        # 實時異常檢測
        anomaly_threshold = 100  # 每小時超過100次同一行為視為異常

        anomalies = windowed_behaviors.filter(col("action_count") > anomaly_threshold)

        print(f"  🚨 檢測到 {anomalies.count()} 個異常行為模式")

        if anomalies.count() > 0:
            print("  異常行為詳情：")
            anomalies.show(10, False)

        # 實時推薦
        print("  💡 實時推薦計算...")

        # 模擬實時推薦邏輯
        recent_behaviors = behaviors_df.filter(
            col("timestamp") > (current_timestamp() - expr("INTERVAL 1 HOUR"))
        )

        popular_pages = (
            recent_behaviors.groupBy("page_url")
            .agg(count("*").alias("visit_count"))
            .orderBy(desc("visit_count"))
            .limit(5)
        )

        print("  🔥 實時熱門頁面：")
        popular_pages.show(5, False)

    def performance_optimization(self, datasets: Dict[str, Any]) -> Dict[str, Any]:
        """性能優化示例"""
        print("⚡ 應用性能優化...")

        transactions_df = datasets["transactions"]
        users_df = datasets["users"]

        # 1. 分區優化
        print("  📊 分區優化...")

        # 按日期分區交易數據
        partitioned_transactions = transactions_df.withColumn(
            "transaction_date",
            date_format(to_timestamp(col("transaction_time")), "yyyy-MM-dd"),
        ).repartition(4, col("transaction_date"))

        # 2. 緩存策略
        print("  💾 應用緩存策略...")

        # 緩存經常使用的用戶數據
        cached_users = users_df.cache()

        # 3. 廣播連接優化
        print("  📡 廣播連接優化...")

        # 將小表廣播用於連接
        user_transactions = partitioned_transactions.join(
            broadcast(cached_users), "user_id"
        )

        # 觸發緩存
        cached_users.count()

        # 4. 列式存儲優化
        print("  📋 查詢優化...")

        # 只選擇需要的列
        optimized_query = user_transactions.select(
            "transaction_id", "user_id", "name", "amount", "subscription_tier"
        ).filter(col("amount") > 100)

        query_count = optimized_query.count()
        print(f"  優化查詢結果數量: {query_count}")

        return {
            "partitioned_transactions": partitioned_transactions,
            "cached_users": cached_users,
            "optimized_query": optimized_query,
        }

    def monitoring_and_alerting(
        self, processed_data: Dict[str, Any], ml_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """監控和告警系統"""
        print("📊 監控和告警系統...")

        # 1. 數據質量監控
        print("  🔍 數據質量檢查...")

        transactions_summary = processed_data["user_transaction_summary"]

        quality_metrics = {
            "total_users": transactions_summary.count(),
            "users_with_transactions": transactions_summary.filter(
                col("total_transactions") > 0
            ).count(),
            "high_value_users": transactions_summary.filter(
                col("total_spent") > 1000
            ).count(),
            "avg_transaction_value": transactions_summary.agg(
                avg("avg_transaction_amount")
            ).collect()[0][0],
        }

        # 2. 模型性能監控
        print("  🤖 模型性能監控...")

        model_metrics = {
            "classification_auc": ml_results["classification_auc"],
            "total_predictions": ml_results["classification_predictions"].count(),
            "high_value_predictions": ml_results["classification_predictions"]
            .filter(col("prediction") == 1)
            .count(),
        }

        # 3. 業務指標監控
        print("  💼 業務指標監控...")

        business_metrics = (
            processed_data["product_sales_summary"]
            .agg(
                spark_sum("total_revenue").alias("total_platform_revenue"),
                avg("total_revenue").alias("avg_product_revenue"),
                count("product_id").alias("active_products"),
            )
            .collect()[0]
        )

        # 4. 告警規則
        print("  🚨 告警檢查...")

        alerts = []

        # 數據質量告警
        if (
            quality_metrics["users_with_transactions"] / quality_metrics["total_users"]
            < 0.8
        ):
            alerts.append("警告：用戶交易參與率低於80%")

        # 模型性能告警
        if model_metrics["classification_auc"] < 0.7:
            alerts.append("警告：分類模型AUC低於0.7")

        # 業務指標告警
        if business_metrics["total_platform_revenue"] < 50000:
            alerts.append("警告：平台總收入低於預期")

        monitoring_report = {
            "quality_metrics": quality_metrics,
            "model_metrics": model_metrics,
            "business_metrics": dict(business_metrics.asDict()),
            "alerts": alerts,
            "timestamp": datetime.now().isoformat(),
        }

        return monitoring_report

    def generate_comprehensive_report(
        self,
        datasets: Dict[str, Any],
        processed_data: Dict[str, Any],
        ml_results: Dict[str, Any],
        monitoring_report: Dict[str, Any],
    ) -> None:
        """生成綜合報告"""
        print("📋 生成綜合數據平台報告...")

        print("\n" + "=" * 80)
        print("🏢 綜合數據平台分析報告")
        print("=" * 80)

        # 1. 數據概覽
        print("\n📊 數據概覽：")
        print(f"  • 用戶總數: {datasets['users'].count()}")
        print(f"  • 產品總數: {datasets['products'].count()}")
        print(f"  • 交易總數: {datasets['transactions'].count()}")
        print(f"  • 行為事件總數: {datasets['behaviors'].count()}")

        # 2. 業務洞察
        print("\n💼 業務洞察：")

        # 熱門產品
        top_products = (
            processed_data["product_sales_summary"]
            .orderBy(desc("total_revenue"))
            .limit(3)
        )
        print("  🔥 熱門產品TOP3：")
        for row in top_products.collect():
            print(f"    - {row.product_name}: ${row.total_revenue:,.2f}")

        # 客戶價值分佈
        clv_distribution = processed_data["clv_analysis"].groupBy("clv_segment").count()
        print("  💎 客戶價值分佈：")
        for row in clv_distribution.collect():
            print(f"    - {row.clv_segment}: {row.count} 位客戶")

        # 3. 機器學習結果
        print("\n🤖 機器學習結果：")
        print(f"  • 分類模型AUC: {ml_results['classification_auc']:.3f}")

        cluster_summary = ml_results["cluster_analysis"]
        print("  🎯 客戶聚類分析：")
        for row in cluster_summary.collect():
            print(
                f"    - 群組 {row.prediction}: {row.cluster_size} 人, 平均消費 ${row.avg_spent:,.2f}"
            )

        # 4. 性能指標
        print("\n⚡ 性能優化效果：")
        print("  ✅ 應用了數據分區優化")
        print("  ✅ 使用了廣播連接")
        print("  ✅ 應用了緩存策略")
        print("  ✅ 實施了查詢優化")

        # 5. 監控告警
        print("\n📊 監控狀態：")
        print(
            f"  • 數據質量得分: {monitoring_report['quality_metrics']['users_with_transactions'] / monitoring_report['quality_metrics']['total_users'] * 100:.1f}%"
        )
        print(
            f"  • 模型性能: AUC {monitoring_report['model_metrics']['classification_auc']:.3f}"
        )
        print(
            f"  • 平台總收入: ${monitoring_report['business_metrics']['total_platform_revenue']:,.2f}"
        )

        if monitoring_report["alerts"]:
            print("\n🚨 系統告警：")
            for alert in monitoring_report["alerts"]:
                print(f"  ⚠️  {alert}")
        else:
            print("\n✅ 系統運行正常，無告警")

        # 6. 建議和下一步
        print("\n💡 建議和下一步：")
        print("  1. 考慮增加更多特徵來提升模型性能")
        print("  2. 實施A/B測試來優化推薦算法")
        print("  3. 增加實時流處理能力")
        print("  4. 建立更完善的數據治理流程")
        print("  5. 擴展到多雲部署架構")

        print("\n" + "=" * 80)
        print("📈 數據平台分析完成！")
        print("=" * 80)

    def run_comprehensive_pipeline(self) -> None:
        """運行完整的數據平台管道"""
        print("🚀 啟動綜合數據平台...")

        try:
            # 1. 創建示例數據
            datasets = self.create_sample_datasets()

            # 2. 批處理管道
            processed_data = self.batch_processing_pipeline(datasets)

            # 3. 機器學習管道
            ml_results = self.machine_learning_pipeline(processed_data)

            # 4. 流處理管道（模擬）
            self.streaming_pipeline(datasets)

            # 5. 性能優化
            optimization_results = self.performance_optimization(datasets)

            # 6. 監控和告警
            monitoring_report = self.monitoring_and_alerting(processed_data, ml_results)

            # 7. 生成綜合報告
            self.generate_comprehensive_report(
                datasets, processed_data, ml_results, monitoring_report
            )

        except Exception as e:
            print(f"❌ 數據平台執行出錯: {e}")
            raise

        finally:
            self.cleanup()

    def cleanup(self):
        """清理資源"""
        print("🧹 清理 Spark 資源...")
        self.spark.stop()


def main():
    """主函數"""
    print("=" * 80)
    print("🎯 第8章 練習4：綜合數據平台構建")
    print("=" * 80)

    # 創建並運行數據平台
    platform = ComprehensiveDataPlatform()
    platform.run_comprehensive_pipeline()


if __name__ == "__main__":
    main()


"""
練習任務
========

基礎任務：
1. 運行完整的數據平台管道
2. 理解每個組件的功能和作用
3. 分析生成的業務洞察報告

進階任務：
1. 修改機器學習模型參數，觀察性能變化
2. 添加新的特徵工程步驟
3. 實現更複雜的異常檢測算法
4. 優化流處理管道的性能

高級任務：
1. 集成外部數據源（如API數據）
2. 實現模型的自動重訓練機制
3. 建立更完善的數據血緣追蹤
4. 設計災難恢復和故障轉移方案

擴展練習：
1. 將批處理和流處理結果存儲到不同的存儲系統
2. 實現數據版本控制和回滾機制
3. 添加數據隱私保護措施
4. 建立成本監控和優化系統

學習重點：
- 端到端數據處理管道設計
- 批流一體化數據處理
- 機器學習與數據工程的集成
- 性能調優最佳實踐
- 生產環境監控和運維

思考問題：
1. 如何設計可擴展的數據平台架構？
2. 如何平衡實時性和準確性的需求？
3. 如何確保數據質量和一致性？
4. 如何實現有效的成本控制？
5. 如何處理數據治理和合規要求？
"""
