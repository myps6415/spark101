#!/usr/bin/env python3
"""
第6章練習2：推薦系統
使用 Spark MLlib 構建協同過濾推薦系統
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum, max as spark_max, \
    min as spark_min, stddev, collect_list, size, array_contains, explode, split, \
    rand, round as spark_round, desc, asc, row_number, dense_rank
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType

from pyspark.ml.recommendation import ALS
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline

import numpy as np
from datetime import datetime, timedelta
import random

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("推薦系統練習") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    print("=== 第6章練習2：推薦系統 ===")
    
    # 1. 創建推薦系統數據
    print("\n1. 創建推薦系統數據:")
    
    # 1.1 創建用戶數據
    print("\n1.1 創建用戶數據:")
    users_data = [
        (1, "Alice", 25, "F", "Engineer", "New York"),
        (2, "Bob", 30, "M", "Manager", "Los Angeles"),
        (3, "Charlie", 35, "M", "Designer", "Chicago"),
        (4, "Diana", 28, "F", "Analyst", "Houston"),
        (5, "Eve", 32, "F", "Teacher", "Phoenix"),
        (6, "Frank", 27, "M", "Developer", "Philadelphia"),
        (7, "Grace", 29, "F", "Writer", "San Antonio"),
        (8, "Henry", 33, "M", "Doctor", "San Diego"),
        (9, "Ivy", 26, "F", "Lawyer", "Dallas"),
        (10, "Jack", 31, "M", "Chef", "San Jose")
    ]
    
    users_columns = ["user_id", "name", "age", "gender", "occupation", "city"]
    users_df = spark.createDataFrame(users_data, users_columns)
    
    # 1.2 創建商品數據
    print("\n1.2 創建商品數據:")
    products_data = [
        (101, "Laptop", "Electronics", 1299.99, "Apple MacBook Pro", ["productivity", "professional"]),
        (102, "Smartphone", "Electronics", 899.99, "iPhone 15", ["communication", "mobile"]),
        (103, "Headphones", "Electronics", 299.99, "Sony WH-1000XM4", ["audio", "music"]),
        (104, "Book - Python", "Books", 49.99, "Python Programming", ["education", "programming"]),
        (105, "Book - AI", "Books", 59.99, "Artificial Intelligence", ["education", "ai"]),
        (106, "Coffee Maker", "Home", 199.99, "Breville Espresso", ["kitchen", "coffee"]),
        (107, "Running Shoes", "Sports", 129.99, "Nike Air Max", ["fitness", "running"]),
        (108, "Gaming Chair", "Furniture", 399.99, "Herman Miller", ["gaming", "ergonomic"]),
        (109, "Tablet", "Electronics", 599.99, "iPad Pro", ["portable", "creative"]),
        (110, "Smart Watch", "Electronics", 399.99, "Apple Watch", ["fitness", "smart"])
    ]
    
    products_columns = ["product_id", "name", "category", "price", "description", "tags"]
    products_df = spark.createDataFrame(products_data, products_columns)
    
    # 1.3 創建評分數據
    print("\n1.3 創建用戶評分數據:")
    
    # 生成模擬評分數據
    def generate_ratings():
        ratings = []
        for user_id in range(1, 11):
            # 每個用戶評分5-8個商品
            num_ratings = random.randint(5, 8)
            rated_products = random.sample(range(101, 111), num_ratings)
            
            for product_id in rated_products:
                # 基於用戶和商品特性生成評分
                base_rating = random.uniform(2.5, 4.5)
                
                # 添加一些模式：
                # 女性更喜歡書籍和家居用品
                if user_id in [1, 4, 5, 7, 9]:  # 女性用戶
                    if product_id in [104, 105, 106]:  # 書籍和家居
                        base_rating += random.uniform(0.3, 0.8)
                
                # 年輕用戶更喜歡電子產品
                if user_id in [1, 6, 9]:  # 年輕用戶
                    if product_id in [101, 102, 103, 109, 110]:  # 電子產品
                        base_rating += random.uniform(0.2, 0.6)
                
                # 職業相關偏好
                if user_id in [1, 6]:  # Engineer/Developer
                    if product_id in [101, 104, 105]:  # 技術相關
                        base_rating += random.uniform(0.4, 0.7)
                
                rating = min(5.0, max(1.0, base_rating))
                timestamp = datetime.now() - timedelta(days=random.randint(1, 90))
                ratings.append((user_id, product_id, round(rating, 1), timestamp.strftime("%Y-%m-%d")))
        
        return ratings
    
    ratings_data = generate_ratings()
    ratings_columns = ["user_id", "product_id", "rating", "timestamp"]
    ratings_df = spark.createDataFrame(ratings_data, ratings_columns)
    
    print(f"用戶數: {users_df.count()}")
    print(f"商品數: {products_df.count()}")
    print(f"評分數: {ratings_df.count()}")
    
    print("\n評分數據示例:")
    ratings_df.show(10)
    
    # 2. 數據探索分析
    print("\n2. 數據探索分析:")
    
    # 2.1 評分分布分析
    print("\n2.1 評分分布分析:")
    
    rating_distribution = ratings_df.groupBy("rating").count().orderBy("rating")
    print("評分分布:")
    rating_distribution.show()
    
    # 用戶評分統計
    user_stats = ratings_df.groupBy("user_id").agg(
        count("*").alias("rating_count"),
        avg("rating").alias("avg_rating"),
        stddev("rating").alias("rating_stddev")
    ).join(users_df, "user_id").orderBy("user_id")
    
    print("用戶評分統計:")
    user_stats.select("user_id", "name", "rating_count", "avg_rating").show()
    
    # 商品評分統計  
    product_stats = ratings_df.groupBy("product_id").agg(
        count("*").alias("rating_count"),
        avg("rating").alias("avg_rating"),
        stddev("rating").alias("rating_stddev")
    ).join(products_df, "product_id").orderBy("product_id")
    
    print("商品評分統計:")
    product_stats.select("product_id", "name", "rating_count", "avg_rating").show()
    
    # 3. 協同過濾推薦系統
    print("\n3. 協同過濾推薦系統:")
    
    # 3.1 數據預處理
    print("\n3.1 數據預處理:")
    
    # 分割訓練和測試數據
    train_df, test_df = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    print(f"訓練數據: {train_df.count()} 筆")
    print(f"測試數據: {test_df.count()} 筆")
    
    # 3.2 ALS 模型訓練
    print("\n3.2 ALS 協同過濾模型:")
    
    # 創建 ALS 模型
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="user_id",
        itemCol="product_id", 
        ratingCol="rating",
        coldStartStrategy="drop"
    )
    
    # 訓練模型
    print("訓練 ALS 模型...")
    als_model = als.fit(train_df)
    
    # 預測
    predictions = als_model.transform(test_df)
    
    print("預測結果示例:")
    predictions.select("user_id", "product_id", "rating", "prediction").show(10)
    
    # 3.3 模型評估
    print("\n3.3 模型評估:")
    
    # 計算 RMSE
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"測試集 RMSE: {rmse:.4f}")
    
    # 計算 MAE
    mae_evaluator = RegressionEvaluator(
        metricName="mae",
        labelCol="rating", 
        predictionCol="prediction"
    )
    
    mae = mae_evaluator.evaluate(predictions)
    print(f"測試集 MAE: {mae:.4f}")
    
    # 4. 超參數調優
    print("\n4. 超參數調優:")
    
    # 4.1 網格搜索
    print("\n4.1 網格搜索最佳參數:")
    
    # 創建參數網格
    param_grid = ParamGridBuilder() \
        .addGrid(als.regParam, [0.01, 0.1, 0.3]) \
        .addGrid(als.rank, [5, 10, 15]) \
        .build()
    
    # 交叉驗證
    crossval = CrossValidator(
        estimator=als,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3
    )
    
    print("執行交叉驗證...")
    cv_model = crossval.fit(train_df)
    
    # 最佳模型
    best_model = cv_model.bestModel
    print(f"最佳參數 - Rank: {best_model.rank}, RegParam: {best_model._java_obj.regParam()}")
    
    # 使用最佳模型預測
    best_predictions = best_model.transform(test_df)
    best_rmse = evaluator.evaluate(best_predictions)
    print(f"最佳模型 RMSE: {best_rmse:.4f}")
    
    # 5. 推薦生成
    print("\n5. 推薦生成:")
    
    # 5.1 為用戶生成推薦
    print("\n5.1 為用戶生成推薦:")
    
    # 為所有用戶生成前5個推薦
    user_recommendations = best_model.recommendForAllUsers(5)
    
    print("用戶推薦示例:")
    user_recs_exploded = user_recommendations.select(
        col("user_id"),
        explode(col("recommendations")).alias("recommendation")
    ).select(
        col("user_id"),
        col("recommendation.product_id"),
        col("recommendation.rating").alias("predicted_rating")
    )
    
    # 添加商品信息
    user_recs_with_products = user_recs_exploded.join(
        products_df.select("product_id", "name", "category", "price"),
        "product_id"
    ).join(
        users_df.select("user_id", "name").withColumnRenamed("name", "user_name"),
        "user_id"
    )
    
    print("為用戶 1-3 的推薦:")
    user_recs_with_products.filter(col("user_id").isin([1, 2, 3])) \
        .orderBy("user_id", desc("predicted_rating")).show(15, truncate=False)
    
    # 5.2 為商品生成推薦
    print("\n5.2 為商品生成推薦用戶:")
    
    # 為所有商品生成前3個推薦用戶
    item_recommendations = best_model.recommendForAllItems(3)
    
    item_recs_exploded = item_recommendations.select(
        col("product_id"),
        explode(col("recommendations")).alias("recommendation")
    ).select(
        col("product_id"),
        col("recommendation.user_id"),
        col("recommendation.rating").alias("predicted_rating")
    )
    
    # 添加用戶和商品信息
    item_recs_with_info = item_recs_exploded.join(
        products_df.select("product_id", "name").withColumnRenamed("name", "product_name"),
        "product_id"
    ).join(
        users_df.select("user_id", "name", "occupation"),
        "user_id"
    )
    
    print("為商品推薦用戶 (前3個商品):")
    item_recs_with_info.filter(col("product_id").isin([101, 102, 103])) \
        .orderBy("product_id", desc("predicted_rating")).show(truncate=False)
    
    # 6. 基於內容的推薦
    print("\n6. 基於內容的推薦:")
    
    # 6.1 商品相似度計算
    print("\n6.1 基於類別的商品推薦:")
    
    # 基於商品類別的簡單推薦
    def get_category_recommendations(user_id, num_recs=3):
        # 獲取用戶已評分的商品類別
        user_categories = ratings_df.filter(col("user_id") == user_id) \
            .join(products_df.select("product_id", "category"), "product_id") \
            .groupBy("category").agg(
                avg("rating").alias("avg_rating"),
                count("*").alias("rating_count")
            ).filter(col("avg_rating") >= 4.0) \
            .select("category").collect()
        
        if user_categories:
            preferred_categories = [row["category"] for row in user_categories]
            
            # 找出該類別中用戶未評分的商品
            rated_products = ratings_df.filter(col("user_id") == user_id) \
                .select("product_id").rdd.flatMap(lambda x: x).collect()
            
            recommendations = products_df.filter(
                col("category").isin(preferred_categories) &
                (~col("product_id").isin(rated_products))
            ).join(
                product_stats.select("product_id", "avg_rating"),
                "product_id"
            ).orderBy(desc("avg_rating")).limit(num_recs)
            
            return recommendations
        else:
            return spark.createDataFrame([], products_df.schema)
    
    print("基於內容的推薦示例 (用戶1):")
    content_recs = get_category_recommendations(1)
    content_recs.select("product_id", "name", "category", "price", "avg_rating").show()
    
    # 7. 混合推薦系統
    print("\n7. 混合推薦系統:")
    
    # 7.1 結合協同過濾和基於內容的推薦
    print("\n7.1 混合推薦策略:")
    
    def hybrid_recommendations(user_id, num_recs=5):
        # 協同過濾推薦 (權重 0.7)
        cf_recs = user_recs_with_products.filter(col("user_id") == user_id) \
            .select("product_id", "predicted_rating") \
            .withColumn("cf_score", col("predicted_rating") * 0.7)
        
        # 基於內容的推薦 (權重 0.3)
        content_recs = get_category_recommendations(user_id, 10) \
            .select("product_id", "avg_rating") \
            .withColumn("content_score", col("avg_rating") * 0.3)
        
        # 合併推薦
        hybrid_recs = cf_recs.join(content_recs, "product_id", "outer") \
            .fillna(0) \
            .withColumn(
                "hybrid_score",
                col("cf_score") + col("content_score")
            ).join(
                products_df.select("product_id", "name", "category", "price"),
                "product_id"
            ).orderBy(desc("hybrid_score")).limit(num_recs)
        
        return hybrid_recs
    
    print("混合推薦結果 (用戶1):")
    hybrid_recs = hybrid_recommendations(1)
    hybrid_recs.select("product_id", "name", "category", "hybrid_score").show()
    
    # 8. 推薦系統評估
    print("\n8. 推薦系統評估:")
    
    # 8.1 多樣性評估
    print("\n8.1 推薦多樣性分析:")
    
    # 計算用戶推薦的類別多樣性
    diversity_analysis = user_recs_with_products.groupBy("user_id").agg(
        collect_list("category").alias("recommended_categories"),
        count("*").alias("total_recommendations")
    ).withColumn(
        "unique_categories", 
        size(collect_list("category").over(Window.partitionBy("user_id")))
    ).withColumn(
        "diversity_score",
        col("unique_categories") / col("total_recommendations")
    )
    
    print("推薦多樣性分析:")
    diversity_analysis.select("user_id", "total_recommendations", "unique_categories", "diversity_score").show()
    
    # 8.2 覆蓋率評估
    print("\n8.2 推薦覆蓋率分析:")
    
    # 計算推薦系統的商品覆蓋率
    total_products = products_df.count()
    recommended_products = user_recs_with_products.select("product_id").distinct().count()
    coverage = recommended_products / total_products * 100
    
    print(f"商品覆蓋率: {coverage:.2f}% ({recommended_products}/{total_products})")
    
    # 長尾商品推薦分析
    popular_products = product_stats.filter(col("rating_count") >= 3).select("product_id")
    long_tail_products = product_stats.filter(col("rating_count") < 3).select("product_id")
    
    popular_in_recs = user_recs_with_products.join(popular_products, "product_id", "inner").count()
    long_tail_in_recs = user_recs_with_products.join(long_tail_products, "product_id", "inner").count()
    
    print(f"熱門商品推薦數: {popular_in_recs}")
    print(f"長尾商品推薦數: {long_tail_in_recs}")
    print(f"長尾商品推薦比例: {long_tail_in_recs/(popular_in_recs + long_tail_in_recs)*100:.2f}%")
    
    # 9. 實時推薦
    print("\n9. 實時推薦系統:")
    
    # 9.1 新用戶冷啟動
    print("\n9.1 新用戶冷啟動策略:")
    
    # 基於人氣的推薦
    popularity_recommendations = product_stats.filter(col("rating_count") >= 2) \
        .orderBy(desc("avg_rating"), desc("rating_count")) \
        .limit(5) \
        .join(products_df.select("product_id", "name", "category", "price"), "product_id")
    
    print("新用戶熱門商品推薦:")
    popularity_recommendations.select("product_id", "name", "category", "avg_rating", "rating_count").show()
    
    # 9.2 新商品冷啟動
    print("\n9.2 新商品冷啟動策略:")
    
    # 基於類別和價格的推薦
    def recommend_new_product(product_id):
        product_info = products_df.filter(col("product_id") == product_id).collect()[0]
        
        # 找到該類別中評分較高的用戶
        similar_category_users = ratings_df.join(
            products_df.filter(col("category") == product_info["category"]).select("product_id"),
            "product_id"
        ).groupBy("user_id").agg(
            avg("rating").alias("avg_category_rating")
        ).filter(col("avg_category_rating") >= 4.0) \
        .join(users_df.select("user_id", "name"), "user_id")
        
        return similar_category_users
    
    print("新商品潛在用戶推薦 (商品111 - 假設為新的電子產品):")
    new_product_users = recommend_new_product(101)  # 使用現有商品模擬
    new_product_users.select("user_id", "name", "avg_category_rating").show()
    
    # 10. 推薦系統總結
    print("\n10. 推薦系統總結:")
    
    # 性能指標總結
    performance_metrics = {
        "模型類型": "ALS 協同過濾",
        "訓練數據量": train_df.count(),
        "測試 RMSE": f"{best_rmse:.4f}",
        "商品覆蓋率": f"{coverage:.2f}%",
        "平均推薦多樣性": f"{diversity_analysis.agg(avg('diversity_score')).collect()[0][0]:.3f}",
        "長尾商品比例": f"{long_tail_in_recs/(popular_in_recs + long_tail_in_recs)*100:.2f}%"
    }
    
    print("推薦系統性能指標:")
    for metric, value in performance_metrics.items():
        print(f"- {metric}: {value}")
    
    # 系統優勢
    advantages = [
        "基於用戶行為的協同過濾，個性化程度高",
        "結合內容過濾，提升推薦準確性",
        "混合推薦策略，平衡準確性和多樣性",
        "處理冷啟動問題，支持新用戶和新商品",
        "可擴展到大規模數據集",
        "支持實時推薦更新"
    ]
    
    print("\n系統優勢:")
    for i, advantage in enumerate(advantages, 1):
        print(f"{i}. {advantage}")
    
    # 改進建議
    improvements = [
        "引入時間衰減因子考慮評分時效性",
        "添加隱式反饋數據 (瀏覽、點擊等)",
        "實施深度學習模型 (Neural Collaborative Filtering)",
        "增加用戶畫像和商品特徵",
        "實現A/B測試框架評估推薦效果",
        "添加推薦解釋性功能"
    ]
    
    print("\n改進建議:")
    for i, improvement in enumerate(improvements, 1):
        print(f"{i}. {improvement}")
    
    # 清理資源
    spark.stop()
    print("\n推薦系統練習完成！")

if __name__ == "__main__":
    main()