#!/usr/bin/env python3
"""
推薦系統

這是一個基於 Apache Spark MLlib 的推薦系統，使用協同過濾演算法為用戶推薦商品。

功能特色：
- 協同過濾推薦演算法
- 矩陣分解 (ALS) 實現
- 推薦品質評估
- 個性化推薦
- 批次和即時推薦

使用方法：
    python recommendation_system.py --ratings-path ratings.csv --output-path ./output
"""

import argparse
import sys
import os
import json
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path
from dataclasses import dataclass
import logging
import random

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.feature import StringIndexer, IndexToString
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
from collections import defaultdict


@dataclass
class RecommendationConfig:
    """推薦系統配置"""
    ratings_path: str
    products_path: str = ""
    users_path: str = ""
    output_path: str = "./recommendation_output"
    model_path: str = "./recommendation_model"
    num_recommendations: int = 10
    min_ratings_per_user: int = 5
    min_ratings_per_item: int = 5
    test_ratio: float = 0.2
    als_config: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.als_config is None:
            self.als_config = {
                "rank": 50,
                "maxIter": 10,
                "regParam": 0.01,
                "alpha": 1.0,
                "coldStartStrategy": "drop",
                "nonnegative": True
            }


class DataProcessor:
    """資料處理器"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.ratings_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("item_id", IntegerType(), True),
            StructField("rating", FloatType(), True),
            StructField("timestamp", LongType(), True)
        ])
        
        self.products_schema = StructType([
            StructField("item_id", IntegerType(), True),
            StructField("title", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("brand", StringType(), True),
            StructField("description", StringType(), True)
        ])
        
        self.users_schema = StructType([
            StructField("user_id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("gender", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("location", StringType(), True)
        ])
    
    def load_ratings(self, ratings_path: str) -> DataFrame:
        """
        載入評分資料
        
        Args:
            ratings_path: 評分資料路徑
            
        Returns:
            DataFrame: 評分資料
        """
        try:
            if ratings_path.endswith('.csv'):
                df = self.spark.read.csv(ratings_path, header=True, inferSchema=True)
            elif ratings_path.endswith('.json'):
                df = self.spark.read.json(ratings_path)
            elif ratings_path.endswith('.parquet'):
                df = self.spark.read.parquet(ratings_path)
            else:
                df = self.spark.read.csv(ratings_path, header=True, inferSchema=True)
            
            # 確保欄位名稱正確
            if "userId" in df.columns:
                df = df.withColumnRenamed("userId", "user_id")
            if "itemId" in df.columns:
                df = df.withColumnRenamed("itemId", "item_id")
            if "productId" in df.columns:
                df = df.withColumnRenamed("productId", "item_id")
            
            return df
            
        except Exception as e:
            logging.error(f"載入評分資料失敗: {str(e)}")
            return None
    
    def load_products(self, products_path: str) -> Optional[DataFrame]:
        """
        載入商品資料
        
        Args:
            products_path: 商品資料路徑
            
        Returns:
            Optional[DataFrame]: 商品資料
        """
        if not products_path or not os.path.exists(products_path):
            return None
            
        try:
            if products_path.endswith('.csv'):
                df = self.spark.read.csv(products_path, header=True, inferSchema=True)
            elif products_path.endswith('.json'):
                df = self.spark.read.json(products_path)
            elif products_path.endswith('.parquet'):
                df = self.spark.read.parquet(products_path)
            else:
                df = self.spark.read.csv(products_path, header=True, inferSchema=True)
            
            # 確保欄位名稱正確
            if "productId" in df.columns:
                df = df.withColumnRenamed("productId", "item_id")
            if "itemId" in df.columns:
                df = df.withColumnRenamed("itemId", "item_id")
            
            return df
            
        except Exception as e:
            logging.error(f"載入商品資料失敗: {str(e)}")
            return None
    
    def load_users(self, users_path: str) -> Optional[DataFrame]:
        """
        載入用戶資料
        
        Args:
            users_path: 用戶資料路徑
            
        Returns:
            Optional[DataFrame]: 用戶資料
        """
        if not users_path or not os.path.exists(users_path):
            return None
            
        try:
            if users_path.endswith('.csv'):
                df = self.spark.read.csv(users_path, header=True, inferSchema=True)
            elif users_path.endswith('.json'):
                df = self.spark.read.json(users_path)
            elif users_path.endswith('.parquet'):
                df = self.spark.read.parquet(users_path)
            else:
                df = self.spark.read.csv(users_path, header=True, inferSchema=True)
            
            # 確保欄位名稱正確
            if "userId" in df.columns:
                df = df.withColumnRenamed("userId", "user_id")
            
            return df
            
        except Exception as e:
            logging.error(f"載入用戶資料失敗: {str(e)}")
            return None
    
    def clean_ratings_data(self, ratings_df: DataFrame, config: RecommendationConfig) -> DataFrame:
        """
        清理評分資料
        
        Args:
            ratings_df: 原始評分資料
            config: 推薦系統配置
            
        Returns:
            DataFrame: 清理後的評分資料
        """
        print("正在清理評分資料...")\n        
        # 移除空值
        cleaned_df = ratings_df.filter(\n            (col("user_id").isNotNull()) & \n            (col("item_id").isNotNull()) & \n            (col("rating").isNotNull())\n        )\n        \n        # 移除評分次數過少的用戶和商品\n        user_counts = cleaned_df.groupBy("user_id").count().filter(col("count") >= config.min_ratings_per_user)\n        item_counts = cleaned_df.groupBy("item_id").count().filter(col("count") >= config.min_ratings_per_item)\n        \n        # 保留活躍用戶和熱門商品\n        cleaned_df = cleaned_df.join(user_counts.select("user_id"), "user_id") \\\n                               .join(item_counts.select("item_id"), "item_id")\n        \n        # 確保評分在合理範圍內\n        cleaned_df = cleaned_df.filter(\n            (col("rating") >= 1.0) & \n            (col("rating") <= 5.0)\n        )\n        \n        print(f"資料清理完成: {cleaned_df.count()} 條評分記錄")\n        return cleaned_df
n    
    def generate_sample_data(self, num_users: int = 1000, num_items: int = 500, num_ratings: int = 50000) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        生成範例資料
        
        Args:
            num_users: 用戶數量
            num_items: 商品數量
            num_ratings: 評分數量
            
        Returns:
            Tuple[DataFrame, DataFrame, DataFrame]: 評分、商品、用戶資料
        """
        print(f"生成範例資料: {num_users} 用戶, {num_items} 商品, {num_ratings} 評分")
        
        # 生成商品資料
        categories = ['Electronics', 'Books', 'Clothing', 'Sports', 'Home', 'Beauty', 'Automotive', 'Food']
        brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'Sony', 'LG', 'Canon', 'Dell']
        
        products = []
        for i in range(num_items):
            products.append((
                i + 1,  # item_id
                f"Product {i+1}",  # title
                random.choice(categories),  # category
                random.uniform(10, 500),  # price
                random.choice(brands),  # brand
                f"Description for product {i+1}"  # description
            ))
        
        products_df = self.spark.createDataFrame(products, self.products_schema)
        
        # 生成用戶資料
        genders = ['M', 'F']
        occupations = ['Student', 'Engineer', 'Teacher', 'Doctor', 'Manager', 'Designer', 'Writer']
        locations = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia']
        
        users = []
        for i in range(num_users):
            users.append((
                i + 1,  # user_id
                random.randint(18, 65),  # age
                random.choice(genders),  # gender
                random.choice(occupations),  # occupation
                random.choice(locations)  # location
            ))
        
        users_df = self.spark.createDataFrame(users, self.users_schema)
        
        # 生成評分資料（考慮用戶偏好）
        ratings = []
        user_preferences = {}  # 用戶偏好的商品類別
        
        # 為每個用戶設定偏好類別
        for user_id in range(1, num_users + 1):
            user_preferences[user_id] = random.choice(categories)
        
        for _ in range(num_ratings):
            user_id = random.randint(1, num_users)
            item_id = random.randint(1, num_items)
            
            # 根據用戶偏好調整評分
            item_category = products[item_id - 1][2]  # 商品類別
            preferred_category = user_preferences[user_id]
            
            if item_category == preferred_category:
                # 偏好類別，給予較高評分
                rating = random.choices([3, 4, 5], weights=[0.2, 0.4, 0.4])[0]
            else:
                # 非偏好類別，評分較分散
                rating = random.choices([1, 2, 3, 4, 5], weights=[0.1, 0.2, 0.4, 0.2, 0.1])[0]
            
            timestamp = int(time.time()) - random.randint(0, 365 * 24 * 3600)  # 過去一年內
            
            ratings.append((user_id, item_id, float(rating), timestamp))
        
        ratings_df = self.spark.createDataFrame(ratings, self.ratings_schema)
        
        print(f"範例資料生成完成")
        return ratings_df, products_df, users_df


class RecommendationEngine:
    """推薦引擎"""
    
    def __init__(self, spark: SparkSession, config: RecommendationConfig):
        self.spark = spark
        self.config = config
        self.model = None
        self.evaluator = RegressionEvaluator(
            metricName="rmse",
            labelCol="rating",
            predictionCol="prediction"
        )
    
    def train_model(self, train_df: DataFrame) -> None:
        """
        訓練推薦模型
        
        Args:
            train_df: 訓練資料
        """
        print("開始訓練 ALS 模型...")
        
        # 建立 ALS 模型
        als = ALS(
            userCol="user_id",
            itemCol="item_id",
            ratingCol="rating",
            **self.config.als_config
        )
        
        # 訓練模型
        start_time = time.time()
        self.model = als.fit(train_df)
        training_time = time.time() - start_time
        
        print(f"模型訓練完成，耗時: {training_time:.2f} 秒")
    
    def tune_hyperparameters(self, train_df: DataFrame, validation_df: DataFrame) -> Dict[str, Any]:
        """
        超參數調優
        
        Args:
            train_df: 訓練資料
            validation_df: 驗證資料
            
        Returns:
            Dict[str, Any]: 最佳超參數
        """
        print("開始超參數調優...")
        
        # 建立參數網格
        als = ALS(
            userCol="user_id",
            itemCol="item_id",
            ratingCol="rating",
            coldStartStrategy="drop",
            nonnegative=True
        )
        
        param_grid = ParamGridBuilder() \
            .addGrid(als.rank, [10, 50, 100]) \
            .addGrid(als.maxIter, [5, 10, 20]) \
            .addGrid(als.regParam, [0.01, 0.05, 0.1]) \
            .build()
        
        # 交叉驗證
        crossval = CrossValidator(
            estimator=als,
            estimatorParamMaps=param_grid,
            evaluator=self.evaluator,
            numFolds=3
        )
        
        # 執行調優
        cv_model = crossval.fit(train_df)
        
        # 獲取最佳模型
        best_model = cv_model.bestModel
        best_params = {
            "rank": best_model.rank,
            "maxIter": best_model._java_obj.parent().getMaxIter(),
            "regParam": best_model._java_obj.parent().getRegParam(),
            "alpha": best_model._java_obj.parent().getAlpha()
        }
        
        # 在驗證集上評估
        predictions = best_model.transform(validation_df)
        rmse = self.evaluator.evaluate(predictions)
        
        print(f"最佳模型 RMSE: {rmse:.4f}")
        print(f"最佳超參數: {best_params}")
        
        # 更新配置
        self.config.als_config.update(best_params)
        self.model = best_model
        
        return best_params
    
    def evaluate_model(self, test_df: DataFrame) -> Dict[str, float]:
        """
        評估模型性能
        
        Args:
            test_df: 測試資料
            
        Returns:
            Dict[str, float]: 評估指標
        """
        print("評估模型性能...")
        
        if self.model is None:
            raise ValueError("模型尚未訓練")
        
        # 預測
        predictions = self.model.transform(test_df)
        
        # 計算評估指標
        rmse = self.evaluator.evaluate(predictions)
        
        mae_evaluator = RegressionEvaluator(
            metricName="mae",
            labelCol="rating",
            predictionCol="prediction"
        )
        mae = mae_evaluator.evaluate(predictions)
        
        r2_evaluator = RegressionEvaluator(
            metricName="r2",
            labelCol="rating",
            predictionCol="prediction"
        )
        r2 = r2_evaluator.evaluate(predictions)
        
        metrics = {
            "rmse": rmse,
            "mae": mae,
            "r2": r2
        }
        
        print(f"模型評估結果: RMSE={rmse:.4f}, MAE={mae:.4f}, R²={r2:.4f}")
        
        return metrics
    
    def generate_user_recommendations(self, user_ids: List[int], num_recommendations: int = None) -> DataFrame:
        """
        為指定用戶生成推薦
        
        Args:
            user_ids: 用戶ID列表
            num_recommendations: 推薦數量
            
        Returns:
            DataFrame: 推薦結果
        """
        if self.model is None:
            raise ValueError("模型尚未訓練")
        
        if num_recommendations is None:
            num_recommendations = self.config.num_recommendations
        
        print(f"為 {len(user_ids)} 個用戶生成推薦...")
        
        # 建立用戶DataFrame
        users_df = self.spark.createDataFrame([(uid,) for uid in user_ids], ["user_id"])
        
        # 生成推薦
        recommendations = self.model.recommendForUserSubset(users_df, num_recommendations)
        
        return recommendations
    
    def generate_item_recommendations(self, item_ids: List[int], num_recommendations: int = None) -> DataFrame:
        """
        為指定商品生成相似商品推薦
        
        Args:
            item_ids: 商品ID列表
            num_recommendations: 推薦數量
            
        Returns:
            DataFrame: 推薦結果
        """
        if self.model is None:
            raise ValueError("模型尚未訓練")
        
        if num_recommendations is None:
            num_recommendations = self.config.num_recommendations
        
        print(f"為 {len(item_ids)} 個商品生成相似推薦...")
        
        # 建立商品DataFrame
        items_df = self.spark.createDataFrame([(iid,) for iid in item_ids], ["item_id"])
        
        # 生成推薦
        recommendations = self.model.recommendForItemSubset(items_df, num_recommendations)
        
        return recommendations
    
    def save_model(self, model_path: str) -> None:
        """
        保存模型
        
        Args:
            model_path: 模型保存路徑
        """
        if self.model is None:
            raise ValueError("模型尚未訓練")
        
        print(f"保存模型至: {model_path}")
        
        try:
            self.model.write().overwrite().save(model_path)
            print("模型保存成功")
        except Exception as e:
            print(f"模型保存失敗: {str(e)}")
    
    def load_model(self, model_path: str) -> None:
        """
        載入模型
        
        Args:
            model_path: 模型路徑
        """
        print(f"載入模型: {model_path}")
        
        try:
            from pyspark.ml.recommendation import ALSModel
            self.model = ALSModel.load(model_path)
            print("模型載入成功")
        except Exception as e:
            print(f"模型載入失敗: {str(e)}")


class RecommendationAnalyzer:
    """推薦分析器"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def analyze_data_quality(self, ratings_df: DataFrame, products_df: DataFrame = None, users_df: DataFrame = None) -> Dict[str, Any]:
        """
        分析資料品質
        
        Args:
            ratings_df: 評分資料
            products_df: 商品資料
            users_df: 用戶資料
            
        Returns:
            Dict[str, Any]: 資料品質分析結果
        """
        print("分析資料品質...")
        
        analysis = {
            "ratings": {},
            "products": {},
            "users": {}
        }
        
        # 分析評分資料
        total_ratings = ratings_df.count()
        unique_users = ratings_df.select("user_id").distinct().count()
        unique_items = ratings_df.select("item_id").distinct().count()
        
        # 評分分佈
        rating_distribution = ratings_df.groupBy("rating").count().orderBy("rating").collect()
        
        # 用戶活躍度
        user_activity = ratings_df.groupBy("user_id").count()
        user_activity_stats = user_activity.describe("count").collect()
        
        # 商品受歡迎程度
        item_popularity = ratings_df.groupBy("item_id").count()
        item_popularity_stats = item_popularity.describe("count").collect()
        
        # 資料稀疏度
        sparsity = 1.0 - (total_ratings / (unique_users * unique_items))
        
        analysis["ratings"] = {
            "total_ratings": total_ratings,
            "unique_users": unique_users,
            "unique_items": unique_items,
            "sparsity": sparsity,
            "rating_distribution": {str(row["rating"]): row["count"] for row in rating_distribution},
            "user_activity_stats": {row["summary"]: row["count"] for row in user_activity_stats},
            "item_popularity_stats": {row["summary"]: row["count"] for row in item_popularity_stats}
        }
        
        # 分析商品資料
        if products_df is not None:
            category_distribution = products_df.groupBy("category").count().orderBy(col("count").desc()).collect()
            analysis["products"] = {
                "total_products": products_df.count(),
                "category_distribution": {row["category"]: row["count"] for row in category_distribution}
            }
        
        # 分析用戶資料
        if users_df is not None:
            gender_distribution = users_df.groupBy("gender").count().collect()
            occupation_distribution = users_df.groupBy("occupation").count().orderBy(col("count").desc()).collect()
            analysis["users"] = {
                "total_users": users_df.count(),
                "gender_distribution": {row["gender"]: row["count"] for row in gender_distribution},
                "occupation_distribution": {row["occupation"]: row["count"] for row in occupation_distribution}
            }
        
        return analysis
    
    def analyze_recommendation_quality(self, recommendations_df: DataFrame, products_df: DataFrame = None) -> Dict[str, Any]:
        """
        分析推薦品質
        
        Args:
            recommendations_df: 推薦結果
            products_df: 商品資料
            
        Returns:
            Dict[str, Any]: 推薦品質分析結果
        """
        print("分析推薦品質...")
        
        # 展開推薦結果
        exploded_recommendations = recommendations_df.select(
            "user_id",
            explode("recommendations").alias("recommendation")
        ).select(
            "user_id",
            col("recommendation.item_id").alias("item_id"),
            col("recommendation.rating").alias("predicted_rating")
        )
        
        # 計算覆蓋率
        recommended_items = exploded_recommendations.select("item_id").distinct().count()
        
        if products_df is not None:
            total_items = products_df.count()
            coverage = recommended_items / total_items
        else:
            coverage = None
        
        # 計算多樣性
        diversity_score = None
        if products_df is not None:
            # 計算推薦的類別多樣性
            recommended_categories = exploded_recommendations.join(
                products_df, "item_id"
            ).select("category").distinct().count()
            
            total_categories = products_df.select("category").distinct().count()
            diversity_score = recommended_categories / total_categories
        
        # 計算新穎性
        item_popularity = exploded_recommendations.groupBy("item_id").count()
        avg_popularity = item_popularity.agg(avg("count")).collect()[0][0]
        
        # 分析預測評分分佈
        rating_stats = exploded_recommendations.describe("predicted_rating").collect()
        
        quality_analysis = {
            "coverage": coverage,
            "diversity": diversity_score,
            "average_popularity": avg_popularity,
            "prediction_stats": {row["summary"]: float(row["predicted_rating"]) for row in rating_stats}
        }
        
        return quality_analysis
    
    def generate_user_profile(self, user_id: int, ratings_df: DataFrame, products_df: DataFrame = None) -> Dict[str, Any]:
        """
        生成用戶畫像
        
        Args:
            user_id: 用戶ID
            ratings_df: 評分資料
            products_df: 商品資料
            
        Returns:
            Dict[str, Any]: 用戶畫像
        """
        print(f"生成用戶 {user_id} 的畫像...")
        
        user_ratings = ratings_df.filter(col("user_id") == user_id)
        
        if user_ratings.count() == 0:
            return {"error": f"用戶 {user_id} 沒有評分記錄"}
        
        # 基本統計
        total_ratings = user_ratings.count()
        avg_rating = user_ratings.agg(avg("rating")).collect()[0][0]
        rating_distribution = user_ratings.groupBy("rating").count().orderBy("rating").collect()
        
        profile = {
            "user_id": user_id,
            "total_ratings": total_ratings,
            "average_rating": avg_rating,
            "rating_distribution": {str(row["rating"]): row["count"] for row in rating_distribution}
        }
        
        # 分析偏好
        if products_df is not None:
            user_preferences = user_ratings.join(products_df, "item_id") \
                                          .groupBy("category") \
                                          .agg(
                                              count("*").alias("rating_count"),
                                              avg("rating").alias("avg_rating")
                                          ) \
                                          .orderBy(col("rating_count").desc())
            
            preferences = user_preferences.collect()
            profile["category_preferences"] = [
                {
                    "category": row["category"],
                    "rating_count": row["rating_count"],
                    "avg_rating": row["avg_rating"]
                } for row in preferences
            ]
        
        return profile


class RecommendationSystem:
    """推薦系統主類"""
    
    def __init__(self, config: RecommendationConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.data_processor = DataProcessor(spark)
        self.recommendation_engine = RecommendationEngine(spark, config)
        self.analyzer = RecommendationAnalyzer(spark)
        
        # 設定日誌
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run_training_pipeline(self) -> Dict[str, Any]:
        """
        執行訓練流程
        
        Returns:
            Dict[str, Any]: 訓練結果
        """
        self.logger.info("開始推薦系統訓練流程")
        
        # 載入資料
        if os.path.exists(self.config.ratings_path):
            ratings_df = self.data_processor.load_ratings(self.config.ratings_path)
            products_df = self.data_processor.load_products(self.config.products_path)
            users_df = self.data_processor.load_users(self.config.users_path)
        else:
            # 使用範例資料
            self.logger.info("使用範例資料進行訓練")
            ratings_df, products_df, users_df = self.data_processor.generate_sample_data()
        
        if ratings_df is None:
            raise ValueError("無法載入評分資料")
        
        # 清理資料
        clean_ratings = self.data_processor.clean_ratings_data(ratings_df, self.config)
        
        # 分析資料品質
        data_quality = self.analyzer.analyze_data_quality(clean_ratings, products_df, users_df)
        
        # 分割訓練和測試資料
        train_df, test_df = clean_ratings.randomSplit([1 - self.config.test_ratio, self.config.test_ratio], seed=42)
        
        self.logger.info(f"訓練資料: {train_df.count()} 筆")
        self.logger.info(f"測試資料: {test_df.count()} 筆")
        
        # 訓練模型
        self.recommendation_engine.train_model(train_df)
        
        # 評估模型
        metrics = self.recommendation_engine.evaluate_model(test_df)
        
        # 生成推薦範例
        sample_users = clean_ratings.select("user_id").distinct().limit(10).rdd.map(lambda x: x[0]).collect()
        recommendations = self.recommendation_engine.generate_user_recommendations(sample_users)
        
        # 分析推薦品質
        recommendation_quality = self.analyzer.analyze_recommendation_quality(recommendations, products_df)
        
        # 生成報告
        training_report = {
            "timestamp": datetime.now().isoformat(),
            "config": self.config.__dict__,
            "data_quality": data_quality,
            "model_metrics": metrics,
            "recommendation_quality": recommendation_quality,
            "sample_recommendations": self.format_recommendations(recommendations, products_df)
        }
        
        # 保存模型和報告
        self.save_results(training_report)
        
        self.logger.info("訓練流程完成")
        return training_report
    
    def format_recommendations(self, recommendations_df: DataFrame, products_df: DataFrame = None) -> List[Dict[str, Any]]:
        """
        格式化推薦結果
        
        Args:
            recommendations_df: 推薦結果
            products_df: 商品資料
            
        Returns:
            List[Dict[str, Any]]: 格式化的推薦結果
        """
        formatted_recommendations = []
        
        for row in recommendations_df.collect():
            user_id = row["user_id"]
            recommendations = row["recommendations"]
            
            user_recommendations = []
            for rec in recommendations:
                item_id = rec["item_id"]
                predicted_rating = rec["rating"]
                
                recommendation = {
                    "item_id": item_id,
                    "predicted_rating": predicted_rating
                }
                
                # 添加商品資訊
                if products_df is not None:
                    product_info = products_df.filter(col("item_id") == item_id).collect()
                    if product_info:
                        product = product_info[0]
                        recommendation.update({
                            "title": product["title"],
                            "category": product["category"],
                            "price": product["price"],
                            "brand": product["brand"]
                        })
                
                user_recommendations.append(recommendation)
            
            formatted_recommendations.append({
                "user_id": user_id,
                "recommendations": user_recommendations
            })
        
        return formatted_recommendations
    
    def generate_recommendations_for_user(self, user_id: int, num_recommendations: int = None) -> Dict[str, Any]:
        """
        為指定用戶生成推薦
        
        Args:
            user_id: 用戶ID
            num_recommendations: 推薦數量
            
        Returns:
            Dict[str, Any]: 推薦結果
        """
        if num_recommendations is None:
            num_recommendations = self.config.num_recommendations
        
        self.logger.info(f"為用戶 {user_id} 生成 {num_recommendations} 個推薦")
        
        # 載入模型
        if self.recommendation_engine.model is None:
            model_path = self.config.model_path
            if os.path.exists(model_path):
                self.recommendation_engine.load_model(model_path)
            else:
                raise ValueError("模型不存在，請先訓練模型")
        
        # 生成推薦
        recommendations = self.recommendation_engine.generate_user_recommendations([user_id], num_recommendations)
        
        # 載入商品資料
        products_df = self.data_processor.load_products(self.config.products_path)
        
        # 格式化結果
        formatted_recommendations = self.format_recommendations(recommendations, products_df)
        
        return {
            "user_id": user_id,
            "timestamp": datetime.now().isoformat(),
            "recommendations": formatted_recommendations[0]["recommendations"] if formatted_recommendations else []
        }
    
    def save_results(self, training_report: Dict[str, Any]) -> None:
        """
        保存結果
        
        Args:
            training_report: 訓練報告
        """
        os.makedirs(self.config.output_path, exist_ok=True)
        
        # 保存訓練報告
        report_path = os.path.join(self.config.output_path, "training_report.json")
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(training_report, f, ensure_ascii=False, indent=2, default=str)
        
        # 保存模型
        self.recommendation_engine.save_model(self.config.model_path)
        
        # 生成可視化報告
        self.generate_visualizations(training_report)
        
        self.logger.info(f"結果已保存至: {self.config.output_path}")
    
    def generate_visualizations(self, training_report: Dict[str, Any]) -> None:
        """
        生成可視化報告
        
        Args:
            training_report: 訓練報告
        """
        self.logger.info("生成可視化報告")
        
        # 評分分佈圖
        if "rating_distribution" in training_report["data_quality"]["ratings"]:
            rating_dist = training_report["data_quality"]["ratings"]["rating_distribution"]
            
            plt.figure(figsize=(10, 6))
            ratings = list(rating_dist.keys())
            counts = list(rating_dist.values())
            
            plt.bar(ratings, counts, alpha=0.7)
            plt.title('評分分佈')
            plt.xlabel('評分')
            plt.ylabel('數量')
            plt.grid(True, alpha=0.3)
            
            viz_path = os.path.join(self.config.output_path, 'rating_distribution.png')
            plt.savefig(viz_path, dpi=300, bbox_inches='tight')
            plt.close()
        
        # 模型評估指標圖
        if "model_metrics" in training_report:
            metrics = training_report["model_metrics"]
            
            plt.figure(figsize=(10, 6))
            metric_names = list(metrics.keys())
            metric_values = list(metrics.values())
            
            plt.bar(metric_names, metric_values, alpha=0.7, color=['red', 'green', 'blue'])
            plt.title('模型評估指標')
            plt.ylabel('數值')
            
            for i, v in enumerate(metric_values):
                plt.text(i, v + 0.01, f'{v:.4f}', ha='center', va='bottom')
            
            viz_path = os.path.join(self.config.output_path, 'model_metrics.png')
            plt.savefig(viz_path, dpi=300, bbox_inches='tight')
            plt.close()
        
        self.logger.info("可視化報告生成完成")


def create_sample_config() -> RecommendationConfig:
    """
    建立範例配置
    
    Returns:
        RecommendationConfig: 範例配置
    """
    return RecommendationConfig(
        ratings_path="./sample_ratings.csv",
        products_path="./sample_products.csv",
        users_path="./sample_users.csv",
        output_path="./recommendation_output",
        model_path="./recommendation_model",
        num_recommendations=10,
        min_ratings_per_user=5,
        min_ratings_per_item=5,
        test_ratio=0.2,
        als_config={
            "rank": 50,
            "maxIter": 10,
            "regParam": 0.01,
            "alpha": 1.0,
            "coldStartStrategy": "drop",
            "nonnegative": True
        }
    )


def main():
    """主函數"""
    parser = argparse.ArgumentParser(description='推薦系統')
    parser.add_argument('--ratings-path', required=True, help='評分資料路徑')
    parser.add_argument('--products-path', help='商品資料路徑')
    parser.add_argument('--users-path', help='用戶資料路徑')
    parser.add_argument('--output-path', default='./recommendation_output', help='輸出路徑')
    parser.add_argument('--model-path', default='./recommendation_model', help='模型路徑')
    parser.add_argument('--mode', choices=['train', 'recommend'], default='train', help='運行模式')
    parser.add_argument('--user-id', type=int, help='用戶ID（推薦模式）')
    parser.add_argument('--num-recommendations', type=int, default=10, help='推薦數量')
    parser.add_argument('--app-name', default='RecommendationSystem', help='Spark 應用名稱')
    
    args = parser.parse_args()
    
    # 建立輸出目錄
    os.makedirs(args.output_path, exist_ok=True)
    
    # 建立配置
    config = RecommendationConfig(
        ratings_path=args.ratings_path,
        products_path=args.products_path or "",
        users_path=args.users_path or "",
        output_path=args.output_path,
        model_path=args.model_path,
        num_recommendations=args.num_recommendations
    )
    
    # 建立 Spark 會話
    spark = SparkSession.builder \
        .appName(args.app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    try:
        # 建立推薦系統
        rec_system = RecommendationSystem(config, spark)
        
        if args.mode == 'train':
            # 訓練模式
            training_report = rec_system.run_training_pipeline()
            print(f"訓練完成！結果已保存至: {args.output_path}")
            print(f"模型 RMSE: {training_report['model_metrics']['rmse']:.4f}")
            
        elif args.mode == 'recommend':
            # 推薦模式
            if args.user_id is None:
                print("推薦模式需要指定用戶ID")
                sys.exit(1)
            
            recommendations = rec_system.generate_recommendations_for_user(
                args.user_id, 
                args.num_recommendations
            )
            
            print(f"\n為用戶 {args.user_id} 的推薦結果:")
            for i, rec in enumerate(recommendations["recommendations"], 1):
                print(f"{i}. 商品 {rec['item_id']}: 預測評分 {rec['predicted_rating']:.2f}")
                if 'title' in rec:
                    print(f"   標題: {rec['title']}")
                if 'category' in rec:
                    print(f"   類別: {rec['category']}")
                if 'price' in rec:
                    print(f"   價格: ${rec['price']:.2f}")
                print()
    
    except Exception as e:
        print(f"錯誤: {str(e)}")
        sys.exit(1)
    
    finally:
        # 關閉 Spark 會話
        spark.stop()


if __name__ == "__main__":
    main()