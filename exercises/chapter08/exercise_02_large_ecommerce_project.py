#!/usr/bin/env python3
"""
第8章練習2：大型電商實時分析平台
建立一個完整的實時電商數據分析平台，處理用戶行為、訂單交易、庫存管理等多個數據流
"""

import json
import queue
import random
import threading
import time
from datetime import datetime, timedelta

from pyspark.ml import Pipeline
from pyspark.ml.classification import (LogisticRegression,
                                       RandomForestClassifier)
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   RegressionEvaluator)
from pyspark.ml.feature import (IndexToString, StandardScaler, StringIndexer,
                                VectorAssembler)
from pyspark.ml.recommendation import ALS
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (array, array_contains, asc, avg, broadcast,
                                   col, collect_list, concat_ws, count,
                                   current_timestamp, date_format, dayofweek,
                                   dense_rank, desc, explode, expr, first,
                                   from_json, hour, lag, last, lead, lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import pow as spark_pow
from pyspark.sql.functions import rand, regexp_extract
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import row_number, split, sqrt, stddev, struct
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_json, unix_timestamp, when, window
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               MapType, StringType, StructField, StructType,
                               TimestampType)
from pyspark.sql.window import Window


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("大型電商實時分析平台")
        .master("local[*]")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/ecommerce_checkpoint")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=== 第8章練習2：大型電商實時分析平台 ===")

    # 1. 數據模型定義
    print("\n1. 定義數據模型和 Schema:")

    # 1.1 用戶行為數據模型
    user_behavior_schema = StructType(
        [
            StructField("user_id", StringType(), True),
            StructField("session_id", StringType(), True),
            StructField(
                "event_type", StringType(), True
            ),  # view, click, cart, purchase, search
            StructField("product_id", StringType(), True),
            StructField("category_id", StringType(), True),
            StructField("brand_id", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("page_url", StringType(), True),
            StructField("referrer", StringType(), True),
            StructField("device_type", StringType(), True),  # mobile, desktop, tablet
            StructField("location", StringType(), True),
            StructField("search_query", StringType(), True),
        ]
    )

    # 1.2 交易數據模型
    transaction_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("user_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("discount_amount", DoubleType(), True),
            StructField("payment_method", StringType(), True),
            StructField(
                "status", StringType(), True
            ),  # pending, paid, shipped, delivered, cancelled
            StructField("timestamp", TimestampType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("promotion_code", StringType(), True),
        ]
    )

    # 1.3 庫存數據模型
    inventory_schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("warehouse_id", StringType(), True),
            StructField("stock_quantity", IntegerType(), True),
            StructField("reserved_quantity", IntegerType(), True),
            StructField("reorder_level", IntegerType(), True),
            StructField("last_updated", TimestampType(), True),
            StructField("cost_price", DoubleType(), True),
            StructField("supplier_id", StringType(), True),
        ]
    )

    print("數據模型定義完成")

    # 2. 模擬數據生成器
    print("\n2. 實時數據生成器:")

    class EcommerceDataGenerator:
        """電商數據生成器"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.users = [f"user_{i}" for i in range(1, 10001)]
            self.products = [f"product_{i}" for i in range(1, 5001)]
            self.categories = [f"category_{i}" for i in range(1, 101)]
            self.brands = [f"brand_{i}" for i in range(1, 501)]
            self.warehouses = [f"warehouse_{i}" for i in range(1, 21)]

        def generate_user_behavior_stream(self):
            """生成用戶行為數據流"""

            # 使用 rate source 模擬實時數據
            base_stream = (
                self.spark.readStream.format("rate")
                .option("rowsPerSecond", 50)
                .option("numPartitions", 4)
                .load()
            )

            # 生成用戶行為事件
            behavior_stream = (
                base_stream.select(
                    col("timestamp"),
                    (col("value") % 10000).alias("user_idx"),
                    (col("value") % 5000).alias("product_idx"),
                    (col("value") % 100).alias("category_idx"),
                    (col("value") % 500).alias("brand_idx"),
                    (col("value") % 10).alias("event_type_idx"),
                    (col("value") % 1000).alias("session_idx"),
                    col("value").alias("sequence"),
                )
                .withColumn("user_id", concat_ws("", lit("user_"), col("user_idx") + 1))
                .withColumn(
                    "product_id", concat_ws("", lit("product_"), col("product_idx") + 1)
                )
                .withColumn(
                    "category_id",
                    concat_ws("", lit("category_"), col("category_idx") + 1),
                )
                .withColumn(
                    "brand_id", concat_ws("", lit("brand_"), col("brand_idx") + 1)
                )
                .withColumn(
                    "session_id",
                    concat_ws("", col("user_id"), lit("_session_"), col("session_idx")),
                )
                .withColumn(
                    "event_type",
                    when(col("event_type_idx") == 0, "view")
                    .when(col("event_type_idx") == 1, "click")
                    .when(col("event_type_idx") == 2, "search")
                    .when(col("event_type_idx") == 3, "cart")
                    .when(col("event_type_idx") == 4, "purchase")
                    .otherwise("view"),
                )
                .withColumn(
                    "device_type",
                    when(col("sequence") % 3 == 0, "mobile")
                    .when(col("sequence") % 3 == 1, "desktop")
                    .otherwise("tablet"),
                )
                .withColumn("price", (col("sequence") % 1000 + 10).cast("double"))
                .withColumn(
                    "quantity",
                    when(col("event_type") == "purchase", (col("sequence") % 5 + 1))
                    .when(col("event_type") == "cart", (col("sequence") % 3 + 1))
                    .otherwise(1),
                )
                .withColumn(
                    "page_url", concat_ws("", lit("/product/"), col("product_id"))
                )
                .withColumn(
                    "referrer",
                    when(col("sequence") % 4 == 0, "google.com")
                    .when(col("sequence") % 4 == 1, "facebook.com")
                    .when(col("sequence") % 4 == 2, "direct")
                    .otherwise("organic"),
                )
                .withColumn(
                    "location",
                    when(col("sequence") % 5 == 0, "US")
                    .when(col("sequence") % 5 == 1, "CN")
                    .when(col("sequence") % 5 == 2, "JP")
                    .when(col("sequence") % 5 == 3, "UK")
                    .otherwise("DE"),
                )
                .withColumn(
                    "search_query",
                    when(
                        col("event_type") == "search",
                        concat_ws(" ", lit("search"), col("category_id")),
                    ).otherwise(None),
                )
                .select([field.name for field in user_behavior_schema.fields])
            )

            return behavior_stream

        def generate_transaction_stream(self):
            """生成交易數據流"""

            base_stream = (
                self.spark.readStream.format("rate")
                .option("rowsPerSecond", 20)
                .option("numPartitions", 2)
                .load()
            )

            transaction_stream = (
                base_stream.select(col("timestamp"), col("value"))
                .withColumn("transaction_id", concat_ws("", lit("txn_"), col("value")))
                .withColumn(
                    "user_id", concat_ws("", lit("user_"), (col("value") % 10000) + 1)
                )
                .withColumn("order_id", concat_ws("", lit("order_"), col("value") // 3))
                .withColumn(
                    "product_id",
                    concat_ws("", lit("product_"), (col("value") % 5000) + 1),
                )
                .withColumn("quantity", (col("value") % 5) + 1)
                .withColumn("unit_price", (col("value") % 1000 + 10).cast("double"))
                .withColumn("total_amount", col("quantity") * col("unit_price"))
                .withColumn(
                    "discount_amount",
                    when(col("value") % 10 == 0, col("total_amount") * 0.1)
                    .when(col("value") % 20 == 0, col("total_amount") * 0.2)
                    .otherwise(0.0),
                )
                .withColumn(
                    "payment_method",
                    when(col("value") % 4 == 0, "credit_card")
                    .when(col("value") % 4 == 1, "debit_card")
                    .when(col("value") % 4 == 2, "paypal")
                    .otherwise("bank_transfer"),
                )
                .withColumn(
                    "status",
                    when(col("value") % 20 == 0, "cancelled")
                    .when(col("value") % 15 == 0, "pending")
                    .when(col("value") % 10 == 0, "shipped")
                    .when(col("value") % 5 == 0, "delivered")
                    .otherwise("paid"),
                )
                .withColumn(
                    "shipping_address",
                    concat_ws(" ", lit("Address"), (col("value") % 1000)),
                )
                .withColumn(
                    "promotion_code",
                    when(
                        col("value") % 50 == 0,
                        concat_ws("", lit("PROMO"), (col("value") % 100)),
                    ).otherwise(None),
                )
                .select([field.name for field in transaction_schema.fields])
            )

            return transaction_stream

    # 3. 用戶行為分析器
    print("\n3. 用戶行為實時分析:")

    class UserBehaviorAnalyzer:
        """用戶行為分析器"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def real_time_metrics(self, behavior_stream):
            """實時用戶行為指標"""

            metrics = (
                behavior_stream.withWatermark("timestamp", "10 minutes")
                .groupBy(
                    window(col("timestamp"), "5 minutes", "1 minute"),
                    col("event_type"),
                    col("device_type"),
                    col("location"),
                )
                .agg(
                    count("*").alias("event_count"),
                    expr("approx_count_distinct(user_id)").alias("unique_users"),
                    expr("approx_count_distinct(session_id)").alias("unique_sessions"),
                    expr("approx_count_distinct(product_id)").alias("unique_products"),
                    avg("price").alias("avg_price"),
                    spark_sum("quantity").alias("total_quantity"),
                )
                .withColumn("events_per_user", col("event_count") / col("unique_users"))
                .withColumn(
                    "conversion_rate",
                    when(
                        col("event_type") == "purchase",
                        col("event_count")
                        / lag("event_count").over(
                            Window.partitionBy(
                                "window", "device_type", "location"
                            ).orderBy("event_type")
                        ),
                    ).otherwise(0.0),
                )
            )

            return metrics

        def user_session_analysis(self, behavior_stream):
            """用戶會話深度分析"""

            session_analysis = (
                behavior_stream.withWatermark("timestamp", "30 minutes")
                .groupBy(
                    window(col("timestamp"), "15 minutes", "5 minutes"),
                    col("user_id"),
                    col("session_id"),
                )
                .agg(
                    count("*").alias("page_views"),
                    expr("approx_count_distinct(product_id)").alias("products_viewed"),
                    expr("approx_count_distinct(category_id)").alias(
                        "categories_viewed"
                    ),
                    collect_list("event_type").alias("event_sequence"),
                    spark_sum(
                        when(col("event_type") == "cart", col("quantity")).otherwise(0)
                    ).alias("cart_items"),
                    spark_sum(
                        when(
                            col("event_type") == "purchase",
                            col("price") * col("quantity"),
                        ).otherwise(0)
                    ).alias("purchase_value"),
                    max("timestamp").alias("last_activity"),
                    min("timestamp").alias("first_activity"),
                )
                .withColumn(
                    "session_duration_minutes",
                    (unix_timestamp("last_activity") - unix_timestamp("first_activity"))
                    / 60,
                )
                .withColumn(
                    "has_purchase", array_contains(col("event_sequence"), "purchase")
                )
                .withColumn(
                    "has_cart_abandonment",
                    array_contains(col("event_sequence"), "cart")
                    & ~array_contains(col("event_sequence"), "purchase"),
                )
                .withColumn(
                    "user_type",
                    when(col("has_purchase"), "Converter")
                    .when(col("has_cart_abandonment"), "Cart Abandoner")
                    .when(col("page_views") > 10, "Browser")
                    .otherwise("Casual Visitor"),
                )
            )

            return session_analysis

        def product_performance_analysis(self, behavior_stream):
            """產品性能分析"""

            product_metrics = (
                behavior_stream.withWatermark("timestamp", "15 minutes")
                .groupBy(
                    window(col("timestamp"), "10 minutes", "2 minutes"),
                    col("product_id"),
                    col("category_id"),
                    col("brand_id"),
                )
                .agg(
                    spark_sum(when(col("event_type") == "view", 1).otherwise(0)).alias(
                        "views"
                    ),
                    spark_sum(when(col("event_type") == "click", 1).otherwise(0)).alias(
                        "clicks"
                    ),
                    spark_sum(when(col("event_type") == "cart", 1).otherwise(0)).alias(
                        "cart_adds"
                    ),
                    spark_sum(
                        when(col("event_type") == "purchase", 1).otherwise(0)
                    ).alias("purchases"),
                    spark_sum(
                        when(
                            col("event_type") == "purchase", col("quantity")
                        ).otherwise(0)
                    ).alias("units_sold"),
                    spark_sum(
                        when(
                            col("event_type") == "purchase",
                            col("price") * col("quantity"),
                        ).otherwise(0)
                    ).alias("revenue"),
                    expr("approx_count_distinct(user_id)").alias("unique_viewers"),
                )
                .withColumn(
                    "click_through_rate",
                    when(
                        col("views") > 0, col("clicks") / col("views") * 100
                    ).otherwise(0),
                )
                .withColumn(
                    "cart_conversion_rate",
                    when(
                        col("clicks") > 0, col("cart_adds") / col("clicks") * 100
                    ).otherwise(0),
                )
                .withColumn(
                    "purchase_conversion_rate",
                    when(
                        col("cart_adds") > 0, col("purchases") / col("cart_adds") * 100
                    ).otherwise(0),
                )
                .withColumn(
                    "revenue_per_view",
                    when(col("views") > 0, col("revenue") / col("views")).otherwise(0),
                )
            )

            return product_metrics

    # 4. 銷售分析器
    print("\n4. 銷售數據實時分析:")

    class SalesAnalyzer:
        """銷售分析器"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def real_time_sales_metrics(self, transaction_stream):
            """實時銷售指標"""

            sales_metrics = (
                transaction_stream.filter(
                    col("status").isin("paid", "shipped", "delivered")
                )
                .withColumn("net_amount", col("total_amount") - col("discount_amount"))
                .withWatermark("timestamp", "10 minutes")
                .groupBy(
                    window(col("timestamp"), "5 minutes", "1 minute"),
                    col("payment_method"),
                )
                .agg(
                    count("*").alias("transaction_count"),
                    spark_sum("net_amount").alias("total_revenue"),
                    spark_sum("quantity").alias("total_units"),
                    avg("net_amount").alias("avg_order_value"),
                    expr("approx_count_distinct(user_id)").alias("unique_customers"),
                    expr("approx_count_distinct(product_id)").alias("unique_products"),
                )
                .withColumn(
                    "revenue_per_customer",
                    col("total_revenue") / col("unique_customers"),
                )
                .withColumn(
                    "units_per_transaction",
                    col("total_units") / col("transaction_count"),
                )
            )

            return sales_metrics

        def fraud_detection(self, transaction_stream):
            """實時欺詐檢測"""

            # 計算用戶近期交易統計
            user_stats = (
                transaction_stream.withWatermark("timestamp", "1 hour")
                .groupBy(
                    window(col("timestamp"), "10 minutes", "1 minute"), col("user_id")
                )
                .agg(
                    count("*").alias("transaction_count"),
                    spark_sum("total_amount").alias("total_amount"),
                    avg("total_amount").alias("avg_amount"),
                    max("total_amount").alias("max_amount"),
                    expr("approx_count_distinct(payment_method)").alias(
                        "unique_payment_methods"
                    ),
                    expr("approx_count_distinct(shipping_address)").alias(
                        "unique_addresses"
                    ),
                )
            )

            # 欺詐風險評分
            fraud_scores = (
                user_stats.withColumn(
                    "velocity_risk",
                    when(col("transaction_count") > 20, 3)
                    .when(col("transaction_count") > 10, 2)
                    .when(col("transaction_count") > 5, 1)
                    .otherwise(0),
                )
                .withColumn(
                    "amount_risk",
                    when(col("max_amount") > 5000, 3)
                    .when(col("avg_amount") > 1000, 2)
                    .when(col("total_amount") > 10000, 2)
                    .otherwise(0),
                )
                .withColumn(
                    "behavior_risk",
                    when(col("unique_payment_methods") > 3, 2)
                    .when(col("unique_addresses") > 3, 2)
                    .otherwise(0),
                )
                .withColumn(
                    "fraud_score",
                    col("velocity_risk") + col("amount_risk") + col("behavior_risk"),
                )
                .withColumn(
                    "risk_level",
                    when(col("fraud_score") >= 6, "CRITICAL")
                    .when(col("fraud_score") >= 4, "HIGH")
                    .when(col("fraud_score") >= 2, "MEDIUM")
                    .otherwise("LOW"),
                )
                .filter(col("risk_level") != "LOW")
            )

            return fraud_scores

        def revenue_forecasting(self, sales_metrics):
            """收入預測分析"""

            # 計算趨勢和預測
            window_spec = Window.orderBy("window")

            forecast_analysis = (
                sales_metrics.withColumn(
                    "revenue_trend", lag("total_revenue", 1).over(window_spec)
                )
                .withColumn(
                    "revenue_growth_rate",
                    when(
                        col("revenue_trend") > 0,
                        (col("total_revenue") - col("revenue_trend"))
                        / col("revenue_trend")
                        * 100,
                    ).otherwise(0),
                )
                .withColumn(
                    "revenue_ma_5",
                    avg("total_revenue").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "predicted_next_period",
                    col("total_revenue") * (1 + col("revenue_growth_rate") / 100),
                )
            )

            return forecast_analysis

    # 5. 推薦系統引擎
    print("\n5. 實時推薦系統:")

    class RecommendationEngine:
        """實時推薦系統"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.als_model = None

        def prepare_training_data(self, behavior_stream):
            """準備推薦模型訓練數據"""

            # 從用戶行為中提取隱式評分
            implicit_ratings = (
                behavior_stream.filter(
                    col("event_type").isin("view", "click", "cart", "purchase")
                )
                .withColumn(
                    "rating_score",
                    when(col("event_type") == "purchase", 5.0)
                    .when(col("event_type") == "cart", 3.0)
                    .when(col("event_type") == "click", 2.0)
                    .otherwise(1.0),
                )
                .groupBy("user_id", "product_id")
                .agg(spark_sum("rating_score").alias("implicit_rating"))
                .withColumn(
                    "rating",
                    when(col("implicit_rating") > 10, 5.0)
                    .when(col("implicit_rating") > 5, 4.0)
                    .when(col("implicit_rating") > 2, 3.0)
                    .otherwise(2.0),
                )
            )

            # 數值編碼
            user_indexer = StringIndexer(inputCol="user_id", outputCol="user_idx")
            product_indexer = StringIndexer(
                inputCol="product_id", outputCol="product_idx"
            )

            indexed_ratings = user_indexer.fit(implicit_ratings).transform(
                implicit_ratings
            )
            indexed_ratings = product_indexer.fit(indexed_ratings).transform(
                indexed_ratings
            )

            return indexed_ratings.select("user_idx", "product_idx", "rating")

        def train_als_model(self, ratings_df):
            """訓練 ALS 協同過濾模型"""

            als = ALS(
                maxIter=10,
                regParam=0.1,
                userCol="user_idx",
                itemCol="product_idx",
                ratingCol="rating",
                coldStartStrategy="drop",
                implicitPrefs=True,
            )

            self.als_model = als.fit(ratings_df)
            return self.als_model

        def content_based_recommendations(self, behavior_stream):
            """基於內容的推薦"""

            # 分析用戶偏好
            user_preferences = (
                behavior_stream.filter(
                    col("event_type").isin("view", "purchase", "cart")
                )
                .withWatermark("timestamp", "2 hours")
                .groupBy("user_id")
                .agg(
                    collect_list("category_id").alias("preferred_categories"),
                    collect_list("brand_id").alias("preferred_brands"),
                    avg("price").alias("avg_price_range"),
                    count("*").alias("interaction_count"),
                )
                .withColumn(
                    "price_segment",
                    when(col("avg_price_range") > 500, "premium")
                    .when(col("avg_price_range") > 100, "mid_range")
                    .otherwise("budget"),
                )
            )

            return user_preferences

        def real_time_recommendations(self, behavior_stream, user_preferences):
            """實時推薦生成"""

            # 基於最近行為的推薦
            recent_activity = (
                behavior_stream.withWatermark("timestamp", "30 minutes")
                .filter(col("event_type").isin("view", "search"))
                .groupBy("user_id")
                .agg(
                    collect_list("category_id").alias("recent_categories"),
                    collect_list("product_id").alias("recent_products"),
                    max("timestamp").alias("last_activity"),
                )
            )

            # 結合用戶偏好生成推薦
            recommendations = (
                recent_activity.join(user_preferences, "user_id", "left")
                .withColumn(
                    "recommended_categories",
                    when(
                        col("preferred_categories").isNotNull(),
                        col("preferred_categories"),
                    ).otherwise(col("recent_categories")),
                )
                .withColumn(
                    "recommendation_type",
                    when(
                        col("preferred_categories").isNotNull(), "personalized"
                    ).otherwise("trending"),
                )
            )

            return recommendations

    # 6. 庫存管理系統
    print("\n6. 智能庫存管理:")

    class InventoryManager:
        """智能庫存管理系統"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def inventory_monitoring(self, transaction_stream):
            """實時庫存監控"""

            # 計算產品需求
            demand_analysis = (
                transaction_stream.filter(col("status").isin("paid", "shipped"))
                .withWatermark("timestamp", "30 minutes")
                .groupBy(
                    window(col("timestamp"), "15 minutes", "5 minutes"),
                    col("product_id"),
                )
                .agg(
                    spark_sum("quantity").alias("demand_quantity"),
                    count("*").alias("order_count"),
                    avg("quantity").alias("avg_order_size"),
                )
                .withColumn(
                    "demand_trend",
                    lag("demand_quantity").over(
                        Window.partitionBy("product_id").orderBy("window")
                    ),
                )
                .withColumn(
                    "demand_growth",
                    when(
                        col("demand_trend") > 0,
                        (col("demand_quantity") - col("demand_trend"))
                        / col("demand_trend")
                        * 100,
                    ).otherwise(0),
                )
            )

            return demand_analysis

        def reorder_alerts(self, demand_analysis):
            """補貨告警系統"""

            # 模擬當前庫存狀態
            current_inventory = (
                demand_analysis.withColumn(
                    "estimated_stock", lit(1000) - col("demand_quantity") * 2
                )
                .withColumn("reorder_point", lit(100))
                .withColumn("max_stock", lit(1000))
                .withColumn("lead_time_days", lit(7))
            )

            # 補貨建議
            reorder_recommendations = (
                current_inventory.withColumn(
                    "days_of_supply",
                    when(
                        col("demand_quantity") > 0,
                        col("estimated_stock") / col("demand_quantity") * 0.25,
                    ).otherwise(  # 15分鐘窗口轉天數
                        999
                    ),
                )
                .withColumn(
                    "reorder_quantity",
                    when(
                        col("estimated_stock") <= col("reorder_point"),
                        col("max_stock") - col("estimated_stock"),
                    ).otherwise(0),
                )
                .withColumn(
                    "urgency_level",
                    when(col("estimated_stock") <= 50, "CRITICAL")
                    .when(col("estimated_stock") <= col("reorder_point"), "HIGH")
                    .when(col("days_of_supply") <= 3, "MEDIUM")
                    .otherwise("LOW"),
                )
                .filter(col("urgency_level") != "LOW")
            )

            return reorder_recommendations

    # 7. 異常檢測系統
    print("\n7. 多維度異常檢測:")

    class AnomalyDetectionSystem:
        """異常檢測系統"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def system_anomaly_detection(self, behavior_stream, transaction_stream):
            """系統級異常檢測"""

            # 系統指標異常檢測
            system_metrics = (
                behavior_stream.withWatermark("timestamp", "5 minutes")
                .groupBy(window(col("timestamp"), "1 minute"))
                .agg(
                    count("*").alias("total_events"),
                    expr("approx_count_distinct(user_id)").alias("active_users"),
                    expr("approx_count_distinct(session_id)").alias("active_sessions"),
                    avg("price").alias("avg_price"),
                )
            )

            # 計算異常分數
            window_spec = Window.orderBy("window")

            anomaly_scores = (
                system_metrics.withColumn(
                    "events_ma",
                    avg("total_events").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "events_std",
                    stddev("total_events").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "events_zscore",
                    when(
                        col("events_std") > 0,
                        spark_abs(col("total_events") - col("events_ma"))
                        / col("events_std"),
                    ).otherwise(0),
                )
                .withColumn(
                    "users_ma", avg("active_users").over(window_spec.rowsBetween(-4, 0))
                )
                .withColumn(
                    "users_std",
                    stddev("active_users").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "users_zscore",
                    when(
                        col("users_std") > 0,
                        spark_abs(col("active_users") - col("users_ma"))
                        / col("users_std"),
                    ).otherwise(0),
                )
                .withColumn("anomaly_score", col("events_zscore") + col("users_zscore"))
                .withColumn(
                    "anomaly_type",
                    when(col("anomaly_score") > 4, "CRITICAL")
                    .when(col("anomaly_score") > 2, "WARNING")
                    .otherwise("NORMAL"),
                )
                .filter(col("anomaly_type") != "NORMAL")
            )

            return anomaly_scores

        def business_anomaly_detection(self, transaction_stream):
            """業務異常檢測"""

            # 業務指標異常
            business_metrics = (
                transaction_stream.withWatermark("timestamp", "10 minutes")
                .groupBy(window(col("timestamp"), "5 minutes"))
                .agg(
                    count("*").alias("transaction_count"),
                    spark_sum("total_amount").alias("total_revenue"),
                    avg("total_amount").alias("avg_order_value"),
                    spark_sum(when(col("status") == "cancelled", 1).otherwise(0)).alias(
                        "cancelled_orders"
                    ),
                    spark_sum(when(col("status") == "pending", 1).otherwise(0)).alias(
                        "pending_orders"
                    ),
                )
                .withColumn(
                    "cancellation_rate",
                    col("cancelled_orders") / col("transaction_count") * 100,
                )
                .withColumn(
                    "pending_rate",
                    col("pending_orders") / col("transaction_count") * 100,
                )
            )

            # 檢測業務異常
            business_anomalies = (
                business_metrics.withColumn(
                    "revenue_anomaly",
                    when(
                        col("total_revenue")
                        < lag("total_revenue").over(Window.orderBy("window")) * 0.5,
                        "LOW_REVENUE",
                    )
                    .when(
                        col("total_revenue")
                        > lag("total_revenue").over(Window.orderBy("window")) * 2,
                        "HIGH_REVENUE",
                    )
                    .otherwise("NORMAL"),
                )
                .withColumn(
                    "cancellation_anomaly",
                    when(col("cancellation_rate") > 20, "HIGH_CANCELLATION").otherwise(
                        "NORMAL"
                    ),
                )
                .withColumn(
                    "alert_level",
                    when(
                        (col("revenue_anomaly") != "NORMAL")
                        | (col("cancellation_anomaly") != "NORMAL"),
                        "ALERT",
                    ).otherwise("OK"),
                )
                .filter(col("alert_level") == "ALERT")
            )

            return business_anomalies

    # 8. 數據管道協調器
    print("\n8. 數據管道協調和執行:")

    class EcommercePipelineOrchestrator:
        """電商數據管道協調器"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.data_generator = EcommerceDataGenerator(spark_session)
            self.behavior_analyzer = UserBehaviorAnalyzer(spark_session)
            self.sales_analyzer = SalesAnalyzer(spark_session)
            self.recommendation_engine = RecommendationEngine(spark_session)
            self.inventory_manager = InventoryManager(spark_session)
            self.anomaly_detector = AnomalyDetectionSystem(spark_session)

        def start_pipeline(self):
            """啟動完整的數據管道"""

            print("\n啟動電商實時分析管道...")

            # 創建數據流
            behavior_stream = self.data_generator.generate_user_behavior_stream()
            transaction_stream = self.data_generator.generate_transaction_stream()

            print("✓ 數據流創建完成")

            # 用戶行為分析
            behavior_metrics = self.behavior_analyzer.real_time_metrics(behavior_stream)
            session_analysis = self.behavior_analyzer.user_session_analysis(
                behavior_stream
            )
            product_performance = self.behavior_analyzer.product_performance_analysis(
                behavior_stream
            )

            # 銷售分析
            sales_metrics = self.sales_analyzer.real_time_sales_metrics(
                transaction_stream
            )
            fraud_detection = self.sales_analyzer.fraud_detection(transaction_stream)
            revenue_forecast = self.sales_analyzer.revenue_forecasting(sales_metrics)

            # 推薦系統
            user_preferences = self.recommendation_engine.content_based_recommendations(
                behavior_stream
            )
            recommendations = self.recommendation_engine.real_time_recommendations(
                behavior_stream, user_preferences
            )

            # 庫存管理
            demand_analysis = self.inventory_manager.inventory_monitoring(
                transaction_stream
            )
            reorder_alerts = self.inventory_manager.reorder_alerts(demand_analysis)

            # 異常檢測
            system_anomalies = self.anomaly_detector.system_anomaly_detection(
                behavior_stream, transaction_stream
            )
            business_anomalies = self.anomaly_detector.business_anomaly_detection(
                transaction_stream
            )

            # 啟動所有查詢
            queries = []

            # 用戶行為監控
            behavior_query = (
                behavior_metrics.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 10)
                .trigger(processingTime="30 seconds")
                .queryName("behavior_metrics")
                .start()
            )
            queries.append(behavior_query)

            # 會話分析
            session_query = (
                session_analysis.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 5)
                .trigger(processingTime="60 seconds")
                .queryName("session_analysis")
                .start()
            )
            queries.append(session_query)

            # 銷售監控
            sales_query = (
                sales_metrics.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 10)
                .trigger(processingTime="30 seconds")
                .queryName("sales_metrics")
                .start()
            )

            queries.append(sales_query)

            # 欺詐檢測告警
            fraud_query = (
                fraud_detection.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .trigger(processingTime="60 seconds")
                .queryName("fraud_detection")
                .start()
            )
            queries.append(fraud_query)

            # 庫存告警
            inventory_query = (
                reorder_alerts.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .trigger(processingTime="120 seconds")
                .queryName("inventory_alerts")
                .start()
            )
            queries.append(inventory_query)

            # 異常檢測告警
            anomaly_query = (
                system_anomalies.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .trigger(processingTime="60 seconds")
                .queryName("anomaly_detection")
                .start()
            )
            queries.append(anomaly_query)

            print(f"✓ 已啟動 {len(queries)} 個實時分析查詢")

            return queries

        def monitor_pipeline_health(self, queries):
            """監控管道健康狀態"""

            print("\n監控管道運行狀態:")

            for query in queries:
                try:
                    progress = query.lastProgress
                    if progress:
                        print(f"\n查詢: {query.name}")
                        print(f"  狀態: {'運行中' if query.isActive else '已停止'}")
                        print(f"  批次ID: {progress.get('batchId', 'N/A')}")
                        print(
                            f"  處理時間: {progress.get('durationMs', {}).get('totalMs', 'N/A')} ms"
                        )
                        print(
                            f"  輸入速率: {progress.get('inputRowsPerSecond', 'N/A')} rows/sec"
                        )
                        print(
                            f"  處理速率: {progress.get('processedRowsPerSecond', 'N/A')} rows/sec"
                        )
                except Exception as e:
                    print(f"查詢 {query.name} 監控失敗: {e}")

    # 9. 執行電商分析管道
    print("\n9. 執行完整電商分析管道:")

    orchestrator = EcommercePipelineOrchestrator(spark)

    # 啟動管道
    running_queries = orchestrator.start_pipeline()

    # 運行一段時間
    print("\n等待數據處理...")
    time.sleep(60)  # 運行1分鐘

    # 監控狀態
    orchestrator.monitor_pipeline_health(running_queries)

    # 再運行一段時間
    time.sleep(60)  # 再運行1分鐘

    # 10. 性能總結和優化建議
    print("\n10. 系統性能總結:")

    def generate_performance_summary():
        """生成性能總結報告"""

        performance_metrics = {
            "管道數量": len(running_queries),
            "數據源": "實時模擬數據流",
            "處理延遲": "秒級響應",
            "分析維度": "用戶行為、銷售、庫存、異常檢測",
            "推薦系統": "基於內容和協同過濾",
            "監控覆蓋": "業務指標、系統指標、安全監控",
        }

        optimization_recommendations = [
            "實施分層存儲策略提高數據訪問性能",
            "使用機器學習模型優化推薦算法",
            "建立自動化告警和響應機制",
            "實施A/B測試框架驗證業務策略",
            "增加數據血緣和質量監控",
            "建立實時模型訓練和部署管道",
            "實施多區域部署提高可用性",
        ]

        print("電商平台性能指標:")
        for metric, value in performance_metrics.items():
            print(f"- {metric}: {value}")

        print("\n系統優化建議:")
        for i, recommendation in enumerate(optimization_recommendations, 1):
            print(f"{i}. {recommendation}")

        return performance_metrics, optimization_recommendations

    metrics, recommendations = generate_performance_summary()

    # 停止所有查詢
    print("\n停止所有數據流...")
    for query in running_queries:
        try:
            query.stop()
            print(f"✓ 已停止查詢: {query.name}")
        except Exception as e:
            print(f"✗ 停止查詢 {query.name} 失敗: {e}")

    # 清理資源
    spark.stop()
    print("\n大型電商實時分析平台練習完成！")


if __name__ == "__main__":
    main()
