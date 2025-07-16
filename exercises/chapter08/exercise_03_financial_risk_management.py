#!/usr/bin/env python3
"""
第8章練習3：金融風險管理系統
建立金融風險實時監控系統，檢測異常交易、欺詐行為、市場風險等
"""

import json
import math
import random
import time
from datetime import datetime, timedelta

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import (DecisionTreeClassifier, GBTClassifier,
                                       LogisticRegression,
                                       RandomForestClassifier)
from pyspark.ml.clustering import GaussianMixture, KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator,
                                   RegressionEvaluator)
from pyspark.ml.feature import (PCA, Bucketizer, PolynomialExpansion,
                                QuantileDiscretizer, StandardScaler,
                                StringIndexer, VectorAssembler)
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import abs as spark_abs
from pyspark.sql.functions import (array, array_contains, asc, avg, broadcast,
                                   col, collect_list, concat_ws, count,
                                   current_timestamp, date_format, dayofweek,
                                   dense_rank, desc, explode, expr, first,
                                   from_json, hour, kurtosis, lag, last, lead,
                                   lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import percentile_approx
from pyspark.sql.functions import pow as spark_pow
from pyspark.sql.functions import rand, regexp_extract
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import (row_number, skewness, split, sqrt, stddev,
                                   struct)
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import (to_json, unix_timestamp, var_pop, when,
                                   window)
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql.types import (ArrayType, BooleanType, DoubleType, IntegerType,
                               MapType, StringType, StructField, StructType,
                               TimestampType)
from pyspark.sql.window import Window


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("金融風險管理系統")
        .master("local[*]")
        .config(
            "spark.sql.streaming.checkpointLocation", "/tmp/risk_management_checkpoint"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=== 第8章練習3：金融風險管理系統 ===")

    # 1. 數據模型定義
    print("\n1. 定義金融數據模型:")

    # 1.1 交易數據模型
    transaction_schema = StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("account_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField(
                "transaction_type", StringType(), True
            ),  # transfer, withdrawal, deposit, payment
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("counterparty_account", StringType(), True),
            StructField("counterparty_bank", StringType(), True),
            StructField("country_code", StringType(), True),
            StructField("channel", StringType(), True),  # atm, online, mobile, branch
            StructField("timestamp", TimestampType(), True),
            StructField("description", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("is_international", BooleanType(), True),
            StructField("risk_score", DoubleType(), True),
        ]
    )

    # 1.2 市場數據模型
    market_data_schema = StructType(
        [
            StructField("symbol", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("volume", IntegerType(), True),
            StructField("bid_price", DoubleType(), True),
            StructField("ask_price", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("exchange", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("market_cap", DoubleType(), True),
            StructField("volatility", DoubleType(), True),
        ]
    )

    # 1.3 客戶檔案模型
    customer_profile_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("income_level", StringType(), True),
            StructField("occupation", StringType(), True),
            StructField("country", StringType(), True),
            StructField("account_type", StringType(), True),
            StructField("credit_score", IntegerType(), True),
            StructField("risk_tolerance", StringType(), True),
            StructField("kyc_status", StringType(), True),
            StructField("account_balance", DoubleType(), True),
            StructField("last_updated", TimestampType(), True),
        ]
    )

    print("金融數據模型定義完成")

    # 2. 金融數據生成器
    print("\n2. 金融數據生成器:")

    class FinancialDataGenerator:
        """金融數據生成器"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.customers = [f"customer_{i}" for i in range(1, 10001)]
            self.accounts = [f"account_{i}" for i in range(1, 15001)]
            self.symbols = [
                "AAPL",
                "GOOGL",
                "MSFT",
                "AMZN",
                "TSLA",
                "META",
                "NVDA",
                "NFLX",
                "JPM",
                "BAC",
            ]

        def generate_transaction_stream(self):
            """生成交易數據流"""

            base_stream = (
                self.spark.readStream.format("rate")
                .option("rowsPerSecond", 100)
                .option("numPartitions", 4)
                .load()
            )

            transaction_stream = (
                base_stream.select(col("timestamp"), col("value"))
                .withColumn("transaction_id", concat_ws("", lit("txn_"), col("value")))
                .withColumn(
                    "account_id",
                    concat_ws("", lit("account_"), (col("value") % 15000) + 1),
                )
                .withColumn(
                    "customer_id",
                    concat_ws("", lit("customer_"), (col("value") % 10000) + 1),
                )
                .withColumn(
                    "transaction_type",
                    when(col("value") % 10 == 0, "transfer")
                    .when(col("value") % 10 == 1, "withdrawal")
                    .when(col("value") % 10 == 2, "deposit")
                    .when(col("value") % 10 == 3, "payment")
                    .otherwise("transfer"),
                )
                .withColumn(
                    "amount",
                    when(
                        col("transaction_type") == "transfer",
                        when(
                            col("value") % 1000 == 0, (col("value") % 100000) + 50000
                        ).otherwise(  # 大額轉帳
                            (col("value") % 10000) + 100
                        ),
                    )
                    .when(
                        col("transaction_type") == "withdrawal",
                        (col("value") % 5000) + 100,
                    )
                    .when(
                        col("transaction_type") == "deposit",
                        (col("value") % 20000) + 500,
                    )
                    .otherwise((col("value") % 1000) + 50),
                )
                .withColumn(
                    "currency",
                    when(col("value") % 20 == 0, "EUR")
                    .when(col("value") % 30 == 0, "GBP")
                    .when(col("value") % 40 == 0, "JPY")
                    .when(col("value") % 50 == 0, "CNY")
                    .otherwise("USD"),
                )
                .withColumn(
                    "counterparty_account",
                    concat_ws("", lit("cp_account_"), (col("value") % 5000) + 1),
                )
                .withColumn(
                    "counterparty_bank",
                    when(col("value") % 5 == 0, "BANK_A")
                    .when(col("value") % 5 == 1, "BANK_B")
                    .when(col("value") % 5 == 2, "BANK_C")
                    .when(col("value") % 5 == 3, "BANK_D")
                    .otherwise("BANK_E"),
                )
                .withColumn(
                    "country_code",
                    when(col("value") % 10 == 0, "US")
                    .when(col("value") % 10 == 1, "UK")
                    .when(col("value") % 10 == 2, "CN")
                    .when(col("value") % 10 == 3, "JP")
                    .when(col("value") % 10 == 4, "DE")
                    .otherwise("FR"),
                )
                .withColumn(
                    "channel",
                    when(col("value") % 4 == 0, "online")
                    .when(col("value") % 4 == 1, "mobile")
                    .when(col("value") % 4 == 2, "atm")
                    .otherwise("branch"),
                )
                .withColumn(
                    "description",
                    concat_ws(" ", col("transaction_type"), lit("transaction")),
                )
                .withColumn(
                    "merchant_category",
                    when(
                        col("transaction_type") == "payment",
                        when(col("value") % 6 == 0, "grocery")
                        .when(col("value") % 6 == 1, "gas")
                        .when(col("value") % 6 == 2, "restaurant")
                        .when(col("value") % 6 == 3, "retail")
                        .when(col("value") % 6 == 4, "online")
                        .otherwise("other"),
                    ).otherwise("N/A"),
                )
                .withColumn("is_international", col("country_code") != "US")
                .withColumn(
                    "risk_score",
                    when(col("amount") > 50000, rand() * 0.3 + 0.7)  # 大額交易高風險
                    .when(
                        col("is_international"), rand() * 0.2 + 0.5
                    )  # 國際交易中等風險
                    .when(col("channel") == "atm", rand() * 0.1 + 0.2)  # ATM交易低風險
                    .otherwise(rand() * 0.4 + 0.1),  # 其他交易
                )
                .select([field.name for field in transaction_schema.fields])
            )

            return transaction_stream

        def generate_market_data_stream(self):
            """生成市場數據流"""

            base_stream = (
                self.spark.readStream.format("rate")
                .option("rowsPerSecond", 30)
                .option("numPartitions", 2)
                .load()
            )

            market_stream = (
                base_stream.select(col("timestamp"), col("value"))
                .withColumn("symbol_idx", col("value") % 10)
                .withColumn(
                    "symbol",
                    when(col("symbol_idx") == 0, "AAPL")
                    .when(col("symbol_idx") == 1, "GOOGL")
                    .when(col("symbol_idx") == 2, "MSFT")
                    .when(col("symbol_idx") == 3, "AMZN")
                    .when(col("symbol_idx") == 4, "TSLA")
                    .when(col("symbol_idx") == 5, "META")
                    .when(col("symbol_idx") == 6, "NVDA")
                    .when(col("symbol_idx") == 7, "NFLX")
                    .when(col("symbol_idx") == 8, "JPM")
                    .otherwise("BAC"),
                )
                .withColumn(
                    "base_price",
                    when(col("symbol") == "AAPL", 150.0)
                    .when(col("symbol") == "GOOGL", 2500.0)
                    .when(col("symbol") == "MSFT", 300.0)
                    .when(col("symbol") == "AMZN", 3000.0)
                    .when(col("symbol") == "TSLA", 800.0)
                    .when(col("symbol") == "META", 200.0)
                    .when(col("symbol") == "NVDA", 400.0)
                    .when(col("symbol") == "NFLX", 350.0)
                    .when(col("symbol") == "JPM", 120.0)
                    .otherwise(50.0),
                )
                .withColumn("price_change", (rand() - 0.5) * 0.1)  # ±5% 隨機變動
                .withColumn("price", col("base_price") * (1 + col("price_change")))
                .withColumn("volume", (col("value") % 1000000) + 100000)
                .withColumn("spread", col("base_price") * 0.001)  # 0.1% 點差
                .withColumn("bid_price", col("price") - col("spread") / 2)
                .withColumn("ask_price", col("price") + col("spread") / 2)
                .withColumn(
                    "exchange",
                    when(col("value") % 3 == 0, "NYSE")
                    .when(col("value") % 3 == 1, "NASDAQ")
                    .otherwise("AMEX"),
                )
                .withColumn(
                    "sector",
                    when(
                        col("symbol").isin(
                            "AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"
                        ),
                        "Technology",
                    )
                    .when(col("symbol") == "TSLA", "Automotive")
                    .otherwise("Financial"),
                )
                .withColumn("market_cap", col("price") * 1000000000)  # 簡化市值計算
                .withColumn("volatility", spark_abs(col("price_change")) * 10)  # 波動率
                .select([field.name for field in market_data_schema.fields])
            )

            return market_stream

    # 3. 欺詐檢測系統
    print("\n3. 智能欺詐檢測系統:")

    class FraudDetectionSystem:
        """欺詐檢測系統"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.fraud_models = {}

        def rule_based_fraud_detection(self, transaction_stream):
            """基於規則的欺詐檢測"""

            # 計算客戶行為統計
            customer_behavior = (
                transaction_stream.withWatermark("timestamp", "1 hour")
                .groupBy(
                    window(col("timestamp"), "10 minutes", "1 minute"),
                    col("customer_id"),
                )
                .agg(
                    count("*").alias("transaction_count"),
                    spark_sum("amount").alias("total_amount"),
                    avg("amount").alias("avg_amount"),
                    max("amount").alias("max_amount"),
                    expr("approx_count_distinct(country_code)").alias(
                        "unique_countries"
                    ),
                    expr("approx_count_distinct(channel)").alias("unique_channels"),
                    expr("approx_count_distinct(merchant_category)").alias(
                        "unique_merchants"
                    ),
                    collect_list("transaction_type").alias("transaction_types"),
                )
            )

            # 欺詐規則評分
            fraud_rules = (
                customer_behavior.withColumn(
                    "velocity_score",
                    when(col("transaction_count") > 50, 4)
                    .when(col("transaction_count") > 20, 3)
                    .when(col("transaction_count") > 10, 2)
                    .when(col("transaction_count") > 5, 1)
                    .otherwise(0),
                )
                .withColumn(
                    "amount_score",
                    when(col("max_amount") > 100000, 4)
                    .when(col("total_amount") > 500000, 4)
                    .when(col("max_amount") > 50000, 3)
                    .when(col("avg_amount") > 10000, 2)
                    .otherwise(0),
                )
                .withColumn(
                    "geographic_score",
                    when(col("unique_countries") > 5, 4)
                    .when(col("unique_countries") > 3, 3)
                    .when(col("unique_countries") > 2, 2)
                    .otherwise(0),
                )
                .withColumn(
                    "behavior_score",
                    when(col("unique_channels") > 3, 2)
                    .when(col("unique_merchants") > 10, 2)
                    .otherwise(0),
                )
                .withColumn(
                    "total_fraud_score",
                    col("velocity_score")
                    + col("amount_score")
                    + col("geographic_score")
                    + col("behavior_score"),
                )
                .withColumn(
                    "fraud_risk_level",
                    when(col("total_fraud_score") >= 10, "CRITICAL")
                    .when(col("total_fraud_score") >= 7, "HIGH")
                    .when(col("total_fraud_score") >= 4, "MEDIUM")
                    .otherwise("LOW"),
                )
                .filter(col("fraud_risk_level") != "LOW")
            )

            return fraud_rules

        def anomaly_based_fraud_detection(self, transaction_stream):
            """基於異常檢測的欺詐識別"""

            # 時間序列異常檢測
            time_series_anomalies = (
                transaction_stream.withWatermark("timestamp", "30 minutes")
                .groupBy(window(col("timestamp"), "5 minutes"), col("customer_id"))
                .agg(
                    count("*").alias("txn_count"),
                    avg("amount").alias("avg_amount"),
                    stddev("amount").alias("amount_stddev"),
                )
            )

            # 計算Z-Score異常
            window_spec = Window.partitionBy("customer_id").orderBy("window")

            anomaly_scores = (
                time_series_anomalies.withColumn(
                    "txn_count_ma",
                    avg("txn_count").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "txn_count_std",
                    stddev("txn_count").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "amount_ma", avg("avg_amount").over(window_spec.rowsBetween(-4, 0))
                )
                .withColumn(
                    "amount_std_ma",
                    avg("amount_stddev").over(window_spec.rowsBetween(-4, 0)),
                )
                .withColumn(
                    "txn_zscore",
                    when(
                        col("txn_count_std") > 0,
                        spark_abs(col("txn_count") - col("txn_count_ma"))
                        / col("txn_count_std"),
                    ).otherwise(0),
                )
                .withColumn(
                    "amount_zscore",
                    when(
                        col("amount_std_ma") > 0,
                        spark_abs(col("avg_amount") - col("amount_ma"))
                        / col("amount_std_ma"),
                    ).otherwise(0),
                )
                .withColumn(
                    "combined_anomaly_score", col("txn_zscore") + col("amount_zscore")
                )
                .withColumn("is_anomaly", col("combined_anomaly_score") > 3)
                .filter(col("is_anomaly"))
            )

            return anomaly_scores

        def network_analysis_fraud_detection(self, transaction_stream):
            """基於網絡分析的欺詐檢測"""

            # 構建交易網絡
            transaction_network = (
                transaction_stream.filter(col("transaction_type") == "transfer")
                .withWatermark("timestamp", "2 hours")
                .groupBy(
                    window(col("timestamp"), "30 minutes"),
                    col("account_id"),
                    col("counterparty_account"),
                )
                .agg(
                    count("*").alias("edge_weight"),
                    spark_sum("amount").alias("total_amount"),
                    avg("amount").alias("avg_amount"),
                )
            )

            # 計算節點中心性指標
            account_centrality = (
                transaction_network.groupBy("window", "account_id")
                .agg(
                    count("*").alias("degree_centrality"),
                    spark_sum("edge_weight").alias("weighted_degree"),
                    spark_sum("total_amount").alias("total_flow"),
                    expr("approx_count_distinct(counterparty_account)").alias(
                        "unique_counterparties"
                    ),
                )
                .withColumn(
                    "centrality_score",
                    col("degree_centrality") * 0.3
                    + col("weighted_degree") * 0.3
                    + col("unique_counterparties") * 0.4,
                )
                .withColumn(
                    "network_risk",
                    when(col("centrality_score") > 100, "HIGH")
                    .when(col("centrality_score") > 50, "MEDIUM")
                    .otherwise("LOW"),
                )
                .filter(col("network_risk") != "LOW")
            )

            return account_centrality

    # 4. 市場風險管理
    print("\n4. 市場風險管理系統:")

    class MarketRiskManager:
        """市場風險管理系統"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def calculate_var(self, market_stream, confidence_level=0.95):
            """計算風險價值 (Value at Risk)"""

            # 計算收益率
            price_returns = (
                market_stream.withWatermark("timestamp", "1 hour")
                .withColumn(
                    "prev_price",
                    lag("price").over(
                        Window.partitionBy("symbol").orderBy("timestamp")
                    ),
                )
                .withColumn(
                    "return",
                    when(
                        col("prev_price") > 0,
                        (col("price") - col("prev_price")) / col("prev_price"),
                    ).otherwise(0),
                )
                .filter(col("prev_price").isNotNull())
            )

            # 計算VaR
            var_calculation = (
                price_returns.withWatermark("timestamp", "2 hours")
                .groupBy(
                    window(col("timestamp"), "1 hour", "15 minutes"),
                    col("symbol"),
                    col("sector"),
                )
                .agg(
                    collect_list("return").alias("returns"),
                    count("return").alias("return_count"),
                    avg("return").alias("mean_return"),
                    stddev("return").alias("return_volatility"),
                )
                .withColumn(
                    "var_95", percentile_approx(col("returns"), 1 - confidence_level)
                )
                .withColumn("var_99", percentile_approx(col("returns"), 0.01))
                .withColumn(
                    "expected_shortfall",
                    # 簡化的ES計算
                    col("var_95") * 1.2,
                )
                .withColumn(
                    "risk_level",
                    when(col("var_95") < -0.05, "EXTREME")
                    .when(col("var_95") < -0.03, "HIGH")
                    .when(col("var_95") < -0.02, "MEDIUM")
                    .otherwise("LOW"),
                )
            )

            return var_calculation

        def volatility_monitoring(self, market_stream):
            """波動率監控"""

            volatility_metrics = (
                market_stream.withWatermark("timestamp", "30 minutes")
                .groupBy(
                    window(col("timestamp"), "15 minutes", "5 minutes"),
                    col("symbol"),
                    col("sector"),
                )
                .agg(
                    avg("price").alias("avg_price"),
                    stddev("price").alias("price_std"),
                    max("price").alias("max_price"),
                    min("price").alias("min_price"),
                    avg("volume").alias("avg_volume"),
                    avg("volatility").alias("avg_volatility"),
                )
                .withColumn(
                    "price_range",
                    (col("max_price") - col("min_price")) / col("avg_price"),
                )
                .withColumn("realized_volatility", col("price_std") / col("avg_price"))
                .withColumn(
                    "volatility_regime",
                    when(col("realized_volatility") > 0.1, "HIGH_VOLATILITY")
                    .when(col("realized_volatility") > 0.05, "MEDIUM_VOLATILITY")
                    .otherwise("LOW_VOLATILITY"),
                )
                .withColumn(
                    "volume_volatility_ratio",
                    col("avg_volume") / (col("realized_volatility") * 1000000),
                )
            )

            return volatility_metrics

        def correlation_analysis(self, market_stream):
            """相關性分析"""

            # 準備配對數據進行相關性計算
            price_pairs = (
                market_stream.withWatermark("timestamp", "1 hour")
                .select("timestamp", "symbol", "price")
                .withColumn(
                    "window_time",
                    (unix_timestamp("timestamp") / 300).cast("long") * 300,
                )
                .groupBy("window_time", "symbol")
                .agg(avg("price").alias("avg_price"))
            )

            # 自連接計算相關性
            correlation_pairs = (
                price_pairs.alias("p1")
                .join(
                    price_pairs.alias("p2"),
                    col("p1.window_time") == col("p2.window_time"),
                )
                .filter(col("p1.symbol") < col("p2.symbol"))
                .select(
                    col("p1.window_time").alias("timestamp"),
                    col("p1.symbol").alias("symbol1"),
                    col("p2.symbol").alias("symbol2"),
                    col("p1.avg_price").alias("price1"),
                    col("p2.avg_price").alias("price2"),
                )
            )

            # 滾動相關性計算（簡化版本）
            rolling_correlation = (
                correlation_pairs.withColumn(
                    "price1_normalized",
                    col("price1")
                    / avg("price1").over(
                        Window.partitionBy("symbol1", "symbol2")
                        .orderBy("timestamp")
                        .rowsBetween(-9, 0)
                    ),
                )
                .withColumn(
                    "price2_normalized",
                    col("price2")
                    / avg("price2").over(
                        Window.partitionBy("symbol1", "symbol2")
                        .orderBy("timestamp")
                        .rowsBetween(-9, 0)
                    ),
                )
                .withColumn(
                    "correlation_proxy",
                    col("price1_normalized") * col("price2_normalized"),
                )
            )

            return rolling_correlation

    # 5. 信用風險評估
    print("\n5. 信用風險評估系統:")

    class CreditRiskAssessment:
        """信用風險評估系統"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def credit_scoring(self, transaction_stream):
            """動態信用評分"""

            credit_metrics = (
                transaction_stream.withWatermark("timestamp", "24 hours")
                .groupBy(
                    window(col("timestamp"), "6 hours", "1 hour"), col("customer_id")
                )
                .agg(
                    count("*").alias("transaction_frequency"),
                    spark_sum("amount").alias("total_transaction_amount"),
                    avg("amount").alias("avg_transaction_amount"),
                    stddev("amount").alias("amount_volatility"),
                    spark_sum(
                        when(
                            col("transaction_type") == "deposit", col("amount")
                        ).otherwise(0)
                    ).alias("total_deposits"),
                    spark_sum(
                        when(
                            col("transaction_type") == "withdrawal", col("amount")
                        ).otherwise(0)
                    ).alias("total_withdrawals"),
                    expr("approx_count_distinct(counterparty_account)").alias(
                        "counterparty_diversity"
                    ),
                    avg("risk_score").alias("avg_risk_score"),
                )
                .withColumn(
                    "balance_trend", col("total_deposits") - col("total_withdrawals")
                )
                .withColumn(
                    "transaction_stability",
                    when(
                        col("amount_volatility") > 0,
                        col("avg_transaction_amount") / col("amount_volatility"),
                    ).otherwise(col("avg_transaction_amount")),
                )
                .withColumn(
                    "credit_score_base",
                    when(col("balance_trend") > 0, 20).otherwise(0)
                    + when(col("transaction_frequency") > 10, 15).otherwise(
                        col("transaction_frequency")
                    )
                    + when(col("transaction_stability") > 1000, 25)
                    .when(col("transaction_stability") > 500, 20)
                    .when(col("transaction_stability") > 100, 15)
                    .otherwise(10)
                    + when(col("counterparty_diversity") > 10, 20)
                    .when(col("counterparty_diversity") > 5, 15)
                    .otherwise(col("counterparty_diversity") * 2),
                )
                .withColumn("risk_adjustment", col("avg_risk_score") * (-20))
                .withColumn(
                    "credit_score",
                    spark_max(
                        lit(300),
                        spark_min(
                            lit(850),
                            col("credit_score_base") + col("risk_adjustment") + 500,
                        ),
                    ),
                )
                .withColumn(
                    "credit_grade",
                    when(col("credit_score") >= 750, "EXCELLENT")
                    .when(col("credit_score") >= 700, "GOOD")
                    .when(col("credit_score") >= 650, "FAIR")
                    .when(col("credit_score") >= 600, "POOR")
                    .otherwise("VERY_POOR"),
                )
            )

            return credit_metrics

        def default_probability_estimation(self, credit_metrics):
            """違約概率估計"""

            default_prob = (
                credit_metrics.withColumn(
                    "pd_base",
                    when(col("credit_grade") == "EXCELLENT", 0.01)
                    .when(col("credit_grade") == "GOOD", 0.03)
                    .when(col("credit_grade") == "FAIR", 0.07)
                    .when(col("credit_grade") == "POOR", 0.15)
                    .otherwise(0.25),
                )
                .withColumn("macroeconomic_factor", lit(1.2))
                .withColumn(
                    "behavior_adjustment",
                    when(col("balance_trend") < -10000, 1.5)
                    .when(col("balance_trend") < -5000, 1.2)
                    .when(col("balance_trend") > 10000, 0.8)
                    .otherwise(1.0),
                )
                .withColumn(
                    "probability_of_default",
                    col("pd_base")
                    * col("macroeconomic_factor")
                    * col("behavior_adjustment"),
                )
                .withColumn(
                    "risk_category",
                    when(col("probability_of_default") > 0.2, "HIGH_RISK")
                    .when(col("probability_of_default") > 0.1, "MEDIUM_RISK")
                    .when(col("probability_of_default") > 0.05, "LOW_RISK")
                    .otherwise("MINIMAL_RISK"),
                )
            )

            return default_prob

        def exposure_at_default(self, transaction_stream, credit_metrics):
            """違約時風險暴露計算"""

            # 計算客戶的信貸額度使用情況
            exposure_metrics = (
                transaction_stream.withWatermark("timestamp", "12 hours")
                .groupBy(window(col("timestamp"), "4 hours"), col("customer_id"))
                .agg(
                    max("amount").alias("max_single_transaction"),
                    spark_sum("amount").alias("total_exposure"),
                    count("*").alias("transaction_count"),
                )
                .join(
                    credit_metrics.select(
                        "window",
                        "customer_id",
                        "credit_score",
                        "probability_of_default",
                    ),
                    ["window", "customer_id"],
                )
                .withColumn(
                    "credit_limit_estimate",
                    when(col("credit_score") >= 750, 100000)
                    .when(col("credit_score") >= 700, 50000)
                    .when(col("credit_score") >= 650, 25000)
                    .when(col("credit_score") >= 600, 10000)
                    .otherwise(5000),
                )
                .withColumn(
                    "utilization_rate",
                    col("total_exposure") / col("credit_limit_estimate"),
                )
                .withColumn(
                    "exposure_at_default",
                    col("total_exposure") * (1 + col("utilization_rate") * 0.5),
                )
                .withColumn(
                    "expected_loss",
                    col("probability_of_default")
                    * col("exposure_at_default")
                    * 0.6,  # 假設回收率40%
                )
            )

            return exposure_metrics

    # 6. 法規合規監控
    print("\n6. 法規合規監控系統:")

    class ComplianceMonitor:
        """法規合規監控"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def aml_monitoring(self, transaction_stream):
            """反洗錢監控"""

            # 可疑交易模式檢測
            suspicious_patterns = (
                transaction_stream.withWatermark("timestamp", "24 hours")
                .groupBy(
                    window(col("timestamp"), "12 hours", "1 hour"), col("customer_id")
                )
                .agg(
                    count("*").alias("transaction_count"),
                    spark_sum("amount").alias("total_amount"),
                    expr("approx_count_distinct(country_code)").alias("countries"),
                    expr("approx_count_distinct(currency)").alias("currencies"),
                    spark_sum(when(col("amount") > 10000, 1).otherwise(0)).alias(
                        "large_transactions"
                    ),
                    spark_sum(when(col("is_international"), 1).otherwise(0)).alias(
                        "international_transactions"
                    ),
                )
                .withColumn(
                    "structuring_score",
                    when(
                        (col("transaction_count") > 20)
                        & (col("total_amount") > 100000)
                        & (col("large_transactions") == 0),
                        3,
                    ).otherwise(  # 拆分交易避免報告
                        0
                    ),
                )
                .withColumn(
                    "layering_score",
                    when(
                        (col("countries") > 3) & (col("currencies") > 2), 2
                    ).otherwise(  # 複雜轉移
                        0
                    ),
                )
                .withColumn(
                    "unusual_activity_score",
                    when(
                        col("international_transactions") / col("transaction_count")
                        > 0.8,
                        2,
                    )
                    .when(col("total_amount") > 1000000, 3)
                    .otherwise(0),
                )
                .withColumn(
                    "aml_risk_score",
                    col("structuring_score")
                    + col("layering_score")
                    + col("unusual_activity_score"),
                )
                .withColumn(
                    "aml_alert_level",
                    when(col("aml_risk_score") >= 5, "CRITICAL")
                    .when(col("aml_risk_score") >= 3, "HIGH")
                    .when(col("aml_risk_score") >= 1, "MEDIUM")
                    .otherwise("LOW"),
                )
                .filter(col("aml_alert_level") != "LOW")
            )

            return suspicious_patterns

        def large_transaction_reporting(self, transaction_stream):
            """大額交易報告"""

            large_transactions = (
                transaction_stream.filter(col("amount") >= 10000)
                .withColumn(
                    "reporting_requirement",
                    when(col("amount") >= 10000, "CTR")  # Currency Transaction Report
                    .when(
                        (col("amount") >= 3000) & col("is_international"), "IFT"
                    )  # International Fund Transfer
                    .otherwise("NONE"),
                )
                .withColumn(
                    "compliance_status",
                    when(
                        col("reporting_requirement") != "NONE", "REQUIRES_REPORTING"
                    ).otherwise("NO_ACTION"),
                )
                .filter(col("compliance_status") == "REQUIRES_REPORTING")
            )

            return large_transactions

        def sanctions_screening(self, transaction_stream):
            """制裁名單篩查"""

            # 模擬制裁名單
            sanctions_list = ["BLOCKED_BANK_1", "BLOCKED_BANK_2", "SANCTIONED_COUNTRY"]

            sanctions_alerts = (
                transaction_stream.withColumn(
                    "sanctions_hit",
                    when(col("counterparty_bank").isin(sanctions_list), "BANK_MATCH")
                    .when(
                        col("country_code").isin("IR", "KP", "SY"), "COUNTRY_SANCTIONS"
                    )
                    .otherwise("CLEAR"),
                )
                .withColumn(
                    "alert_severity",
                    when(col("sanctions_hit") == "BANK_MATCH", "CRITICAL")
                    .when(col("sanctions_hit") == "COUNTRY_SANCTIONS", "HIGH")
                    .otherwise("NONE"),
                )
                .filter(col("alert_severity") != "NONE")
            )

            return sanctions_alerts

    # 7. 壓力測試系統
    print("\n7. 壓力測試系統:")

    class StressTesting:
        """壓力測試系統"""

        def __init__(self, spark_session):
            self.spark = spark_session

        def market_stress_scenarios(self, market_stream):
            """市場壓力情境測試"""

            # 定義壓力情境
            stress_scenarios = [
                ("market_crash", -0.3),  # 市場崩盤30%
                ("moderate_decline", -0.15),  # 中度下跌15%
                ("volatility_spike", 0.5),  # 波動率飆升50%
                ("liquidity_crisis", -0.2),  # 流動性危機20%
            ]

            base_portfolio = (
                market_stream.withWatermark("timestamp", "1 hour")
                .groupBy(
                    window(col("timestamp"), "30 minutes"), col("symbol"), col("sector")
                )
                .agg(
                    avg("price").alias("current_price"),
                    avg("volume").alias("avg_volume"),
                    avg("volatility").alias("current_volatility"),
                    count("*").alias("data_points"),
                )
            )

            # 應用壓力情境
            stress_results = base_portfolio

            for scenario_name, shock_factor in stress_scenarios:
                stress_results = (
                    stress_results.withColumn(
                        f"{scenario_name}_price",
                        when(
                            scenario_name == "volatility_spike", col("current_price")
                        ).otherwise(  # 波動率情境不改變價格
                            col("current_price") * (1 + shock_factor)
                        ),
                    )
                    .withColumn(
                        f"{scenario_name}_volatility",
                        when(
                            scenario_name == "volatility_spike",
                            col("current_volatility") * (1 + shock_factor),
                        ).otherwise(col("current_volatility")),
                    )
                    .withColumn(f"{scenario_name}_impact", spark_abs(shock_factor))
                )

            return stress_results

        def portfolio_var_stress(self, stress_results):
            """投資組合VaR壓力測試"""

            portfolio_stress = (
                stress_results.groupBy("window")
                .agg(
                    avg("market_crash_impact").alias("avg_crash_impact"),
                    avg("moderate_decline_impact").alias("avg_decline_impact"),
                    avg("volatility_spike_impact").alias("avg_vol_impact"),
                    avg("liquidity_crisis_impact").alias("avg_liquidity_impact"),
                    count("*").alias("portfolio_size"),
                )
                .withColumn(
                    "worst_case_scenario",
                    spark_max(
                        spark_max("avg_crash_impact", "avg_decline_impact"),
                        spark_max("avg_vol_impact", "avg_liquidity_impact"),
                    ),
                )
                .withColumn(
                    "stress_test_rating",
                    when(col("worst_case_scenario") > 0.25, "FAIL")
                    .when(col("worst_case_scenario") > 0.15, "WARNING")
                    .otherwise("PASS"),
                )
            )

            return portfolio_stress

    # 8. 風險管理協調器
    print("\n8. 風險管理系統協調:")

    class RiskManagementOrchestrator:
        """風險管理系統協調器"""

        def __init__(self, spark_session):
            self.spark = spark_session
            self.data_generator = FinancialDataGenerator(spark_session)
            self.fraud_detector = FraudDetectionSystem(spark_session)
            self.market_risk_manager = MarketRiskManager(spark_session)
            self.credit_risk_assessor = CreditRiskAssessment(spark_session)
            self.compliance_monitor = ComplianceMonitor(spark_session)
            self.stress_tester = StressTesting(spark_session)

        def start_risk_management_pipeline(self):
            """啟動風險管理管道"""

            print("\n啟動金融風險管理系統...")

            # 創建數據流
            transaction_stream = self.data_generator.generate_transaction_stream()
            market_stream = self.data_generator.generate_market_data_stream()

            print("✓ 金融數據流創建完成")

            # 欺詐檢測
            rule_fraud = self.fraud_detector.rule_based_fraud_detection(
                transaction_stream
            )
            anomaly_fraud = self.fraud_detector.anomaly_based_fraud_detection(
                transaction_stream
            )
            network_fraud = self.fraud_detector.network_analysis_fraud_detection(
                transaction_stream
            )

            # 市場風險管理
            var_analysis = self.market_risk_manager.calculate_var(market_stream)
            volatility_monitoring = self.market_risk_manager.volatility_monitoring(
                market_stream
            )
            correlation_analysis = self.market_risk_manager.correlation_analysis(
                market_stream
            )

            # 信用風險評估
            credit_scoring = self.credit_risk_assessor.credit_scoring(
                transaction_stream
            )
            default_probability = (
                self.credit_risk_assessor.default_probability_estimation(credit_scoring)
            )
            exposure_analysis = self.credit_risk_assessor.exposure_at_default(
                transaction_stream, credit_scoring
            )

            # 合規監控
            aml_monitoring = self.compliance_monitor.aml_monitoring(transaction_stream)
            large_transaction_reports = (
                self.compliance_monitor.large_transaction_reporting(transaction_stream)
            )
            sanctions_screening = self.compliance_monitor.sanctions_screening(
                transaction_stream
            )

            # 壓力測試
            stress_scenarios = self.stress_tester.market_stress_scenarios(market_stream)
            portfolio_stress = self.stress_tester.portfolio_var_stress(stress_scenarios)

            # 啟動所有查詢
            queries = []

            # 欺詐檢測監控
            fraud_rule_query = (
                rule_fraud.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 5)
                .trigger(processingTime="60 seconds")
                .queryName("fraud_rules")
                .start()
            )
            queries.append(fraud_rule_query)

            # 市場風險監控
            var_query = (
                var_analysis.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 5)
                .trigger(processingTime="45 seconds")
                .queryName("var_analysis")
                .start()
            )
            queries.append(var_query)

            # 信用風險監控
            credit_query = (
                default_probability.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 5)
                .trigger(processingTime="90 seconds")
                .queryName("credit_risk")
                .start()
            )
            queries.append(credit_query)

            # 合規監控
            aml_query = (
                aml_monitoring.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 3)
                .trigger(processingTime="120 seconds")
                .queryName("aml_monitoring")
                .start()
            )
            queries.append(aml_query)

            # 大額交易報告
            large_txn_query = (
                large_transaction_reports.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .option("numRows", 5)
                .trigger(processingTime="30 seconds")
                .queryName("large_transactions")
                .start()
            )
            queries.append(large_txn_query)

            # 制裁篩查
            sanctions_query = (
                sanctions_screening.writeStream.outputMode("append")
                .format("console")
                .option("truncate", False)
                .trigger(processingTime="60 seconds")
                .queryName("sanctions_screening")
                .start()
            )
            queries.append(sanctions_query)

            print(f"✓ 已啟動 {len(queries)} 個風險管理查詢")

            return queries

        def generate_risk_dashboard(self, queries):
            """生成風險管理儀表板"""

            print("\n風險管理儀表板:")
            print("=" * 60)

            risk_metrics = {
                "欺詐檢測": "實時監控異常交易模式",
                "市場風險": "VaR計算和波動率監控",
                "信用風險": "動態信用評分和違約概率",
                "合規監控": "AML和制裁名單篩查",
                "壓力測試": "市場情境分析",
                "實時告警": f"{len(queries)} 個監控流程運行中",
            }

            for category, description in risk_metrics.items():
                print(f"📊 {category:<12}: {description}")

            print("=" * 60)

            return risk_metrics

    # 9. 執行風險管理系統
    print("\n9. 執行風險管理系統:")

    orchestrator = RiskManagementOrchestrator(spark)

    # 啟動風險管理管道
    running_queries = orchestrator.start_risk_management_pipeline()

    # 生成儀表板
    dashboard_metrics = orchestrator.generate_risk_dashboard(running_queries)

    # 運行一段時間
    print("\n等待風險數據處理...")
    time.sleep(90)  # 運行1.5分鐘

    # 監控查詢狀態
    print("\n查詢狀態監控:")
    for query in running_queries:
        try:
            progress = query.lastProgress
            if progress:
                print(
                    f"  {query.name}: 批次ID {progress.get('batchId', 'N/A')}, "
                    f"處理時間 {progress.get('durationMs', {}).get('totalMs', 'N/A')} ms"
                )
        except Exception as e:
            print(f"  {query.name}: 監控失敗 - {e}")

    # 再運行一段時間
    time.sleep(90)  # 再運行1.5分鐘

    # 10. 系統總結和建議
    print("\n10. 風險管理系統總結:")

    def generate_risk_management_summary():
        """生成風險管理系統總結"""

        system_capabilities = {
            "欺詐檢測": "規則基礎 + 異常檢測 + 網絡分析",
            "市場風險": "VaR計算 + 波動率監控 + 相關性分析",
            "信用風險": "動態評分 + 違約概率 + 風險暴露",
            "合規監控": "反洗錢 + 大額交易 + 制裁篩查",
            "壓力測試": "多情境分析 + 投資組合評估",
            "實時處理": "秒級響應 + 自動告警",
        }

        implementation_benefits = [
            "實時風險監控和預警",
            "多維度風險評估體系",
            "自動化合規報告生成",
            "智能化異常檢測",
            "壓力測試和情境分析",
            "可擴展的風險管理架構",
        ]

        future_enhancements = [
            "整合機器學習模型提高檢測準確性",
            "建立風險限額動態調整機制",
            "實施模型風險管理框架",
            "增加ESG風險評估維度",
            "建立跨機構風險數據共享",
            "實現監管報告自動化生成",
        ]

        print("系統功能覆蓋:")
        for capability, description in system_capabilities.items():
            print(f"- {capability}: {description}")

        print("\n實施效益:")
        for i, benefit in enumerate(implementation_benefits, 1):
            print(f"{i}. {benefit}")

        print("\n未來增強方向:")
        for i, enhancement in enumerate(future_enhancements, 1):
            print(f"{i}. {enhancement}")

        return system_capabilities, implementation_benefits, future_enhancements

    capabilities, benefits, enhancements = generate_risk_management_summary()

    # 停止所有查詢
    print("\n停止風險管理系統...")
    for query in running_queries:
        try:
            query.stop()
            print(f"✓ 已停止查詢: {query.name}")
        except Exception as e:
            print(f"✗ 停止查詢 {query.name} 失敗: {e}")

    # 清理資源
    spark.stop()
    print("\n金融風險管理系統練習完成！")


if __name__ == "__main__":
    main()
