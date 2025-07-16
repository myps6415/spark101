"""
第8章：實戰項目的測試
"""

import json
import os
import tempfile

import pytest
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql.functions import (asc, avg, col, count, current_timestamp,
                                   date_format, desc, explode, lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import regexp_extract, row_number, size, split
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, when, window
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)
from pyspark.sql.window import Window


class TestLogAnalyzer:
    """測試日誌分析器項目"""

    @pytest.fixture(scope="function")
    def sample_log_data(self, spark_session, temp_dir):
        """創建示例日誌數據"""
        log_entries = [
            '192.168.1.1 - - [10/Oct/2023:13:55:36 +0000] "GET /index.html HTTP/1.1" 200 2326',
            '192.168.1.2 - - [10/Oct/2023:13:55:37 +0000] "POST /login HTTP/1.1" 401 317',
            '192.168.1.1 - - [10/Oct/2023:13:55:38 +0000] "GET /home HTTP/1.1" 200 1024',
            '192.168.1.3 - - [10/Oct/2023:13:55:39 +0000] "GET /api/data HTTP/1.1" 500 256',
            '192.168.1.2 - - [10/Oct/2023:13:55:40 +0000] "POST /login HTTP/1.1" 200 512',
            '192.168.1.4 - - [10/Oct/2023:13:55:41 +0000] "GET /admin HTTP/1.1" 403 128',
            '192.168.1.1 - - [10/Oct/2023:13:55:42 +0000] "DELETE /file HTTP/1.1" 200 64',
            '192.168.1.5 - - [10/Oct/2023:13:55:43 +0000] "GET /index.html HTTP/1.1" 200 2326',
        ]

        log_file = os.path.join(temp_dir, "access.log")
        with open(log_file, "w") as f:
            for entry in log_entries:
                f.write(entry + "\n")

        return log_file

    def test_log_parsing(self, spark_session, sample_log_data):
        """測試日誌解析"""
        # 讀取日誌文件
        logs_df = spark_session.read.text(sample_log_data)

        # 解析日誌格式 (Apache Common Log Format)
        parsed_logs = logs_df.select(
            regexp_extract(col("value"), r"^(\S+)", 1).alias("ip"),
            regexp_extract(col("value"), r"\[([^\]]+)\]", 1).alias("timestamp"),
            regexp_extract(col("value"), r'"(\S+)', 1).alias("method"),
            regexp_extract(col("value"), r'"[^"]*" (\d+)', 1)
            .cast("int")
            .alias("status"),
            regexp_extract(col("value"), r'"[^"]*" \d+ (\d+)', 1)
            .cast("int")
            .alias("size"),
        ).filter(
            col("ip") != ""
        )  # 過濾解析失敗的行

        # 檢查解析結果
        assert parsed_logs.count() == 8

        # 檢查解析的字段
        sample_row = parsed_logs.first()
        assert sample_row.ip is not None
        assert sample_row.method is not None
        assert sample_row.status is not None

    def test_traffic_analysis(self, spark_session, sample_log_data):
        """測試流量分析"""
        # 讀取並解析日誌
        logs_df = spark_session.read.text(sample_log_data)

        parsed_logs = logs_df.select(
            regexp_extract(col("value"), r"^(\S+)", 1).alias("ip"),
            regexp_extract(col("value"), r'"(\S+)', 1).alias("method"),
            regexp_extract(col("value"), r'"[^"]*" (\d+)', 1)
            .cast("int")
            .alias("status"),
        ).filter(col("ip") != "")

        # IP訪問統計
        ip_stats = (
            parsed_logs.groupBy("ip")
            .agg(
                count("*").alias("request_count"),
                spark_sum(when(col("status") >= 400, 1).otherwise(0)).alias(
                    "error_count"
                ),
            )
            .orderBy(desc("request_count"))
        )

        # 檢查結果
        assert ip_stats.count() == 5  # 5個不同的IP

        top_ip = ip_stats.first()
        assert top_ip.request_count >= 1

        # 狀態碼統計
        status_stats = parsed_logs.groupBy("status").count().orderBy("status")
        status_codes = [row.status for row in status_stats.collect()]

        assert 200 in status_codes  # 成功請求
        assert (
            401 in status_codes or 403 in status_codes or 500 in status_codes
        )  # 錯誤請求

    def test_error_detection(self, spark_session, sample_log_data):
        """測試錯誤檢測"""
        # 讀取並解析日誌
        logs_df = spark_session.read.text(sample_log_data)

        parsed_logs = logs_df.select(
            regexp_extract(col("value"), r"^(\S+)", 1).alias("ip"),
            regexp_extract(col("value"), r'"[^"]*" (\d+)', 1)
            .cast("int")
            .alias("status"),
            regexp_extract(col("value"), r'"[A-Z]+ ([^"]*) HTTP', 1).alias("url"),
        ).filter(col("ip") != "")

        # 檢測錯誤（4xx, 5xx狀態碼）
        errors = parsed_logs.filter(col("status") >= 400)

        # 錯誤統計
        error_stats = errors.groupBy("status", "url").count().orderBy(desc("count"))

        # 檢查結果
        assert errors.count() >= 1  # 至少有一個錯誤

        # 檢查錯誤類型
        error_statuses = [row.status for row in errors.collect()]
        assert any(status >= 400 for status in error_statuses)

    def test_peak_hour_analysis(self, spark_session, temp_dir):
        """測試高峰時段分析"""
        # 創建包含時間戳的更詳細日誌
        log_entries_with_time = [
            '192.168.1.1 - - [10/Oct/2023:08:30:00 +0000] "GET /index.html HTTP/1.1" 200 2326',
            '192.168.1.2 - - [10/Oct/2023:09:15:00 +0000] "GET /home HTTP/1.1" 200 1024',
            '192.168.1.3 - - [10/Oct/2023:09:30:00 +0000] "POST /api HTTP/1.1" 200 512',
            '192.168.1.1 - - [10/Oct/2023:12:00:00 +0000] "GET /data HTTP/1.1" 200 256',
            '192.168.1.4 - - [10/Oct/2023:12:15:00 +0000] "GET /report HTTP/1.1" 200 1024',
            '192.168.1.2 - - [10/Oct/2023:14:30:00 +0000] "POST /submit HTTP/1.1" 200 128',
            '192.168.1.5 - - [10/Oct/2023:14:45:00 +0000] "GET /dashboard HTTP/1.1" 200 2048',
            '192.168.1.3 - - [10/Oct/2023:18:00:00 +0000] "GET /logout HTTP/1.1" 200 64',
        ]

        log_file = os.path.join(temp_dir, "time_logs.log")
        with open(log_file, "w") as f:
            for entry in log_entries_with_time:
                f.write(entry + "\n")

        # 讀取並解析日誌
        logs_df = spark_session.read.text(log_file)

        parsed_logs = logs_df.select(
            regexp_extract(col("value"), r"\[([^\]]+)\]", 1).alias("timestamp_str"),
            regexp_extract(col("value"), r"^(\S+)", 1).alias("ip"),
        ).filter(col("ip") != "")

        # 轉換時間戳並提取小時
        time_parsed = parsed_logs.withColumn(
            "hour", regexp_extract(col("timestamp_str"), r":(\d{2}):", 1).cast("int")
        )

        # 按小時統計請求量
        hourly_stats = time_parsed.groupBy("hour").count().orderBy("hour")

        # 檢查結果
        assert hourly_stats.count() >= 1

        # 找出最繁忙的小時
        peak_hour = hourly_stats.orderBy(desc("count")).first()
        assert peak_hour.count >= 1


class TestRealtimeETL:
    """測試實時ETL項目"""

    def test_data_validation(self, spark_session):
        """測試數據驗證"""
        # 創建測試數據（包含一些無效數據）
        raw_data = [
            (1, "Alice", 25, "alice@test.com", 50000.0),
            (2, "Bob", -5, "invalid-email", 60000.0),  # 無效年齡和郵件
            (3, "Charlie", 35, "charlie@test.com", -1000.0),  # 無效薪水
            (None, "Diana", 28, "diana@test.com", 55000.0),  # 缺失ID
            (5, "", 30, "eve@test.com", 65000.0),  # 空姓名
            (6, "Frank", 40, "frank@test.com", 70000.0),  # 正常數據
        ]

        df = spark_session.createDataFrame(
            raw_data, ["id", "name", "age", "email", "salary"]
        )

        # 數據驗證規則
        validated_df = df.filter(
            (col("id").isNotNull())
            & (col("name").isNotNull())
            & (col("name") != "")
            & (col("age") > 0)
            & (col("age") < 120)
            & (col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"))
            & (col("salary") > 0)
        )

        # 檢查驗證結果
        assert validated_df.count() == 2  # 只有Alice和Frank通過所有驗證

        valid_names = [row.name for row in validated_df.collect()]
        assert "Alice" in valid_names
        assert "Frank" in valid_names

    def test_data_transformation(self, spark_session):
        """測試數據轉換"""
        # 創建原始數據
        raw_data = [
            ("ALICE,SMITH", "25", "Engineering", "50000.50"),
            ("bob,johnson", "30", "Sales", "60000.75"),
            ("Charlie,Brown", "35", "Marketing", "55000.25"),
        ]

        df = spark_session.createDataFrame(
            raw_data, ["full_name", "age_str", "department", "salary_str"]
        )

        # 數據轉換
        transformed_df = df.select(
            split(col("full_name"), ",").getItem(0).alias("first_name"),
            split(col("full_name"), ",").getItem(1).alias("last_name"),
            col("age_str").cast("int").alias("age"),
            col("department"),
            col("salary_str").cast("double").alias("salary"),
        ).withColumn(
            "first_name",
            when(
                col("first_name").rlike("^[A-Z]+$"),
                regexp_replace(
                    col("first_name"),
                    "^(.)(.*)",
                    lambda m: m.group(1) + m.group(2).lower(),
                ),
            ).otherwise(
                regexp_replace(
                    col("first_name"),
                    "^(.)(.*)",
                    lambda m: m.group(1).upper() + m.group(2).lower(),
                )
            ),
        )

        # 簡化的名字格式化
        formatted_df = transformed_df.select(
            regexp_replace(
                col("first_name"), "^(.)", lambda m: m.group(1).upper()
            ).alias("first_name"),
            regexp_replace(
                col("last_name"), "^(.)", lambda m: m.group(1).upper()
            ).alias("last_name"),
            col("age"),
            col("department"),
            col("salary"),
        )

        # 檢查轉換結果
        assert formatted_df.count() == 3

        # 檢查數據類型
        assert dict(formatted_df.dtypes)["age"] == "int"
        assert dict(formatted_df.dtypes)["salary"] == "double"

    def test_incremental_processing(self, spark_session, temp_dir):
        """測試增量處理"""
        # 模擬第一批數據
        batch1_data = [
            (1, "Alice", "2023-10-01", 100.0),
            (2, "Bob", "2023-10-01", 150.0),
            (3, "Charlie", "2023-10-01", 200.0),
        ]

        batch1_df = spark_session.createDataFrame(
            batch1_data, ["user_id", "name", "date", "amount"]
        )

        # 寫入第一批數據
        batch1_path = os.path.join(temp_dir, "batch1")
        batch1_df.write.mode("overwrite").parquet(batch1_path)

        # 模擬第二批數據（增量）
        batch2_data = [
            (4, "Diana", "2023-10-02", 120.0),
            (1, "Alice", "2023-10-02", 110.0),  # Alice的新記錄
            (5, "Eve", "2023-10-02", 180.0),
        ]

        batch2_df = spark_session.createDataFrame(
            batch2_data, ["user_id", "name", "date", "amount"]
        )

        # 讀取歷史數據
        historical_df = spark_session.read.parquet(batch1_path)

        # 合併增量數據
        combined_df = historical_df.union(batch2_df)

        # 檢查合併結果
        assert combined_df.count() == 6  # 3 + 3

        # 檢查用戶統計
        user_stats = combined_df.groupBy("user_id", "name").agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
        )

        assert user_stats.count() == 5  # 5個不同用戶

        alice_stats = user_stats.filter(col("name") == "Alice").first()
        assert alice_stats.transaction_count == 2  # Alice有兩筆交易

    def test_data_quality_monitoring(self, spark_session):
        """測試數據質量監控"""
        # 創建測試數據
        data = [
            (1, "Alice", 25, "alice@test.com", 50000.0, "2023-10-01"),
            (2, None, 30, "bob@test.com", 60000.0, "2023-10-01"),  # 缺失姓名
            (3, "Charlie", None, "charlie@test.com", 55000.0, "2023-10-01"),  # 缺失年齡
            (4, "Diana", 28, None, 65000.0, "2023-10-01"),  # 缺失郵件
            (5, "Eve", 35, "eve@test.com", None, "2023-10-01"),  # 缺失薪水
            (6, "Frank", 40, "frank@test.com", 70000.0, None),  # 缺失日期
            (7, "Grace", 32, "grace@test.com", 58000.0, "2023-10-01"),  # 完整數據
        ]

        df = spark_session.createDataFrame(
            data, ["id", "name", "age", "email", "salary", "date"]
        )

        # 數據質量檢查
        total_records = df.count()

        quality_report = df.agg(
            count("*").alias("total_records"),
            count("name").alias("name_not_null"),
            count("age").alias("age_not_null"),
            count("email").alias("email_not_null"),
            count("salary").alias("salary_not_null"),
            count("date").alias("date_not_null"),
        ).collect()[0]

        # 計算完整性比率
        name_completeness = quality_report.name_not_null / quality_report.total_records
        age_completeness = quality_report.age_not_null / quality_report.total_records

        # 檢查質量指標
        assert quality_report.total_records == 7
        assert quality_report.name_not_null == 6  # 一個NULL
        assert quality_report.age_not_null == 6  # 一個NULL
        assert name_completeness > 0.8  # 80%以上完整性
        assert age_completeness > 0.8


class TestEcommerceAnalytics:
    """測試電商分析項目"""

    @pytest.fixture(scope="function")
    def ecommerce_data(self, spark_session):
        """創建電商測試數據"""
        # 用戶數據
        users_data = [
            (1, "Alice", "alice@test.com", "Premium", "2023-01-01"),
            (2, "Bob", "bob@test.com", "Standard", "2023-02-01"),
            (3, "Charlie", "charlie@test.com", "Premium", "2023-01-15"),
            (4, "Diana", "diana@test.com", "Standard", "2023-03-01"),
            (5, "Eve", "eve@test.com", "VIP", "2023-01-10"),
        ]
        users_df = spark_session.createDataFrame(
            users_data, ["user_id", "name", "email", "tier", "registration_date"]
        )

        # 產品數據
        products_data = [
            (101, "Laptop", "Electronics", 999.99),
            (102, "Phone", "Electronics", 699.99),
            (103, "Book", "Books", 29.99),
            (104, "Headphones", "Electronics", 199.99),
            (105, "Tablet", "Electronics", 499.99),
        ]
        products_df = spark_session.createDataFrame(
            products_data, ["product_id", "product_name", "category", "price"]
        )

        # 訂單數據
        orders_data = [
            (1001, 1, "2023-10-01", 1029.98),  # Alice
            (1002, 2, "2023-10-02", 699.99),  # Bob
            (1003, 1, "2023-10-03", 229.98),  # Alice again
            (1004, 3, "2023-10-04", 999.99),  # Charlie
            (1005, 4, "2023-10-05", 29.99),  # Diana
            (1006, 5, "2023-10-06", 1199.98),  # Eve
            (1007, 2, "2023-10-07", 199.99),  # Bob again
        ]
        orders_df = spark_session.createDataFrame(
            orders_data, ["order_id", "user_id", "order_date", "total_amount"]
        )

        return users_df, products_df, orders_df

    def test_customer_segmentation(self, ecommerce_data):
        """測試客戶分群"""
        users_df, products_df, orders_df = ecommerce_data

        # 計算客戶指標
        customer_metrics = orders_df.groupBy("user_id").agg(
            count("order_id").alias("order_count"),
            spark_sum("total_amount").alias("total_spent"),
            avg("total_amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date"),
        )

        # 加入用戶信息
        customer_analysis = customer_metrics.join(users_df, "user_id")

        # 客戶分群
        segmented_customers = customer_analysis.withColumn(
            "segment",
            when(col("total_spent") > 1000, "High Value")
            .when(col("order_count") > 1, "Repeat Customer")
            .otherwise("Regular"),
        )

        # 檢查分群結果
        segment_counts = segmented_customers.groupBy("segment").count().collect()
        segments = {row.segment: row.count for row in segment_counts}

        assert "High Value" in segments
        assert segments["High Value"] >= 1

    def test_product_recommendation(self, ecommerce_data):
        """測試產品推薦分析"""
        users_df, products_df, orders_df = ecommerce_data

        # 模擬訂單詳情（簡化版本）
        # 假設每個訂單包含一個主要產品
        order_details = [
            (1001, 101),
            (1001, 104),  # Alice: Laptop + Headphones
            (1002, 102),  # Bob: Phone
            (1003, 103),
            (1003, 104),  # Alice: Book + Headphones
            (1004, 101),  # Charlie: Laptop
            (1005, 103),  # Diana: Book
            (1006, 101),
            (1006, 105),  # Eve: Laptop + Tablet
            (1007, 104),  # Bob: Headphones
        ]

        order_details_df = spark_session.createDataFrame(
            order_details, ["order_id", "product_id"]
        )

        # 找出經常一起購買的產品
        # 連接訂單詳情以找到同一訂單中的產品組合
        product_pairs = (
            order_details_df.alias("od1")
            .join(
                order_details_df.alias("od2"),
                col("od1.order_id") == col("od2.order_id"),
            )
            .filter(col("od1.product_id") < col("od2.product_id"))  # 避免重複對
            .select(
                col("od1.product_id").alias("product_a"),
                col("od2.product_id").alias("product_b"),
            )
        )

        # 統計產品對的出現頻率
        product_associations = product_pairs.groupBy("product_a", "product_b").count()

        # 檢查結果
        assert product_associations.count() >= 1

        # 找出最常見的產品組合
        top_association = product_associations.orderBy(desc("count")).first()
        assert top_association.count >= 1

    def test_sales_performance(self, ecommerce_data):
        """測試銷售績效分析"""
        users_df, products_df, orders_df = ecommerce_data

        # 日銷售統計
        daily_sales = (
            orders_df.groupBy("order_date")
            .agg(
                count("order_id").alias("order_count"),
                spark_sum("total_amount").alias("daily_revenue"),
                avg("total_amount").alias("avg_order_value"),
            )
            .orderBy("order_date")
        )

        # 檢查日銷售數據
        assert daily_sales.count() == 7  # 7天的數據

        # 總體績效指標
        total_revenue = orders_df.agg(spark_sum("total_amount")).collect()[0][0]
        total_orders = orders_df.count()
        avg_order_value = total_revenue / total_orders

        assert total_revenue > 0
        assert total_orders == 7
        assert avg_order_value > 0

        # 用戶層級分析
        user_performance = (
            orders_df.join(users_df, "user_id")
            .groupBy("tier")
            .agg(
                count("order_id").alias("orders"),
                spark_sum("total_amount").alias("revenue"),
                avg("total_amount").alias("avg_order"),
            )
        )

        # 檢查不同層級用戶的表現
        assert user_performance.count() >= 3  # 至少3個用戶層級

    def test_cohort_analysis(self, ecommerce_data):
        """測試同期群分析"""
        users_df, products_df, orders_df = ecommerce_data

        # 為簡化測試，我們創建一個基本的同期群分析
        user_first_order = orders_df.groupBy("user_id").agg(
            min("order_date").alias("first_order_date")
        )

        # 連接用戶註冊信息
        cohort_data = user_first_order.join(users_df, "user_id")

        # 按註冊月份分群
        cohort_analysis = cohort_data.withColumn(
            "registration_month", date_format(col("registration_date"), "yyyy-MM")
        ).withColumn(
            "first_order_month", date_format(col("first_order_date"), "yyyy-MM")
        )

        # 計算轉換率（註冊到首次購買）
        conversion_stats = (
            cohort_analysis.groupBy("registration_month")
            .agg(
                count("*").alias("registered_users"),
                spark_sum(
                    when(col("first_order_date").isNotNull(), 1).otherwise(0)
                ).alias("converted_users"),
            )
            .withColumn(
                "conversion_rate", col("converted_users") / col("registered_users")
            )
        )

        # 檢查同期群分析結果
        assert conversion_stats.count() >= 1

        # 檢查轉換率計算
        sample_cohort = conversion_stats.first()
        assert sample_cohort.conversion_rate >= 0
        assert sample_cohort.conversion_rate <= 1


class TestFinancialRiskManagement:
    """測試金融風險管理項目"""

    @pytest.fixture(scope="function")
    def financial_data(self, spark_session):
        """創建金融測試數據"""
        # 交易數據
        transactions_data = [
            (1, "2023-10-01 09:30:00", 1001, 1500.0, "ATM", "Withdrawal"),
            (2, "2023-10-01 14:25:00", 1002, 50.0, "POS", "Purchase"),
            (3, "2023-10-01 16:45:00", 1001, 5000.0, "Online", "Transfer"),  # 可疑大額
            (4, "2023-10-01 20:15:00", 1003, 25.0, "POS", "Purchase"),
            (5, "2023-10-02 02:30:00", 1001, 200.0, "ATM", "Withdrawal"),  # 可疑時間
            (6, "2023-10-02 08:45:00", 1004, 100.0, "POS", "Purchase"),
            (7, "2023-10-02 10:00:00", 1002, 3000.0, "Online", "Transfer"),
            (8, "2023-10-02 11:30:00", 1001, 500.0, "POS", "Purchase"),
            (9, "2023-10-02 15:45:00", 1005, 75.0, "ATM", "Withdrawal"),
            (10, "2023-10-02 18:20:00", 1001, 1000.0, "Online", "Transfer"),
        ]

        transactions_df = spark_session.createDataFrame(
            transactions_data,
            ["transaction_id", "timestamp", "account_id", "amount", "channel", "type"],
        )

        # 轉換時間戳
        transactions_df = transactions_df.withColumn(
            "timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss")
        )

        # 賬戶數據
        accounts_data = [
            (1001, "Alice", "Premium", 50000.0, 5),
            (1002, "Bob", "Standard", 15000.0, 3),
            (1003, "Charlie", "Basic", 5000.0, 1),
            (1004, "Diana", "Standard", 25000.0, 4),
            (1005, "Eve", "Premium", 75000.0, 8),
        ]

        accounts_df = spark_session.createDataFrame(
            accounts_data,
            ["account_id", "name", "tier", "balance", "account_age_years"],
        )

        return transactions_df, accounts_df

    def test_fraud_detection(self, financial_data):
        """測試欺詐檢測"""
        transactions_df, accounts_df = financial_data

        # 欺詐檢測規則
        suspicious_transactions = transactions_df.withColumn(
            "hour", date_format(col("timestamp"), "HH").cast("int")
        ).withColumn(
            "is_suspicious",
            when(col("amount") > 3000, "Large Amount")  # 大額交易
            .when((col("hour") < 6) | (col("hour") > 22), "Unusual Time")  # 異常時間
            .when(col("channel") == "Online", "Online Risk")  # 線上風險
            .otherwise("Normal"),
        )

        # 統計可疑交易
        fraud_stats = suspicious_transactions.groupBy("is_suspicious").count()
        fraud_counts = {row.is_suspicious: row.count for row in fraud_stats.collect()}

        assert "Large Amount" in fraud_counts
        assert "Unusual Time" in fraud_counts
        assert fraud_counts["Large Amount"] >= 1

        # 按賬戶統計風險
        account_risk = (
            suspicious_transactions.groupBy("account_id")
            .agg(
                count("*").alias("total_transactions"),
                spark_sum(when(col("is_suspicious") != "Normal", 1).otherwise(0)).alias(
                    "suspicious_count"
                ),
            )
            .withColumn(
                "risk_ratio", col("suspicious_count") / col("total_transactions")
            )
        )

        # 檢查風險評估
        high_risk_accounts = account_risk.filter(col("risk_ratio") > 0.3)
        assert high_risk_accounts.count() >= 1

    def test_transaction_monitoring(self, financial_data):
        """測試交易監控"""
        transactions_df, accounts_df = financial_data

        # 加入賬戶信息
        enriched_transactions = transactions_df.join(accounts_df, "account_id")

        # 計算交易頻率
        window_spec = Window.partitionBy("account_id").orderBy("timestamp")

        transaction_patterns = enriched_transactions.withColumn(
            "prev_transaction_time", lag("timestamp").over(window_spec)
        ).withColumn(
            "time_diff_seconds",
            when(
                col("prev_transaction_time").isNotNull(),
                (
                    col("timestamp").cast("long")
                    - col("prev_transaction_time").cast("long")
                ),
            ).otherwise(None),
        )

        # 檢測短時間內多次交易
        rapid_transactions = transaction_patterns.filter(
            col("time_diff_seconds") < 3600  # 1小時內
        )

        # 檢查結果
        if rapid_transactions.count() > 0:
            assert rapid_transactions.count() >= 1

        # 每日交易限制檢查
        daily_limits = (
            enriched_transactions.withColumn(
                "date", date_format(col("timestamp"), "yyyy-MM-dd")
            )
            .groupBy("account_id", "date")
            .agg(
                count("*").alias("daily_transaction_count"),
                spark_sum("amount").alias("daily_total_amount"),
            )
        )

        # 檢查是否有超過限制的賬戶
        exceeded_limits = daily_limits.filter(
            (col("daily_transaction_count") > 10) | (col("daily_total_amount") > 10000)
        )

        # 檢查監控結果
        assert daily_limits.count() >= 1

    def test_credit_risk_assessment(self, financial_data):
        """測試信用風險評估"""
        transactions_df, accounts_df = financial_data

        # 計算賬戶風險指標
        account_metrics = transactions_df.groupBy("account_id").agg(
            count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_transaction_amount"),
            avg("amount").alias("avg_transaction_amount"),
            spark_max("amount").alias("max_transaction_amount"),
            spark_sum(
                when(col("type") == "Transfer", col("amount")).otherwise(0)
            ).alias("total_transfers"),
        )

        # 加入賬戶基本信息
        risk_assessment = account_metrics.join(accounts_df, "account_id")

        # 計算風險評分
        risk_scored = risk_assessment.withColumn(
            "utilization_ratio", col("total_transaction_amount") / col("balance")
        ).withColumn(
            "risk_score",
            when(col("utilization_ratio") > 0.5, 3)  # 高使用率
            .when(col("max_transaction_amount") > col("balance") * 0.3, 2)  # 大額交易
            .when(col("account_age_years") < 2, 1)  # 新賬戶
            .otherwise(0),
        )

        # 風險分級
        risk_categorized = risk_scored.withColumn(
            "risk_category",
            when(col("risk_score") >= 3, "High Risk")
            .when(col("risk_score") >= 2, "Medium Risk")
            .when(col("risk_score") >= 1, "Low Risk")
            .otherwise("Minimal Risk"),
        )

        # 檢查風險評估結果
        risk_distribution = risk_categorized.groupBy("risk_category").count().collect()
        risk_counts = {row.risk_category: row.count for row in risk_distribution}

        assert len(risk_counts) >= 1
        assert sum(risk_counts.values()) == accounts_df.count()

    def test_portfolio_analysis(self, financial_data):
        """測試組合分析"""
        transactions_df, accounts_df = financial_data

        # 模擬投資組合數據
        portfolio_data = [
            (1001, "AAPL", 100, 150.0),
            (1001, "GOOGL", 50, 2800.0),
            (1002, "MSFT", 75, 300.0),
            (1002, "TSLA", 25, 250.0),
            (1003, "AAPL", 200, 150.0),
            (1004, "GOOGL", 30, 2800.0),
            (1005, "MSFT", 150, 300.0),
        ]

        portfolio_df = spark_session.createDataFrame(
            portfolio_data, ["account_id", "symbol", "shares", "current_price"]
        )

        # 計算投資組合價值
        portfolio_value = portfolio_df.withColumn(
            "position_value", col("shares") * col("current_price")
        )

        # 按賬戶聚合
        account_portfolio = portfolio_value.groupBy("account_id").agg(
            spark_sum("position_value").alias("total_portfolio_value"),
            count("symbol").alias("num_positions"),
            spark_max("position_value").alias("largest_position"),
        )

        # 加入賬戶信息
        portfolio_analysis = account_portfolio.join(accounts_df, "account_id")

        # 計算投資比例
        investment_ratio = portfolio_analysis.withColumn(
            "investment_to_balance_ratio", col("total_portfolio_value") / col("balance")
        )

        # 檢查組合分析結果
        assert (
            investment_ratio.count()
            == portfolio_df.select("account_id").distinct().count()
        )

        # 檢查風險分散度
        concentration_risk = investment_ratio.withColumn(
            "concentration_risk", col("largest_position") / col("total_portfolio_value")
        )

        high_concentration = concentration_risk.filter(col("concentration_risk") > 0.5)
        # 這個測試主要確保計算不會出錯
        assert concentration_risk.count() >= 1
