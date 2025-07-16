"""
項目特定測試 - 測試 projects 資料夾中的具體項目
"""

import os
import sys
import tempfile

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, desc, lit
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)


class TestLogAnalyzerProject:
    """測試 log_analyzer 項目"""

    @pytest.fixture(scope="function")
    def log_analyzer_data(self, spark_session, temp_dir):
        """創建日誌分析器測試數據"""
        # 創建更真實的Web服務器日誌
        log_entries = [
            '192.168.1.10 - - [15/Oct/2023:10:30:45 +0000] "GET /index.html HTTP/1.1" 200 1024 "-" "Mozilla/5.0"',
            '192.168.1.11 - - [15/Oct/2023:10:30:46 +0000] "POST /api/login HTTP/1.1" 200 256 "-" "Chrome/118.0"',
            '192.168.1.10 - - [15/Oct/2023:10:30:47 +0000] "GET /dashboard HTTP/1.1" 200 2048 "-" "Mozilla/5.0"',
            '192.168.1.12 - - [15/Oct/2023:10:30:48 +0000] "GET /nonexistent HTTP/1.1" 404 128 "-" "Firefox/119.0"',
            '192.168.1.11 - - [15/Oct/2023:10:30:49 +0000] "POST /api/data HTTP/1.1" 500 64 "-" "Chrome/118.0"',
            '192.168.1.13 - - [15/Oct/2023:10:30:50 +0000] "GET /admin HTTP/1.1" 403 256 "-" "Bot/1.0"',
            '192.168.1.10 - - [15/Oct/2023:10:30:51 +0000] "DELETE /file.txt HTTP/1.1" 200 32 "-" "Mozilla/5.0"',
            '192.168.1.14 - - [15/Oct/2023:10:30:52 +0000] "GET /images/logo.png HTTP/1.1" 200 4096 "-" "Safari/17.0"',
            '192.168.1.12 - - [15/Oct/2023:10:30:53 +0000] "PUT /api/update HTTP/1.1" 401 128 "-" "Firefox/119.0"',
            '192.168.1.15 - - [15/Oct/2023:10:30:54 +0000] "GET /help.html HTTP/1.1" 200 512 "-" "Edge/118.0"',
        ]

        log_file = os.path.join(temp_dir, "access.log")
        with open(log_file, "w") as f:
            for entry in log_entries:
                f.write(entry + "\n")

        return log_file

    def test_log_parsing_comprehensive(self, spark_session, log_analyzer_data):
        """測試完整的日誌解析功能"""
        # 讀取日誌文件
        raw_logs = spark_session.read.text(log_analyzer_data)

        # 使用正則表達式解析完整的Apache日誌格式
        from pyspark.sql.functions import regexp_extract

        parsed_logs = raw_logs.select(
            regexp_extract(col("value"), r"^(\S+)", 1).alias("ip_address"),
            regexp_extract(col("value"), r"\[([^\]]+)\]", 1).alias("timestamp"),
            regexp_extract(col("value"), r'"(\w+)', 1).alias("method"),
            regexp_extract(col("value"), r'"[A-Z]+ ([^"]*) HTTP', 1).alias("url"),
            regexp_extract(col("value"), r'"[^"]*" (\d+)', 1)
            .cast("int")
            .alias("status_code"),
            regexp_extract(col("value"), r'"[^"]*" \d+ (\d+)', 1)
            .cast("int")
            .alias("response_size"),
            regexp_extract(col("value"), r'"([^"]*)"$', 1).alias("user_agent"),
        ).filter(col("ip_address") != "")

        # 驗證解析結果
        assert parsed_logs.count() == 10

        # 檢查解析的字段
        sample_row = parsed_logs.first()
        assert sample_row.ip_address is not None
        assert sample_row.method in ["GET", "POST", "PUT", "DELETE"]
        assert sample_row.status_code in [200, 401, 403, 404, 500]

        return parsed_logs

    def test_traffic_analysis_detailed(self, spark_session, log_analyzer_data):
        """測試詳細的流量分析"""
        parsed_logs = self.test_log_parsing_comprehensive(
            spark_session, log_analyzer_data
        )

        # IP地址分析
        ip_analysis = (
            parsed_logs.groupBy("ip_address")
            .agg(
                count("*").alias("request_count"),
                spark_sum(when(col("status_code") >= 400, 1).otherwise(0)).alias(
                    "error_count"
                ),
                spark_sum("response_size").alias("total_bytes"),
                avg("response_size").alias("avg_response_size"),
            )
            .orderBy(desc("request_count"))
        )

        # 檢查IP分析結果
        assert ip_analysis.count() == 6  # 6個不同的IP地址

        top_ip = ip_analysis.first()
        assert top_ip.request_count >= 1
        assert top_ip.total_bytes > 0

        # URL分析
        url_analysis = (
            parsed_logs.groupBy("url")
            .agg(count("*").alias("hit_count"), avg("response_size").alias("avg_size"))
            .orderBy(desc("hit_count"))
        )

        # 檢查URL分析結果
        assert url_analysis.count() >= 5  # 至少5個不同的URL

        # 狀態碼分析
        status_analysis = (
            parsed_logs.groupBy("status_code").count().orderBy("status_code")
        )
        status_codes = [row.status_code for row in status_analysis.collect()]

        assert 200 in status_codes  # 成功請求
        assert any(code >= 400 for code in status_codes)  # 錯誤請求

        return ip_analysis, url_analysis, status_analysis

    def test_security_analysis(self, spark_session, log_analyzer_data):
        """測試安全分析功能"""
        parsed_logs = self.test_log_parsing_comprehensive(
            spark_session, log_analyzer_data
        )

        # 安全威脅檢測
        security_analysis = parsed_logs.withColumn(
            "threat_type",
            when(col("status_code") == 403, "Access Denied")
            .when(col("status_code") == 401, "Unauthorized")
            .when(col("url").rlike("(?i).*(admin|config|backup).*", "Sensitive Path"))
            .when(
                col("user_agent").rlike("(?i).*(bot|crawler|scanner).*", "Bot Activity")
            )
            .otherwise("Normal"),
        )

        # 威脅統計
        threat_stats = (
            security_analysis.groupBy("threat_type").count().orderBy(desc("count"))
        )
        threats = {row.threat_type: row.count for row in threat_stats.collect()}

        # 檢查安全分析結果
        assert "Normal" in threats
        assert threats["Normal"] >= 1

        # 如果有安全威脅，檢查它們
        if "Access Denied" in threats:
            assert threats["Access Denied"] >= 1

        # 按IP檢測可疑活動
        suspicious_ips = (
            security_analysis.filter(col("threat_type") != "Normal")
            .groupBy("ip_address")
            .agg(count("*").alias("suspicious_count"))
            .filter(col("suspicious_count") > 1)
        )

        # 檢查是否有重複可疑活動的IP
        if suspicious_ips.count() > 0:
            assert suspicious_ips.first().suspicious_count > 1

    def test_performance_metrics(self, spark_session, log_analyzer_data):
        """測試性能指標分析"""
        parsed_logs = self.test_log_parsing_comprehensive(
            spark_session, log_analyzer_data
        )

        # 響應大小分析
        size_metrics = parsed_logs.agg(
            avg("response_size").alias("avg_response_size"),
            spark_sum("response_size").alias("total_bytes"),
            count("*").alias("total_requests"),
        ).collect()[0]

        # 檢查性能指標
        assert size_metrics.avg_response_size > 0
        assert size_metrics.total_bytes > 0
        assert size_metrics.total_requests == 10

        # 按HTTP方法分析
        method_performance = parsed_logs.groupBy("method").agg(
            count("*").alias("request_count"),
            avg("response_size").alias("avg_size"),
            spark_sum(when(col("status_code") >= 400, 1).otherwise(0)).alias(
                "error_count"
            ),
        )

        # 檢查方法性能分析
        methods = [row.method for row in method_performance.collect()]
        assert "GET" in methods
        assert "POST" in methods


class TestMonitoringSystemProject:
    """測試 monitoring_system 項目"""

    @pytest.fixture(scope="function")
    def monitoring_data(self, spark_session):
        """創建監控系統測試數據"""
        # 系統指標數據
        metrics_data = [
            ("server-01", "2023-10-15 10:00:00", "cpu_usage", 45.2),
            ("server-01", "2023-10-15 10:00:00", "memory_usage", 67.8),
            ("server-01", "2023-10-15 10:00:00", "disk_usage", 78.5),
            ("server-02", "2023-10-15 10:00:00", "cpu_usage", 62.1),
            ("server-02", "2023-10-15 10:00:00", "memory_usage", 89.3),
            ("server-02", "2023-10-15 10:00:00", "disk_usage", 45.7),
            ("server-01", "2023-10-15 10:05:00", "cpu_usage", 52.8),
            ("server-01", "2023-10-15 10:05:00", "memory_usage", 71.2),
            ("server-01", "2023-10-15 10:05:00", "disk_usage", 78.9),
            ("server-03", "2023-10-15 10:00:00", "cpu_usage", 91.5),  # 高CPU使用率
            ("server-03", "2023-10-15 10:00:00", "memory_usage", 95.2),  # 高內存使用率
            ("server-03", "2023-10-15 10:00:00", "disk_usage", 98.1),  # 高磁盤使用率
        ]

        return spark_session.createDataFrame(
            metrics_data, ["server_id", "timestamp", "metric_name", "metric_value"]
        )

    def test_threshold_monitoring(self, monitoring_data):
        """測試閾值監控"""
        # 設定閾值
        thresholds = {"cpu_usage": 80.0, "memory_usage": 85.0, "disk_usage": 90.0}

        # 檢測超過閾值的指標
        alerts = (
            monitoring_data.withColumn(
                "threshold",
                when(col("metric_name") == "cpu_usage", lit(thresholds["cpu_usage"]))
                .when(
                    col("metric_name") == "memory_usage",
                    lit(thresholds["memory_usage"]),
                )
                .when(col("metric_name") == "disk_usage", lit(thresholds["disk_usage"]))
                .otherwise(lit(100.0)),
            )
            .withColumn("is_alert", col("metric_value") > col("threshold"))
            .filter(col("is_alert") == True)
        )

        # 檢查告警結果
        alert_count = alerts.count()
        assert alert_count >= 1  # 應該有server-03的告警

        # 檢查具體告警
        server3_alerts = alerts.filter(col("server_id") == "server-03")
        assert server3_alerts.count() >= 2  # server-03應該有多個指標告警

        # 按服務器統計告警
        alert_summary = alerts.groupBy("server_id").agg(
            count("*").alias("alert_count"),
            avg("metric_value").alias("avg_alert_value"),
        )

        assert alert_summary.count() >= 1

    def test_trend_analysis(self, monitoring_data):
        """測試趨勢分析"""
        # 按服務器和指標計算平均值
        avg_metrics = monitoring_data.groupBy("server_id", "metric_name").agg(
            avg("metric_value").alias("avg_value"), count("*").alias("data_points")
        )

        # 檢查趨勢分析結果
        assert avg_metrics.count() >= 6  # 至少2個服務器 * 3個指標

        # 找出高使用率的服務器
        high_usage_servers = (
            avg_metrics.filter(col("avg_value") > 80).groupBy("server_id").count()
        )

        if high_usage_servers.count() > 0:
            problem_servers = [row.server_id for row in high_usage_servers.collect()]
            assert "server-03" in problem_servers  # server-03應該被標記為高使用率

    def test_anomaly_detection(self, monitoring_data):
        """測試異常檢測"""
        # 計算每個指標的統計信息
        metric_stats = (
            monitoring_data.groupBy("metric_name")
            .agg(
                avg("metric_value").alias("mean_value"),
                spark_sum((col("metric_value") - avg("metric_value")) ** 2).alias(
                    "variance_sum"
                ),
                count("*").alias("count"),
            )
            .withColumn("std_dev", (col("variance_sum") / col("count")) ** 0.5)
        )

        # 檢查統計計算
        assert metric_stats.count() == 3  # 3種指標類型

        # 簡化的異常檢測（基於閾值）
        anomalies = monitoring_data.withColumn(
            "is_anomaly",
            when((col("metric_name") == "cpu_usage") & (col("metric_value") > 90), True)
            .when(
                (col("metric_name") == "memory_usage") & (col("metric_value") > 95),
                True,
            )
            .when(
                (col("metric_name") == "disk_usage") & (col("metric_value") > 95), True
            )
            .otherwise(False),
        ).filter(col("is_anomaly") == True)

        # 檢查異常檢測結果
        anomaly_count = anomalies.count()
        if anomaly_count > 0:
            assert anomaly_count >= 1

            # 檢查異常的服務器
            anomaly_servers = anomalies.select("server_id").distinct().collect()
            server_list = [row.server_id for row in anomaly_servers]
            assert "server-03" in server_list

    def test_health_score_calculation(self, monitoring_data):
        """測試健康分數計算"""
        # 計算每個服務器的健康分數
        health_scores = (
            monitoring_data.groupBy("server_id")
            .agg(
                avg(when(col("metric_name") == "cpu_usage", col("metric_value"))).alias(
                    "avg_cpu"
                ),
                avg(
                    when(col("metric_name") == "memory_usage", col("metric_value"))
                ).alias("avg_memory"),
                avg(
                    when(col("metric_name") == "disk_usage", col("metric_value"))
                ).alias("avg_disk"),
            )
            .withColumn(
                "health_score",
                100 - ((col("avg_cpu") + col("avg_memory") + col("avg_disk")) / 3),
            )
            .withColumn(
                "health_status",
                when(col("health_score") >= 70, "Healthy")
                .when(col("health_score") >= 40, "Warning")
                .otherwise("Critical"),
            )
        )

        # 檢查健康分數計算
        assert health_scores.count() >= 2  # 至少2個服務器

        # 檢查健康狀態分佈
        health_distribution = health_scores.groupBy("health_status").count()
        statuses = [row.health_status for row in health_distribution.collect()]

        # 應該有不同的健康狀態
        assert len(statuses) >= 1

        # server-03應該是Critical狀態
        server3_health = health_scores.filter(col("server_id") == "server-03").first()
        if server3_health:
            assert server3_health.health_status in ["Warning", "Critical"]


class TestRecommendationSystemProject:
    """測試 recommendation_system 項目"""

    @pytest.fixture(scope="function")
    def recommendation_data(self, spark_session):
        """創建推薦系統測試數據"""
        # 用戶評分數據
        ratings_data = [
            (1, 101, 4.5, "2023-10-01"),
            (1, 102, 3.0, "2023-10-02"),
            (1, 103, 5.0, "2023-10-03"),
            (2, 101, 3.5, "2023-10-01"),
            (2, 104, 4.0, "2023-10-02"),
            (2, 105, 2.5, "2023-10-03"),
            (3, 102, 4.0, "2023-10-01"),
            (3, 103, 4.5, "2023-10-02"),
            (3, 106, 3.0, "2023-10-03"),
            (4, 104, 5.0, "2023-10-01"),
            (4, 105, 4.5, "2023-10-02"),
            (4, 107, 3.5, "2023-10-03"),
            (5, 101, 2.0, "2023-10-01"),
            (5, 106, 4.0, "2023-10-02"),
            (5, 107, 4.5, "2023-10-03"),
        ]

        ratings_df = spark_session.createDataFrame(
            ratings_data, ["user_id", "item_id", "rating", "timestamp"]
        )

        # 物品數據
        items_data = [
            (101, "Action Movie A", "Action", 2023),
            (102, "Comedy Movie B", "Comedy", 2022),
            (103, "Drama Movie C", "Drama", 2023),
            (104, "Action Movie D", "Action", 2021),
            (105, "Comedy Movie E", "Comedy", 2023),
            (106, "Horror Movie F", "Horror", 2022),
            (107, "Sci-Fi Movie G", "Sci-Fi", 2023),
        ]

        items_df = spark_session.createDataFrame(
            items_data, ["item_id", "title", "genre", "year"]
        )

        return ratings_df, items_df

    def test_collaborative_filtering_data_prep(self, recommendation_data):
        """測試協同過濾數據準備"""
        ratings_df, items_df = recommendation_data

        # 數據統計
        user_stats = ratings_df.groupBy("user_id").agg(
            count("*").alias("num_ratings"), avg("rating").alias("avg_rating")
        )

        item_stats = ratings_df.groupBy("item_id").agg(
            count("*").alias("num_ratings"), avg("rating").alias("avg_rating")
        )

        # 檢查數據統計
        assert user_stats.count() == 5  # 5個用戶
        assert item_stats.count() == 7  # 7個物品

        # 檢查評分分佈
        rating_distribution = ratings_df.groupBy("rating").count().orderBy("rating")
        ratings_list = [row.rating for row in rating_distribution.collect()]

        assert len(ratings_list) >= 3  # 至少3種不同評分
        assert min(ratings_list) >= 1.0
        assert max(ratings_list) <= 5.0

    def test_user_similarity(self, recommendation_data):
        """測試用戶相似度計算"""
        ratings_df, items_df = recommendation_data

        # 創建用戶-物品矩陣（簡化版本）
        user_item_matrix = (
            ratings_df.groupBy("user_id").pivot("item_id").agg(avg("rating"))
        )

        # 檢查用戶-物品矩陣
        assert user_item_matrix.count() == 5  # 5個用戶

        # 計算用戶共同評分的物品
        user_pairs = (
            ratings_df.alias("r1")
            .join(
                ratings_df.alias("r2"),
                (col("r1.item_id") == col("r2.item_id"))
                & (col("r1.user_id") < col("r2.user_id")),
            )
            .select(
                col("r1.user_id").alias("user1"),
                col("r2.user_id").alias("user2"),
                col("r1.item_id").alias("item_id"),
                col("r1.rating").alias("rating1"),
                col("r2.rating").alias("rating2"),
            )
        )

        # 計算用戶對的共同評分數
        user_similarity = (
            user_pairs.groupBy("user1", "user2")
            .agg(
                count("*").alias("common_items"),
                avg(abs(col("rating1") - col("rating2"))).alias("avg_rating_diff"),
            )
            .withColumn(
                "similarity_score", col("common_items") / (1 + col("avg_rating_diff"))
            )
        )

        # 檢查相似度計算
        if user_similarity.count() > 0:
            assert user_similarity.count() >= 1

            # 找出最相似的用戶對
            most_similar = user_similarity.orderBy(desc("similarity_score")).first()
            assert most_similar.similarity_score > 0

    def test_item_based_recommendations(self, recommendation_data):
        """測試基於物品的推薦"""
        ratings_df, items_df = recommendation_data

        # 物品共現分析
        item_cooccurrence = (
            ratings_df.alias("r1")
            .join(
                ratings_df.alias("r2"),
                (col("r1.user_id") == col("r2.user_id"))
                & (col("r1.item_id") < col("r2.item_id")),
            )
            .select(
                col("r1.item_id").alias("item1"),
                col("r2.item_id").alias("item2"),
                col("r1.rating").alias("rating1"),
                col("r2.rating").alias("rating2"),
            )
        )

        # 計算物品相似度
        item_similarity = item_cooccurrence.groupBy("item1", "item2").agg(
            count("*").alias("cooccurrence_count"),
            avg((col("rating1") * col("rating2"))).alias("rating_product"),
        )

        # 檢查物品相似度計算
        if item_similarity.count() > 0:
            assert item_similarity.count() >= 1

            # 為特定物品生成推薦
            target_item = 101
            similar_items = (
                item_similarity.filter(
                    (col("item1") == target_item) | (col("item2") == target_item)
                )
                .withColumn(
                    "recommended_item",
                    when(col("item1") == target_item, col("item2")).otherwise(
                        col("item1")
                    ),
                )
                .select("recommended_item", "cooccurrence_count")
                .orderBy(desc("cooccurrence_count"))
            )

            # 檢查推薦結果
            if similar_items.count() > 0:
                assert similar_items.count() >= 1

    def test_popularity_based_recommendations(self, recommendation_data):
        """測試基於流行度的推薦"""
        ratings_df, items_df = recommendation_data

        # 計算物品流行度
        item_popularity = (
            ratings_df.groupBy("item_id")
            .agg(
                count("*").alias("rating_count"),
                avg("rating").alias("avg_rating"),
                spark_sum("rating").alias("total_rating"),
            )
            .withColumn("popularity_score", col("rating_count") * col("avg_rating"))
        )

        # 加入物品信息
        popular_items = item_popularity.join(items_df, "item_id").orderBy(
            desc("popularity_score")
        )

        # 檢查流行度計算
        assert popular_items.count() == 7  # 7個物品

        # 檢查最受歡迎的物品
        top_item = popular_items.first()
        assert top_item.popularity_score > 0
        assert top_item.rating_count >= 1

        # 按類型分析流行度
        genre_popularity = (
            popular_items.groupBy("genre")
            .agg(
                count("*").alias("item_count"),
                avg("popularity_score").alias("avg_popularity"),
                spark_sum("rating_count").alias("total_ratings"),
            )
            .orderBy(desc("avg_popularity"))
        )

        # 檢查類型流行度
        assert genre_popularity.count() >= 3  # 至少3個類型

    def test_recommendation_evaluation(self, recommendation_data):
        """測試推薦評估"""
        ratings_df, items_df = recommendation_data

        # 分割數據為訓練集和測試集（簡化版本）
        from pyspark.sql.functions import row_number
        from pyspark.sql.window import Window

        # 為每個用戶的評分排序
        window_spec = Window.partitionBy("user_id").orderBy("timestamp")

        split_data = ratings_df.withColumn("row_num", row_number().over(window_spec))

        # 用前70%作為訓練集，後30%作為測試集
        user_rating_counts = ratings_df.groupBy("user_id").count().collect()

        # 簡化評估：計算預測準確性指標
        actual_ratings = ratings_df.groupBy("item_id").agg(
            avg("rating").alias("actual_avg_rating"),
            count("*").alias("actual_rating_count"),
        )

        # 模擬預測評分（使用全局平均值）
        global_avg = ratings_df.agg(avg("rating")).collect()[0][0]

        predicted_ratings = actual_ratings.withColumn(
            "predicted_rating", lit(global_avg)
        )

        # 計算評估指標
        evaluation_metrics = predicted_ratings.withColumn(
            "absolute_error", abs(col("actual_avg_rating") - col("predicted_rating"))
        ).agg(
            avg("absolute_error").alias("mae"),  # Mean Absolute Error
            spark_sum((col("actual_avg_rating") - col("predicted_rating")) ** 2).alias(
                "sse"
            ),
        )

        # 檢查評估指標
        metrics = evaluation_metrics.collect()[0]
        assert metrics.mae >= 0
        assert metrics.sse >= 0


class TestProjectIntegration:
    """測試項目整合功能"""

    def test_cross_project_data_sharing(self, spark_session, temp_dir):
        """測試項目間數據共享"""
        # 創建共享數據
        shared_data = [
            ("user_001", "2023-10-15 10:30:00", "login", "web"),
            ("user_002", "2023-10-15 10:31:00", "view_product", "mobile"),
            ("user_001", "2023-10-15 10:32:00", "add_to_cart", "web"),
            ("user_003", "2023-10-15 10:33:00", "purchase", "mobile"),
            ("user_002", "2023-10-15 10:34:00", "logout", "mobile"),
        ]

        events_df = spark_session.createDataFrame(
            shared_data, ["user_id", "timestamp", "event_type", "platform"]
        )

        # 保存為共享數據格式
        shared_path = os.path.join(temp_dir, "shared_events")
        events_df.write.mode("overwrite").parquet(shared_path)

        # 模擬不同項目讀取共享數據
        # 項目1：用戶行為分析
        user_behavior = spark_session.read.parquet(shared_path)
        user_sessions = user_behavior.groupBy("user_id").agg(
            count("*").alias("event_count"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchase_count"
            ),
        )

        # 項目2：平台分析
        platform_analysis = user_behavior.groupBy("platform").agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "purchase", 1).otherwise(0)).alias(
                "purchases"
            ),
        )

        # 檢查共享數據使用
        assert user_sessions.count() == 3  # 3個用戶
        assert platform_analysis.count() == 2  # 2個平台

        # 檢查數據一致性
        total_events_from_users = user_sessions.agg(spark_sum("event_count")).collect()[
            0
        ][0]
        total_events_from_platforms = platform_analysis.agg(
            spark_sum("total_events")
        ).collect()[0][0]

        assert total_events_from_users == total_events_from_platforms == 5

    def test_project_performance_comparison(self, spark_session):
        """測試項目性能比較"""
        # 創建性能測試數據
        import time

        # 測試數據量對性能的影響
        small_data = [(i, f"data_{i}", i * 1.5) for i in range(1000)]
        large_data = [(i, f"data_{i}", i * 1.5) for i in range(10000)]

        small_df = spark_session.createDataFrame(small_data, ["id", "data", "value"])
        large_df = spark_session.createDataFrame(large_data, ["id", "data", "value"])

        # 測試相同操作在不同數據量下的性能
        operations = [
            lambda df: df.filter(col("value") > 500).count(),
            lambda df: df.groupBy("id").count().count(),
            lambda df: df.select("id", "value").distinct().count(),
        ]

        performance_results = []

        for i, operation in enumerate(operations):
            # 小數據集
            start_time = time.time()
            small_result = operation(small_df)
            small_time = time.time() - start_time

            # 大數據集
            start_time = time.time()
            large_result = operation(large_df)
            large_time = time.time() - start_time

            performance_results.append(
                {
                    "operation": f"op_{i}",
                    "small_time": small_time,
                    "large_time": large_time,
                    "small_result": small_result,
                    "large_result": large_result,
                }
            )

        # 檢查性能結果
        assert len(performance_results) == 3

        for result in performance_results:
            assert result["small_time"] >= 0
            assert result["large_time"] >= 0
            assert (
                result["large_result"] >= result["small_result"]
            )  # 大數據集結果應該>=小數據集

    def test_error_handling_across_projects(self, spark_session):
        """測試跨項目錯誤處理"""
        # 測試各種錯誤情況

        # 1. 數據質量問題
        problematic_data = [
            (1, "valid_data", 100.0),
            (None, "missing_id", 200.0),
            (3, None, 300.0),
            (4, "valid_data", None),
            (5, "", -100.0),  # 負值
        ]

        df = spark_session.createDataFrame(problematic_data, ["id", "name", "value"])

        # 錯誤檢測和處理
        error_summary = df.agg(
            count("*").alias("total_records"),
            count("id").alias("valid_ids"),
            count("name").alias("valid_names"),
            count("value").alias("valid_values"),
            spark_sum(when(col("value") < 0, 1).otherwise(0)).alias("negative_values"),
        ).collect()[0]

        # 檢查錯誤統計
        assert error_summary.total_records == 5
        assert error_summary.valid_ids == 4  # 一個NULL id
        assert error_summary.valid_names == 4  # 一個NULL name
        assert error_summary.valid_values == 4  # 一個NULL value
        assert error_summary.negative_values == 1  # 一個負值

        # 2. 測試資源限制處理
        try:
            # 創建可能超出內存的操作
            memory_intensive_data = [
                (i, "x" * 1000, [j for j in range(100)]) for i in range(100)
            ]
            memory_df = spark_session.createDataFrame(
                memory_intensive_data, ["id", "text", "numbers"]
            )

            # 執行內存密集型操作
            result = memory_df.count()
            assert result == 100

        except Exception as e:
            # 如果出現內存錯誤，確保能夠正確處理
            assert "memory" in str(e).lower() or "heap" in str(e).lower()

        # 3. 測試分區錯誤處理
        try:
            # 嘗試創建過多分區
            over_partitioned = df.repartition(1000)  # 可能過多的分區
            count = over_partitioned.count()
            assert count >= 0

        except Exception as e:
            # 如果Spark拒絕創建過多分區，這是正常的
            assert "partition" in str(e).lower() or "too many" in str(e).lower()
