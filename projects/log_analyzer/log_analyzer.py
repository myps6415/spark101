#!/usr/bin/env python3
"""
日誌分析系統

這是一個完整的日誌分析系統，用於分析網站訪問日誌，檢測異常行為和安全威脅。

功能特色：
- 日誌解析和資料清理
- 異常檢測和安全分析
- 時間序列分析
- 實時監控和警報
- 可視化報告生成

使用方法：
    python log_analyzer.py --input-path /path/to/logs --output-path /path/to/output
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


class LogAnalyzer:
    """日誌分析器主類"""

    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.log_df = None
        self.config = {
            "suspicious_ips": [],
            "sensitive_paths": ["/admin", "/config", "/etc/passwd", "/.env"],
            "error_threshold": 100,
            "high_freq_threshold": 1000,
            "response_time_threshold": 2.0,
        }

    def parse_apache_log(self, log_file_path: str) -> DataFrame:
        """
        解析 Apache Common Log Format 日誌

        Args:
            log_file_path: 日誌文件路徑

        Returns:
            DataFrame: 解析後的日誌資料
        """
        print(f"正在解析日誌文件: {log_file_path}")

        # 讀取原始日誌
        raw_logs = self.spark.read.text(log_file_path)

        # Apache Common Log Format 正規表達式
        log_pattern = r'^(\S+) \S+ \S+ \[([^\]]+)\] "(\S+) (\S+) (\S+)" (\d+) (\d+) "([^"]*)" "([^"]*)" (\S+)$'

        # 解析日誌
        parsed_logs = raw_logs.select(
            regexp_extract(col("value"), log_pattern, 1).alias("ip"),
            regexp_extract(col("value"), log_pattern, 2).alias("timestamp_str"),
            regexp_extract(col("value"), log_pattern, 3).alias("method"),
            regexp_extract(col("value"), log_pattern, 4).alias("path"),
            regexp_extract(col("value"), log_pattern, 5).alias("protocol"),
            regexp_extract(col("value"), log_pattern, 6)
            .cast("int")
            .alias("status_code"),
            regexp_extract(col("value"), log_pattern, 7)
            .cast("int")
            .alias("response_size"),
            regexp_extract(col("value"), log_pattern, 8).alias("referer"),
            regexp_extract(col("value"), log_pattern, 9).alias("user_agent"),
            regexp_extract(col("value"), log_pattern, 10)
            .cast("double")
            .alias("response_time"),
            col("value").alias("raw_log"),
        )

        # 轉換時間戳
        parsed_logs = parsed_logs.withColumn(
            "timestamp", to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z")
        )

        # 過濾掉解析失敗的記錄
        self.log_df = parsed_logs.filter(
            (col("ip").isNotNull())
            & (col("timestamp").isNotNull())
            & (col("status_code").isNotNull())
        )

        print(f"成功解析 {self.log_df.count()} 條日誌記錄")
        return self.log_df

    def detect_security_threats(self) -> Dict[str, DataFrame]:
        """
        檢測安全威脅

        Returns:
            Dict[str, DataFrame]: 各種安全威脅的檢測結果
        """
        print("正在檢測安全威脅...")

        threats = {}

        # 1. SQL 注入檢測
        sql_injection_patterns = [
            r"(?i)(union|select|drop|insert|update|delete|script|alert)",
            r"['\"]",
            r"--",
            r"/\*.*\*/",
        ]

        sql_injection_condition = col("path").rlike("|".join(sql_injection_patterns))
        threats["sql_injection"] = (
            self.log_df.filter(sql_injection_condition)
            .groupBy("ip", "path")
            .agg(
                count("*").alias("attempt_count"),
                collect_list("timestamp").alias("timestamps"),
            )
            .orderBy(col("attempt_count").desc())
        )

        # 2. 目錄遍歷攻擊檢測
        directory_traversal_condition = col("path").rlike(r"\.\.\/|\.\.\\")
        threats["directory_traversal"] = (
            self.log_df.filter(directory_traversal_condition)
            .groupBy("ip", "path")
            .agg(
                count("*").alias("attempt_count"),
                collect_list("timestamp").alias("timestamps"),
            )
            .orderBy(col("attempt_count").desc())
        )

        # 3. 暴力破解檢測
        brute_force_condition = (col("path").rlike(r"(?i)(login|admin|signin)")) & (
            col("status_code").isin([401, 403])
        )
        threats["brute_force"] = (
            self.log_df.filter(brute_force_condition)
            .groupBy("ip")
            .agg(
                count("*").alias("failed_attempts"),
                collect_list("timestamp").alias("timestamps"),
                collect_list("path").alias("paths"),
            )
            .filter(col("failed_attempts") > 10)
            .orderBy(col("failed_attempts").desc())
        )

        # 4. 異常高頻訪問檢測
        high_freq_condition = (
            self.log_df.groupBy("ip")
            .agg(count("*").alias("request_count"))
            .filter(col("request_count") > self.config["high_freq_threshold"])
        )

        threats["high_frequency"] = high_freq_condition.join(
            self.log_df.groupBy("ip").agg(
                collect_list("path").alias("paths"),
                collect_list("user_agent").alias("user_agents"),
            ),
            "ip",
        ).orderBy(col("request_count").desc())

        # 5. 敏感路徑訪問檢測
        sensitive_path_condition = col("path").rlike(
            "|".join(self.config["sensitive_paths"])
        )
        threats["sensitive_access"] = (
            self.log_df.filter(sensitive_path_condition)
            .groupBy("ip", "path")
            .agg(
                count("*").alias("access_count"),
                collect_list("timestamp").alias("timestamps"),
                collect_list("status_code").alias("status_codes"),
            )
            .orderBy(col("access_count").desc())
        )

        # 6. 異常用戶代理檢測
        bot_patterns = [
            r"(?i)(bot|crawler|spider|scraper)",
            r"(?i)(curl|wget|python|java)",
            r"^-$",  # 空用戶代理
            r"^$",  # 完全空白
        ]

        bot_condition = col("user_agent").rlike("|".join(bot_patterns))
        threats["suspicious_bots"] = (
            self.log_df.filter(bot_condition)
            .groupBy("ip", "user_agent")
            .agg(count("*").alias("request_count"), collect_list("path").alias("paths"))
            .filter(col("request_count") > 100)
            .orderBy(col("request_count").desc())
        )

        # 統計威脅摘要
        threat_summary = {}
        for threat_type, threat_df in threats.items():
            count = threat_df.count()
            threat_summary[threat_type] = count
            print(f"檢測到 {threat_type}: {count} 個威脅")

        return threats

    def analyze_traffic_patterns(self) -> Dict[str, DataFrame]:
        """
        分析流量模式

        Returns:
            Dict[str, DataFrame]: 流量分析結果
        """
        print("正在分析流量模式...")

        patterns = {}

        # 1. 時間序列分析
        patterns["hourly_traffic"] = (
            self.log_df.withColumn("hour", hour("timestamp"))
            .groupBy("hour")
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                countDistinct("ip").alias("unique_ips"),
                sum(when(col("status_code") >= 400, 1).otherwise(0)).alias(
                    "error_count"
                ),
            )
            .orderBy("hour")
        )

        # 2. 每日流量模式
        patterns["daily_traffic"] = (
            self.log_df.withColumn("date", date_format("timestamp", "yyyy-MM-dd"))
            .groupBy("date")
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                countDistinct("ip").alias("unique_ips"),
                sum(when(col("status_code") >= 400, 1).otherwise(0)).alias(
                    "error_count"
                ),
            )
            .orderBy("date")
        )

        # 3. 熱門路徑分析
        patterns["popular_paths"] = (
            self.log_df.groupBy("path")
            .agg(
                count("*").alias("request_count"),
                avg("response_time").alias("avg_response_time"),
                countDistinct("ip").alias("unique_visitors"),
            )
            .orderBy(col("request_count").desc())
        )

        # 4. 狀態碼分析
        patterns["status_analysis"] = (
            self.log_df.groupBy("status_code")
            .agg(
                count("*").alias("count"),
                (count("*") * 100.0 / self.log_df.count()).alias("percentage"),
            )
            .orderBy("status_code")
        )

        # 5. 用戶代理分析
        patterns["user_agent_analysis"] = (
            self.log_df.groupBy("user_agent")
            .agg(
                count("*").alias("request_count"),
                countDistinct("ip").alias("unique_ips"),
            )
            .orderBy(col("request_count").desc())
        )

        # 6. 響應時間分析
        patterns["response_time_analysis"] = (
            self.log_df.select(
                "path",
                "response_time",
                when(col("response_time") < 0.1, "very_fast")
                .when(col("response_time") < 0.5, "fast")
                .when(col("response_time") < 1.0, "normal")
                .when(col("response_time") < 2.0, "slow")
                .otherwise("very_slow")
                .alias("response_category"),
            )
            .groupBy("response_category")
            .agg(
                count("*").alias("count"),
                avg("response_time").alias("avg_response_time"),
            )
            .orderBy("response_category")
        )

        return patterns

    def detect_anomalies(self) -> Dict[str, DataFrame]:
        """
        檢測異常行為

        Returns:
            Dict[str, DataFrame]: 異常檢測結果
        """
        print("正在檢測異常行為...")

        anomalies = {}

        # 1. 響應時間異常
        response_time_stats = self.log_df.select(
            mean("response_time").alias("mean_response_time"),
            stddev("response_time").alias("std_response_time"),
        ).collect()[0]

        mean_time = response_time_stats["mean_response_time"]
        std_time = response_time_stats["std_response_time"]
        threshold = mean_time + 2 * std_time

        anomalies["slow_requests"] = (
            self.log_df.filter(col("response_time") > threshold)
            .select("timestamp", "ip", "path", "response_time", "status_code")
            .orderBy(col("response_time").desc())
        )

        # 2. 請求頻率異常
        window_spec = (
            Window.partitionBy("ip").orderBy("timestamp").rangeBetween(-300, 0)
        )  # 5分鐘窗口

        request_frequency = self.log_df.withColumn(
            "requests_in_window", count("*").over(window_spec)
        ).filter(
            col("requests_in_window") > 100
        )  # 5分鐘內超過100次請求

        anomalies["high_frequency_requests"] = request_frequency.select(
            "timestamp", "ip", "path", "requests_in_window"
        ).orderBy(col("requests_in_window").desc())

        # 3. 狀態碼異常模式
        error_patterns = (
            self.log_df.filter(col("status_code") >= 400)
            .groupBy("ip", "status_code")
            .agg(
                count("*").alias("error_count"),
                collect_list("path").alias("error_paths"),
            )
            .filter(col("error_count") > 50)
        )

        anomalies["error_patterns"] = error_patterns.orderBy(col("error_count").desc())

        # 4. 路徑訪問異常
        path_anomalies = (
            self.log_df.groupBy("ip", "path")
            .agg(count("*").alias("access_count"))
            .filter(col("access_count") > 200)
        )  # 單個IP對單個路徑的訪問超過200次

        anomalies["path_anomalies"] = path_anomalies.orderBy(col("access_count").desc())

        return anomalies

    def generate_security_report(self, output_path: str) -> None:
        """
        生成安全報告

        Args:
            output_path: 報告輸出路徑
        """
        print(f"正在生成安全報告: {output_path}")

        # 檢測威脅
        threats = self.detect_security_threats()
        anomalies = self.detect_anomalies()
        patterns = self.analyze_traffic_patterns()

        # 生成報告
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "summary": {
                "total_requests": self.log_df.count(),
                "unique_ips": self.log_df.select("ip").distinct().count(),
                "date_range": {
                    "start": self.log_df.agg(min("timestamp")).collect()[0][0],
                    "end": self.log_df.agg(max("timestamp")).collect()[0][0],
                },
            },
            "threats": {},
            "anomalies": {},
            "recommendations": [],
        }

        # 添加威脅信息
        for threat_type, threat_df in threats.items():
            count = threat_df.count()
            report["threats"][threat_type] = {
                "count": count,
                "severity": "high" if count > 10 else "medium" if count > 0 else "low",
            }

        # 添加異常信息
        for anomaly_type, anomaly_df in anomalies.items():
            count = anomaly_df.count()
            report["anomalies"][anomaly_type] = {
                "count": count,
                "severity": (
                    "high" if count > 100 else "medium" if count > 10 else "low"
                ),
            }

        # 生成建議
        recommendations = []

        if report["threats"]["sql_injection"]["count"] > 0:
            recommendations.append(
                "檢測到 SQL 注入嘗試，建議加強輸入驗證和使用參數化查詢"
            )

        if report["threats"]["brute_force"]["count"] > 0:
            recommendations.append("檢測到暴力破解嘗試，建議實施帳戶鎖定策略")

        if report["threats"]["high_frequency"]["count"] > 0:
            recommendations.append("檢測到高頻率訪問，建議實施速率限制")

        if report["anomalies"]["slow_requests"]["count"] > 100:
            recommendations.append("檢測到大量慢請求，建議優化應用程式性能")

        report["recommendations"] = recommendations

        # 保存報告
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)

        print(f"安全報告已保存至: {output_path}")

    def generate_visualizations(self, output_dir: str) -> None:
        """
        生成可視化圖表

        Args:
            output_dir: 圖表輸出目錄
        """
        print(f"正在生成可視化圖表: {output_dir}")

        os.makedirs(output_dir, exist_ok=True)

        # 分析流量模式
        patterns = self.analyze_traffic_patterns()

        # 1. 每小時流量圖
        hourly_data = patterns["hourly_traffic"].toPandas()

        fig, axes = plt.subplots(2, 2, figsize=(15, 10))

        # 每小時請求量
        axes[0, 0].plot(hourly_data["hour"], hourly_data["request_count"], marker="o")
        axes[0, 0].set_title("每小時請求量")
        axes[0, 0].set_xlabel("小時")
        axes[0, 0].set_ylabel("請求數")
        axes[0, 0].grid(True)

        # 每小時響應時間
        axes[0, 1].plot(
            hourly_data["hour"],
            hourly_data["avg_response_time"],
            marker="o",
            color="red",
        )
        axes[0, 1].set_title("每小時平均響應時間")
        axes[0, 1].set_xlabel("小時")
        axes[0, 1].set_ylabel("響應時間 (秒)")
        axes[0, 1].grid(True)

        # 每小時唯一IP數
        axes[1, 0].plot(
            hourly_data["hour"], hourly_data["unique_ips"], marker="o", color="green"
        )
        axes[1, 0].set_title("每小時唯一IP數")
        axes[1, 0].set_xlabel("小時")
        axes[1, 0].set_ylabel("唯一IP數")
        axes[1, 0].grid(True)

        # 每小時錯誤數
        axes[1, 1].plot(
            hourly_data["hour"], hourly_data["error_count"], marker="o", color="orange"
        )
        axes[1, 1].set_title("每小時錯誤數")
        axes[1, 1].set_xlabel("小時")
        axes[1, 1].set_ylabel("錯誤數")
        axes[1, 1].grid(True)

        plt.tight_layout()
        plt.savefig(
            os.path.join(output_dir, "hourly_traffic_analysis.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        # 2. 狀態碼分佈圖
        status_data = patterns["status_analysis"].toPandas()

        plt.figure(figsize=(10, 6))
        plt.pie(
            status_data["count"], labels=status_data["status_code"], autopct="%1.1f%%"
        )
        plt.title("HTTP 狀態碼分佈")
        plt.savefig(
            os.path.join(output_dir, "status_code_distribution.png"),
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        # 3. 熱門路徑圖
        popular_paths = patterns["popular_paths"].limit(10).toPandas()

        plt.figure(figsize=(12, 8))
        plt.barh(popular_paths["path"], popular_paths["request_count"])
        plt.title("熱門路徑 (前10名)")
        plt.xlabel("請求數")
        plt.ylabel("路徑")
        plt.tight_layout()
        plt.savefig(
            os.path.join(output_dir, "popular_paths.png"), dpi=300, bbox_inches="tight"
        )
        plt.close()

        print(f"可視化圖表已保存至: {output_dir}")


def main():
    """主函數"""
    parser = argparse.ArgumentParser(description="日誌分析系統")
    parser.add_argument("--input-path", required=True, help="日誌文件路徑")
    parser.add_argument("--output-path", required=True, help="輸出目錄路徑")
    parser.add_argument("--app-name", default="LogAnalyzer", help="Spark 應用名稱")

    args = parser.parse_args()

    # 檢查輸入文件
    if not os.path.exists(args.input_path):
        print(f"錯誤: 輸入文件不存在: {args.input_path}")
        sys.exit(1)

    # 建立輸出目錄
    os.makedirs(args.output_path, exist_ok=True)

    # 建立 Spark 會話
    spark = (
        SparkSession.builder.appName(args.app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )

    try:
        # 建立日誌分析器
        analyzer = LogAnalyzer(spark)

        # 解析日誌
        analyzer.parse_apache_log(args.input_path)

        # 生成安全報告
        report_path = os.path.join(args.output_path, "security_report.json")
        analyzer.generate_security_report(report_path)

        # 生成可視化圖表
        viz_dir = os.path.join(args.output_path, "visualizations")
        analyzer.generate_visualizations(viz_dir)

        print(f"分析完成! 結果已保存至: {args.output_path}")

    except Exception as e:
        print(f"錯誤: {str(e)}")
        sys.exit(1)

    finally:
        # 關閉 Spark 會話
        spark.stop()


if __name__ == "__main__":
    main()
