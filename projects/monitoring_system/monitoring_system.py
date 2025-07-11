#!/usr/bin/env python3
"""
即時監控系統

這是一個基於 Apache Spark 的即時監控系統，用於監控系統指標、檢測異常並發送警報。

功能特色：
- 即時系統指標監控
- 閾值監控和警報
- 異常檢測和預測
- 歷史趨勢分析
- 自動化報告生成

使用方法：
    python monitoring_system.py --config config.json --output-path /path/to/output
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
from threading import Thread
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


@dataclass
class AlertConfig:
    """警報配置"""
    cpu_threshold: float = 80.0
    memory_threshold: float = 85.0
    disk_threshold: float = 90.0
    network_latency_threshold: float = 100.0
    error_rate_threshold: float = 5.0
    response_time_threshold: float = 1000.0
    request_rate_threshold: int = 1000


@dataclass
class MonitoringConfig:
    """監控配置"""
    input_path: str
    output_path: str
    alert_config: AlertConfig
    monitoring_interval: int = 60  # 秒
    retention_days: int = 30
    enable_ml_detection: bool = True
    email_alerts: bool = False
    email_recipients: List[str] = None


class MetricsCollector:
    """指標收集器"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.schema = StructType([
            StructField("timestamp", TimestampType(), True),
            StructField("server_id", StringType(), True),
            StructField("service_name", StringType(), True),
            StructField("cpu_usage", DoubleType(), True),
            StructField("memory_usage", DoubleType(), True),
            StructField("disk_usage", DoubleType(), True),
            StructField("network_latency", DoubleType(), True),
            StructField("error_rate", DoubleType(), True),
            StructField("request_count", IntegerType(), True),
            StructField("response_time", DoubleType(), True),
            StructField("active_connections", IntegerType(), True),
            StructField("thread_count", IntegerType(), True),
            StructField("heap_usage", DoubleType(), True)
        ])
    
    def load_metrics(self, input_path: str) -> DataFrame:
        """
        載入指標資料
        
        Args:
            input_path: 指標資料路徑
            
        Returns:
            DataFrame: 指標資料
        """
        try:
            # 支援多種格式
            if input_path.endswith('.json'):
                df = self.spark.read.json(input_path)
            elif input_path.endswith('.csv'):
                df = self.spark.read.csv(input_path, header=True, inferSchema=True)
            elif input_path.endswith('.parquet'):
                df = self.spark.read.parquet(input_path)
            else:
                # 預設嘗試 JSON 格式
                df = self.spark.read.json(input_path)
            
            # 確保資料格式正確
            df = df.withColumn("timestamp", 
                              when(col("timestamp").isNotNull(), col("timestamp"))
                              .otherwise(current_timestamp()))
            
            return df
        
        except Exception as e:
            logging.error(f"載入指標資料失敗: {str(e)}")
            return None
    
    def generate_sample_metrics(self, num_records: int = 10000) -> DataFrame:
        """
        生成範例指標資料
        
        Args:
            num_records: 記錄數量
            
        Returns:
            DataFrame: 生成的指標資料
        """
        import random
        
        print(f"生成 {num_records} 條範例指標資料...")
        
        servers = [f"server-{i:03d}" for i in range(1, 21)]
        services = ["web", "api", "database", "cache", "queue", "worker"]
        
        metrics = []
        base_time = datetime.now() - timedelta(hours=24)
        
        for i in range(num_records):
            timestamp = base_time + timedelta(seconds=i * 30)
            server_id = random.choice(servers)
            service_name = random.choice(services)
            
            # 根據服務類型生成不同的基準值
            if service_name == "database":
                cpu_base = 60 + random.gauss(0, 10)
                memory_base = 70 + random.gauss(0, 10)
                response_time_base = 50 + random.gauss(0, 20)
            elif service_name == "cache":
                cpu_base = 20 + random.gauss(0, 5)
                memory_base = 80 + random.gauss(0, 10)
                response_time_base = 5 + random.gauss(0, 2)
            elif service_name == "web":
                cpu_base = 30 + random.gauss(0, 10)
                memory_base = 40 + random.gauss(0, 10)
                response_time_base = 100 + random.gauss(0, 30)
            else:
                cpu_base = 40 + random.gauss(0, 10)
                memory_base = 50 + random.gauss(0, 10)
                response_time_base = 80 + random.gauss(0, 25)
            
            # 模擬異常情況
            is_anomaly = random.random() < 0.1
            if is_anomaly:
                cpu_usage = min(100, max(0, cpu_base + random.gauss(30, 10)))
                memory_usage = min(100, max(0, memory_base + random.gauss(25, 10)))
                response_time = max(0, response_time_base + random.gauss(200, 50))
                error_rate = max(0, random.gauss(8, 3))
            else:
                cpu_usage = min(100, max(0, cpu_base))
                memory_usage = min(100, max(0, memory_base))
                response_time = max(0, response_time_base)
                error_rate = max(0, random.gauss(1, 0.5))
            
            disk_usage = min(100, max(0, random.gauss(65, 15)))
            network_latency = max(0, random.gauss(25, 10))
            request_count = max(0, int(random.gauss(100, 30)))
            active_connections = max(0, int(random.gauss(50, 15)))
            thread_count = max(0, int(random.gauss(20, 5)))
            heap_usage = min(100, max(0, random.gauss(45, 15)))
            
            metrics.append((
                timestamp,
                server_id,
                service_name,
                cpu_usage,
                memory_usage,
                disk_usage,
                network_latency,
                error_rate,
                request_count,
                response_time,
                active_connections,
                thread_count,
                heap_usage
            ))
        
        df = self.spark.createDataFrame(metrics, self.schema)
        print(f"成功生成 {df.count()} 條指標記錄")
        return df


class AlertManager:
    """警報管理器"""
    
    def __init__(self, config: AlertConfig):
        self.config = config
        self.alert_history = []
    
    def check_thresholds(self, metrics_df: DataFrame) -> DataFrame:
        """
        檢查閾值並生成警報
        
        Args:
            metrics_df: 指標資料
            
        Returns:
            DataFrame: 觸發的警報
        """
        alerts = []
        
        # CPU 使用率警報
        cpu_alerts = metrics_df.filter(
            col("cpu_usage") > self.config.cpu_threshold
        ).withColumn("alert_type", lit("cpu_high")) \
         .withColumn("alert_message", 
                    concat(lit("CPU 使用率過高: "), col("cpu_usage"), lit("%"))) \
         .withColumn("severity", 
                    when(col("cpu_usage") > 95, "critical")
                    .when(col("cpu_usage") > 90, "high")
                    .otherwise("medium"))
        
        # 記憶體使用率警報
        memory_alerts = metrics_df.filter(
            col("memory_usage") > self.config.memory_threshold
        ).withColumn("alert_type", lit("memory_high")) \
         .withColumn("alert_message", 
                    concat(lit("記憶體使用率過高: "), col("memory_usage"), lit("%"))) \
         .withColumn("severity", 
                    when(col("memory_usage") > 95, "critical")
                    .when(col("memory_usage") > 90, "high")
                    .otherwise("medium"))
        
        # 磁碟使用率警報
        disk_alerts = metrics_df.filter(
            col("disk_usage") > self.config.disk_threshold
        ).withColumn("alert_type", lit("disk_high")) \
         .withColumn("alert_message", 
                    concat(lit("磁碟使用率過高: "), col("disk_usage"), lit("%"))) \
         .withColumn("severity", 
                    when(col("disk_usage") > 98, "critical")
                    .when(col("disk_usage") > 95, "high")
                    .otherwise("medium"))
        
        # 網路延遲警報
        latency_alerts = metrics_df.filter(
            col("network_latency") > self.config.network_latency_threshold
        ).withColumn("alert_type", lit("latency_high")) \
         .withColumn("alert_message", 
                    concat(lit("網路延遲過高: "), col("network_latency"), lit("ms"))) \
         .withColumn("severity", 
                    when(col("network_latency") > 500, "critical")
                    .when(col("network_latency") > 200, "high")
                    .otherwise("medium"))
        
        # 錯誤率警報
        error_alerts = metrics_df.filter(
            col("error_rate") > self.config.error_rate_threshold
        ).withColumn("alert_type", lit("error_rate_high")) \
         .withColumn("alert_message", 
                    concat(lit("錯誤率過高: "), col("error_rate"), lit("%"))) \
         .withColumn("severity", 
                    when(col("error_rate") > 20, "critical")
                    .when(col("error_rate") > 10, "high")
                    .otherwise("medium"))
        
        # 響應時間警報
        response_time_alerts = metrics_df.filter(
            col("response_time") > self.config.response_time_threshold
        ).withColumn("alert_type", lit("response_time_high")) \
         .withColumn("alert_message", 
                    concat(lit("響應時間過高: "), col("response_time"), lit("ms"))) \
         .withColumn("severity", 
                    when(col("response_time") > 5000, "critical")
                    .when(col("response_time") > 2000, "high")
                    .otherwise("medium"))
        
        # 合併所有警報
        all_alerts = cpu_alerts.union(memory_alerts) \
                              .union(disk_alerts) \
                              .union(latency_alerts) \
                              .union(error_alerts) \
                              .union(response_time_alerts)
        
        # 添加警報 ID 和時間戳
        all_alerts = all_alerts.withColumn("alert_id", 
                                         concat(col("server_id"), lit("_"), 
                                               col("alert_type"), lit("_"), 
                                               col("timestamp").cast("string"))) \
                               .withColumn("alert_timestamp", current_timestamp())
        
        return all_alerts
    
    def process_alerts(self, alerts_df: DataFrame) -> Dict[str, int]:
        """
        處理警報
        
        Args:
            alerts_df: 警報資料
            
        Returns:
            Dict[str, int]: 警報統計
        """
        alert_counts = {}
        
        if alerts_df.count() > 0:
            # 按嚴重程度統計
            severity_counts = alerts_df.groupBy("severity").count().collect()
            for row in severity_counts:
                alert_counts[row['severity']] = row['count']
            
            # 按類型統計
            type_counts = alerts_df.groupBy("alert_type").count().collect()
            for row in type_counts:
                alert_counts[row['alert_type']] = row['count']
            
            # 記錄警報歷史
            self.alert_history.append({
                'timestamp': datetime.now(),
                'total_alerts': alerts_df.count(),
                'severity_breakdown': dict(severity_counts),
                'type_breakdown': dict(type_counts)
            })
        
        return alert_counts
    
    def send_alert_notification(self, alert_summary: Dict[str, int]) -> None:
        """
        發送警報通知
        
        Args:
            alert_summary: 警報摘要
        """
        # 這裡可以集成實際的通知服務
        # 如 Email, Slack, PagerDuty 等
        
        if alert_summary:
            print(f"🚨 警報通知: {alert_summary}")
            
            # 範例：發送 Email 警報
            # if self.config.email_alerts:
            #     self.send_email_alert(alert_summary)


class AnomalyDetector:
    """異常檢測器"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.models = {}
    
    def detect_statistical_anomalies(self, metrics_df: DataFrame) -> DataFrame:
        """
        使用統計方法檢測異常
        
        Args:
            metrics_df: 指標資料
            
        Returns:
            DataFrame: 異常檢測結果
        """
        print("正在進行統計異常檢測...")
        
        # 計算移動平均和標準差
        window_spec = Window.partitionBy("server_id", "service_name") \
                           .orderBy("timestamp") \
                           .rowsBetween(-5, 0)
        
        with_stats = metrics_df.withColumn(
            "cpu_moving_avg", avg("cpu_usage").over(window_spec)
        ).withColumn(
            "cpu_moving_std", stddev("cpu_usage").over(window_spec)
        ).withColumn(
            "memory_moving_avg", avg("memory_usage").over(window_spec)
        ).withColumn(
            "memory_moving_std", stddev("memory_usage").over(window_spec)
        ).withColumn(
            "response_time_moving_avg", avg("response_time").over(window_spec)
        ).withColumn(
            "response_time_moving_std", stddev("response_time").over(window_spec)
        )
        
        # 檢測異常（超過 2 個標準差）
        anomalies = with_stats.filter(
            (abs(col("cpu_usage") - col("cpu_moving_avg")) > 2 * col("cpu_moving_std")) |
            (abs(col("memory_usage") - col("memory_moving_avg")) > 2 * col("memory_moving_std")) |
            (abs(col("response_time") - col("response_time_moving_avg")) > 2 * col("response_time_moving_std"))
        ).withColumn("anomaly_type", lit("statistical")) \
         .withColumn("anomaly_score", 
                    greatest(
                        abs(col("cpu_usage") - col("cpu_moving_avg")) / col("cpu_moving_std"),
                        abs(col("memory_usage") - col("memory_moving_avg")) / col("memory_moving_std"),
                        abs(col("response_time") - col("response_time_moving_avg")) / col("response_time_moving_std")
                    ))
        
        return anomalies
    
    def detect_ml_anomalies(self, metrics_df: DataFrame) -> DataFrame:
        """
        使用機器學習方法檢測異常
        
        Args:
            metrics_df: 指標資料
            
        Returns:
            DataFrame: 異常檢測結果
        """
        print("正在進行機器學習異常檢測...")
        
        # 特徵工程
        feature_cols = ["cpu_usage", "memory_usage", "disk_usage", 
                       "network_latency", "error_rate", "response_time"]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
        feature_df = assembler.transform(metrics_df)
        
        # 使用 K-means 聚類進行異常檢測
        kmeans = KMeans(k=3, featuresCol="features", predictionCol="cluster")
        
        try:
            model = kmeans.fit(feature_df)
            predictions = model.transform(feature_df)
            
            # 計算到聚類中心的距離
            def calculate_distance(features, centers):
                # 這裡簡化處理，實際應該計算到各聚類中心的距離
                return 1.0  # 占位符
            
            # 標記異常（距離聚類中心較遠的點）
            anomalies = predictions.filter(col("cluster") == 0)  # 簡化處理
            
            # 添加異常資訊
            anomalies = anomalies.withColumn("anomaly_type", lit("ml_clustering")) \
                               .withColumn("anomaly_score", lit(1.0))
            
            return anomalies
            
        except Exception as e:
            print(f"機器學習異常檢測失敗: {str(e)}")
            return metrics_df.limit(0)  # 返回空 DataFrame
    
    def detect_pattern_anomalies(self, metrics_df: DataFrame) -> DataFrame:
        """
        檢測模式異常
        
        Args:
            metrics_df: 指標資料
            
        Returns:
            DataFrame: 模式異常檢測結果
        """
        print("正在進行模式異常檢測...")
        
        # 檢測同一服務器多個指標同時異常
        threshold_conditions = [
            col("cpu_usage") > 80,
            col("memory_usage") > 80,
            col("disk_usage") > 80,
            col("network_latency") > 100,
            col("error_rate") > 5,
            col("response_time") > 1000
        ]
        
        # 計算異常指標數量
        anomaly_count = metrics_df.withColumn(
            "anomaly_count",
            sum([when(condition, 1).otherwise(0) for condition in threshold_conditions])
        )
        
        # 多個指標同時異常視為模式異常
        pattern_anomalies = anomaly_count.filter(col("anomaly_count") >= 3) \
                                       .withColumn("anomaly_type", lit("pattern")) \
                                       .withColumn("anomaly_score", col("anomaly_count") / 6.0)
        
        return pattern_anomalies


class MonitoringSystem:
    """監控系統主類"""
    
    def __init__(self, config: MonitoringConfig, spark: SparkSession):
        self.config = config
        self.spark = spark
        self.metrics_collector = MetricsCollector(spark)
        self.alert_manager = AlertManager(config.alert_config)
        self.anomaly_detector = AnomalyDetector(spark)
        
        # 設定日誌
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
    
    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """
        執行一個監控週期
        
        Returns:
            Dict[str, Any]: 監控結果
        """
        self.logger.info("開始監控週期")
        
        # 載入指標資料
        if os.path.exists(self.config.input_path):
            metrics_df = self.metrics_collector.load_metrics(self.config.input_path)
        else:
            # 使用範例資料
            metrics_df = self.metrics_collector.generate_sample_metrics()
        
        if metrics_df is None:
            self.logger.error("無法載入指標資料")
            return {}
        
        # 檢查警報
        alerts_df = self.alert_manager.check_thresholds(metrics_df)
        alert_summary = self.alert_manager.process_alerts(alerts_df)
        
        # 異常檢測
        anomalies = {}
        if self.config.enable_ml_detection:
            anomalies['statistical'] = self.anomaly_detector.detect_statistical_anomalies(metrics_df)
            anomalies['ml'] = self.anomaly_detector.detect_ml_anomalies(metrics_df)
            anomalies['pattern'] = self.anomaly_detector.detect_pattern_anomalies(metrics_df)
        
        # 生成報告
        report = self.generate_monitoring_report(metrics_df, alerts_df, anomalies)
        
        # 發送警報
        if alert_summary:
            self.alert_manager.send_alert_notification(alert_summary)
        
        self.logger.info("監控週期完成")
        return report
    
    def generate_monitoring_report(self, metrics_df: DataFrame, 
                                 alerts_df: DataFrame, 
                                 anomalies: Dict[str, DataFrame]) -> Dict[str, Any]:
        """
        生成監控報告
        
        Args:
            metrics_df: 指標資料
            alerts_df: 警報資料
            anomalies: 異常檢測結果
            
        Returns:
            Dict[str, Any]: 監控報告
        """
        self.logger.info("生成監控報告")
        
        # 基本統計
        total_metrics = metrics_df.count()
        total_alerts = alerts_df.count()
        unique_servers = metrics_df.select("server_id").distinct().count()
        
        # 計算平均指標
        avg_metrics = metrics_df.agg(
            avg("cpu_usage").alias("avg_cpu"),
            avg("memory_usage").alias("avg_memory"),
            avg("disk_usage").alias("avg_disk"),
            avg("network_latency").alias("avg_latency"),
            avg("error_rate").alias("avg_error_rate"),
            avg("response_time").alias("avg_response_time")
        ).collect()[0]
        
        # 系統健康度評分
        health_score = self.calculate_health_score(avg_metrics)
        
        # 異常統計
        anomaly_counts = {}
        for anomaly_type, anomaly_df in anomalies.items():
            anomaly_counts[anomaly_type] = anomaly_df.count()
        
        # 趨勢分析
        trends = self.analyze_trends(metrics_df)
        
        report = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_metrics": total_metrics,
                "total_alerts": total_alerts,
                "unique_servers": unique_servers,
                "health_score": health_score,
                "status": "healthy" if health_score > 80 else "warning" if health_score > 60 else "critical"
            },
            "metrics": {
                "avg_cpu": float(avg_metrics['avg_cpu']),
                "avg_memory": float(avg_metrics['avg_memory']),
                "avg_disk": float(avg_metrics['avg_disk']),
                "avg_latency": float(avg_metrics['avg_latency']),
                "avg_error_rate": float(avg_metrics['avg_error_rate']),
                "avg_response_time": float(avg_metrics['avg_response_time'])
            },
            "anomalies": anomaly_counts,
            "trends": trends,
            "recommendations": self.generate_recommendations(avg_metrics, alert_summary)
        }
        
        # 保存報告
        self.save_report(report)
        
        return report
    
    def calculate_health_score(self, avg_metrics) -> float:
        """
        計算系統健康度評分
        
        Args:
            avg_metrics: 平均指標
            
        Returns:
            float: 健康度評分 (0-100)
        """
        cpu_score = max(0, 100 - avg_metrics['avg_cpu'])
        memory_score = max(0, 100 - avg_metrics['avg_memory'])
        disk_score = max(0, 100 - avg_metrics['avg_disk'])
        latency_score = max(0, 100 - avg_metrics['avg_latency'])
        error_score = max(0, 100 - avg_metrics['avg_error_rate'] * 10)
        response_score = max(0, 100 - avg_metrics['avg_response_time'] / 10)
        
        return (cpu_score + memory_score + disk_score + latency_score + error_score + response_score) / 6
    
    def analyze_trends(self, metrics_df: DataFrame) -> Dict[str, Any]:
        """
        分析趨勢
        
        Args:
            metrics_df: 指標資料
            
        Returns:
            Dict[str, Any]: 趨勢分析結果
        """
        # 按小時分析趨勢
        hourly_trends = metrics_df.withColumn("hour", hour("timestamp")) \
                                 .groupBy("hour") \
                                 .agg(
                                     avg("cpu_usage").alias("avg_cpu"),
                                     avg("memory_usage").alias("avg_memory"),
                                     avg("response_time").alias("avg_response_time"),
                                     count("*").alias("metric_count")
                                 ) \
                                 .orderBy("hour")
        
        trends_data = hourly_trends.collect()
        
        # 計算趨勢方向
        cpu_trend = "stable"
        memory_trend = "stable"
        response_time_trend = "stable"
        
        if len(trends_data) >= 2:
            cpu_values = [row['avg_cpu'] for row in trends_data]
            memory_values = [row['avg_memory'] for row in trends_data]
            response_time_values = [row['avg_response_time'] for row in trends_data]
            
            # 簡單的趨勢計算
            if cpu_values[-1] > cpu_values[0] * 1.1:
                cpu_trend = "increasing"
            elif cpu_values[-1] < cpu_values[0] * 0.9:
                cpu_trend = "decreasing"
            
            if memory_values[-1] > memory_values[0] * 1.1:
                memory_trend = "increasing"
            elif memory_values[-1] < memory_values[0] * 0.9:
                memory_trend = "decreasing"
            
            if response_time_values[-1] > response_time_values[0] * 1.1:
                response_time_trend = "increasing"
            elif response_time_values[-1] < response_time_values[0] * 0.9:
                response_time_trend = "decreasing"
        
        return {
            "cpu_trend": cpu_trend,
            "memory_trend": memory_trend,
            "response_time_trend": response_time_trend,
            "hourly_data": [row.asDict() for row in trends_data]
        }
    
    def generate_recommendations(self, avg_metrics, alert_summary) -> List[str]:
        """
        生成建議
        
        Args:
            avg_metrics: 平均指標
            alert_summary: 警報摘要
            
        Returns:
            List[str]: 建議列表
        """
        recommendations = []
        
        if avg_metrics['avg_cpu'] > 70:
            recommendations.append("CPU 使用率偏高，建議檢查程式效能或增加資源")
        
        if avg_metrics['avg_memory'] > 80:
            recommendations.append("記憶體使用率偏高，建議檢查記憶體洩漏或增加記憶體")
        
        if avg_metrics['avg_disk'] > 85:
            recommendations.append("磁碟使用率偏高，建議清理磁碟或擴展儲存")
        
        if avg_metrics['avg_latency'] > 50:
            recommendations.append("網路延遲較高，建議檢查網路配置")
        
        if avg_metrics['avg_error_rate'] > 3:
            recommendations.append("錯誤率偏高，建議檢查應用程式日誌")
        
        if avg_metrics['avg_response_time'] > 500:
            recommendations.append("響應時間較慢，建議優化應用程式或資料庫查詢")
        
        if alert_summary and alert_summary.get('critical', 0) > 0:
            recommendations.append("檢測到嚴重警報，建議立即處理")
        
        return recommendations
    
    def save_report(self, report: Dict[str, Any]) -> None:
        """
        保存報告
        
        Args:
            report: 監控報告
        """
        os.makedirs(self.config.output_path, exist_ok=True)
        
        # 保存 JSON 報告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(self.config.output_path, f"monitoring_report_{timestamp}.json")
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"監控報告已保存至: {report_path}")
    
    def generate_dashboard(self, metrics_df: DataFrame) -> None:
        """
        生成監控儀表板
        
        Args:
            metrics_df: 指標資料
        """
        self.logger.info("生成監控儀表板")
        
        # 按服務分組分析
        service_metrics = metrics_df.groupBy("service_name").agg(
            avg("cpu_usage").alias("avg_cpu"),
            avg("memory_usage").alias("avg_memory"),
            avg("disk_usage").alias("avg_disk"),
            avg("network_latency").alias("avg_latency"),
            avg("error_rate").alias("avg_error_rate"),
            avg("response_time").alias("avg_response_time")
        ).toPandas()
        
        # 時間趨勢分析
        hourly_metrics = metrics_df.withColumn("hour", hour("timestamp")) \
                                  .groupBy("hour") \
                                  .agg(
                                      avg("cpu_usage").alias("avg_cpu"),
                                      avg("memory_usage").alias("avg_memory"),
                                      avg("response_time").alias("avg_response_time")
                                  ) \
                                  .orderBy("hour") \
                                  .toPandas()
        
        # 生成圖表
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        
        # 各服務 CPU 使用率
        axes[0, 0].bar(service_metrics['service_name'], service_metrics['avg_cpu'])
        axes[0, 0].set_title('各服務平均 CPU 使用率')
        axes[0, 0].set_ylabel('CPU 使用率 (%)')
        axes[0, 0].tick_params(axis='x', rotation=45)
        
        # 各服務記憶體使用率
        axes[0, 1].bar(service_metrics['service_name'], service_metrics['avg_memory'], color='orange')
        axes[0, 1].set_title('各服務平均記憶體使用率')
        axes[0, 1].set_ylabel('記憶體使用率 (%)')
        axes[0, 1].tick_params(axis='x', rotation=45)
        
        # 各服務響應時間
        axes[0, 2].bar(service_metrics['service_name'], service_metrics['avg_response_time'], color='green')
        axes[0, 2].set_title('各服務平均響應時間')
        axes[0, 2].set_ylabel('響應時間 (ms)')
        axes[0, 2].tick_params(axis='x', rotation=45)
        
        # 每小時 CPU 趨勢
        axes[1, 0].plot(hourly_metrics['hour'], hourly_metrics['avg_cpu'], marker='o')
        axes[1, 0].set_title('每小時 CPU 使用率趨勢')
        axes[1, 0].set_xlabel('小時')
        axes[1, 0].set_ylabel('CPU 使用率 (%)')
        axes[1, 0].grid(True)
        
        # 每小時記憶體趨勢
        axes[1, 1].plot(hourly_metrics['hour'], hourly_metrics['avg_memory'], marker='o', color='orange')
        axes[1, 1].set_title('每小時記憶體使用率趨勢')
        axes[1, 1].set_xlabel('小時')
        axes[1, 1].set_ylabel('記憶體使用率 (%)')
        axes[1, 1].grid(True)
        
        # 每小時響應時間趨勢
        axes[1, 2].plot(hourly_metrics['hour'], hourly_metrics['avg_response_time'], marker='o', color='green')
        axes[1, 2].set_title('每小時響應時間趨勢')
        axes[1, 2].set_xlabel('小時')
        axes[1, 2].set_ylabel('響應時間 (ms)')
        axes[1, 2].grid(True)
        
        plt.tight_layout()
        
        # 保存圖表
        dashboard_path = os.path.join(self.config.output_path, 'monitoring_dashboard.png')
        plt.savefig(dashboard_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        self.logger.info(f"監控儀表板已保存至: {dashboard_path}")
    
    def run_continuous_monitoring(self) -> None:
        """
        執行持續監控
        """
        self.logger.info("開始持續監控")
        
        try:
            while True:
                report = self.run_monitoring_cycle()
                
                # 生成儀表板
                if os.path.exists(self.config.input_path):
                    metrics_df = self.metrics_collector.load_metrics(self.config.input_path)
                else:
                    metrics_df = self.metrics_collector.generate_sample_metrics()
                
                if metrics_df is not None:
                    self.generate_dashboard(metrics_df)
                
                # 等待下一個監控週期
                time.sleep(self.config.monitoring_interval)
                
        except KeyboardInterrupt:
            self.logger.info("監控系統已停止")
        except Exception as e:
            self.logger.error(f"監控系統發生錯誤: {str(e)}")


def load_config(config_path: str) -> MonitoringConfig:
    """
    載入配置文件
    
    Args:
        config_path: 配置文件路徑
        
    Returns:
        MonitoringConfig: 監控配置
    """
    if os.path.exists(config_path):
        with open(config_path, 'r', encoding='utf-8') as f:
            config_data = json.load(f)
        
        alert_config = AlertConfig(**config_data.get('alert_config', {}))
        
        return MonitoringConfig(
            input_path=config_data.get('input_path', ''),
            output_path=config_data.get('output_path', './monitoring_output'),
            alert_config=alert_config,
            monitoring_interval=config_data.get('monitoring_interval', 60),
            retention_days=config_data.get('retention_days', 30),
            enable_ml_detection=config_data.get('enable_ml_detection', True),
            email_alerts=config_data.get('email_alerts', False),
            email_recipients=config_data.get('email_recipients', [])
        )
    else:
        # 使用預設配置
        return MonitoringConfig(
            input_path='',
            output_path='./monitoring_output',
            alert_config=AlertConfig()
        )


def main():
    """主函數"""
    parser = argparse.ArgumentParser(description='即時監控系統')
    parser.add_argument('--config', default='config.json', help='配置文件路徑')
    parser.add_argument('--mode', choices=['single', 'continuous'], default='single', 
                       help='執行模式：single(單次執行) 或 continuous(持續監控)')
    parser.add_argument('--app-name', default='MonitoringSystem', help='Spark 應用名稱')
    
    args = parser.parse_args()
    
    # 載入配置
    config = load_config(args.config)
    
    # 建立輸出目錄
    os.makedirs(config.output_path, exist_ok=True)
    
    # 建立 Spark 會話
    spark = SparkSession.builder \
        .appName(args.app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    try:
        # 建立監控系統
        monitoring_system = MonitoringSystem(config, spark)
        
        if args.mode == 'single':
            # 單次執行
            report = monitoring_system.run_monitoring_cycle()
            print(f"監控完成，報告已保存至: {config.output_path}")
        else:
            # 持續監控
            monitoring_system.run_continuous_monitoring()
    
    except Exception as e:
        print(f"錯誤: {str(e)}")
        sys.exit(1)
    
    finally:
        # 關閉 Spark 會話
        spark.stop()


if __name__ == "__main__":
    main()