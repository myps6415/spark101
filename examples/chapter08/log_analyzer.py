#!/usr/bin/env python3
"""
第8章：實戰項目 - 日誌分析系統
構建一個完整的日誌分析系統，展示 Spark 在實際項目中的應用
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import (
    col, regexp_extract, to_timestamp, date_format, hour, dayofweek,
    count, sum as spark_sum, avg, max as spark_max, min as spark_min,
    desc, asc, when, isnan, isnull, split, size, collect_list, window,
    current_timestamp, date_sub, lit, monotonically_increasing_id
)
import re
import tempfile
import os
import json
from datetime import datetime, timedelta
import random

class LogAnalyzer:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.temp_dir = tempfile.mkdtemp()
        print(f"臨時目錄: {self.temp_dir}")
        
    def generate_sample_logs(self, num_logs=10000):
        """生成示例日誌數據"""
        
        # 日誌模式配置
        ips = [f"192.168.{i}.{j}" for i in range(1, 11) for j in range(1, 26)]
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X)",
            "Mozilla/5.0 (Android 11; Mobile; rv:68.0) Gecko/68.0"
        ]
        
        methods = ["GET", "POST", "PUT", "DELETE"]
        paths = [
            "/", "/home", "/login", "/logout", "/api/users", "/api/products",
            "/api/orders", "/search", "/profile", "/settings", "/admin",
            "/static/css/style.css", "/static/js/app.js", "/favicon.ico"
        ]
        
        status_codes = [200, 200, 200, 200, 200, 301, 302, 400, 401, 403, 404, 500, 502, 503]
        
        logs = []
        base_time = datetime.now() - timedelta(days=7)
        
        for i in range(num_logs):
            # 生成時間戳（過去7天內）
            timestamp = base_time + timedelta(
                days=random.randint(0, 6),
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59),
                seconds=random.randint(0, 59)
            )
            
            # 生成日誌字段
            ip = random.choice(ips)
            method = random.choice(methods)
            path = random.choice(paths)
            status = random.choice(status_codes)
            size = random.randint(100, 10000)
            user_agent = random.choice(user_agents)
            
            # 生成日誌行（Common Log Format）
            log_line = f'{ip} - - [{timestamp.strftime("%d/%b/%Y:%H:%M:%S %z")}] "{method} {path} HTTP/1.1" {status} {size} "-" "{user_agent}"'
            logs.append(log_line)
        
        # 寫入日誌文件
        log_file = os.path.join(self.temp_dir, "access.log")
        with open(log_file, 'w') as f:
            for log in logs:
                f.write(log + '\n')
        
        print(f"生成了 {num_logs} 條日誌記錄")
        return log_file
    
    def parse_logs(self, log_file):
        """解析日誌文件"""
        
        # 讀取原始日誌
        raw_logs = self.spark.read.text(log_file)
        
        print("原始日誌樣本:")
        raw_logs.show(5, truncate=False)
        
        # 定義日誌格式正則表達式 (Common Log Format)
        log_pattern = r'^(\S+) \S+ \S+ \[(.*?)\] "(\S+) (\S+) \S+" (\d+) (\d+) "([^"]*)" "([^"]*)"'
        
        # 解析日誌
        parsed_logs = raw_logs.select(
            regexp_extract(col("value"), log_pattern, 1).alias("ip"),
            regexp_extract(col("value"), log_pattern, 2).alias("timestamp_str"),
            regexp_extract(col("value"), log_pattern, 3).alias("method"),
            regexp_extract(col("value"), log_pattern, 4).alias("path"),
            regexp_extract(col("value"), log_pattern, 5).cast(IntegerType()).alias("status"),
            regexp_extract(col("value"), log_pattern, 6).cast(IntegerType()).alias("size"),
            regexp_extract(col("value"), log_pattern, 7).alias("referer"),
            regexp_extract(col("value"), log_pattern, 8).alias("user_agent")
        ).filter(col("ip") != "")  # 過濾解析失敗的記錄
        
        # 轉換時間戳
        parsed_logs = parsed_logs.withColumn(
            "timestamp",
            to_timestamp(col("timestamp_str"), "dd/MMM/yyyy:HH:mm:ss Z")
        )
        
        # 添加時間維度
        parsed_logs = parsed_logs.withColumn("hour", hour(col("timestamp"))) \
                                 .withColumn("day_of_week", dayofweek(col("timestamp"))) \
                                 .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))
        
        print("解析後的日誌:")
        parsed_logs.show(5, truncate=False)
        parsed_logs.printSchema()
        
        return parsed_logs
    
    def data_quality_check(self, df):
        """數據品質檢查"""
        
        print("\n📊 數據品質檢查")
        print("=" * 30)
        
        # 基本統計
        total_records = df.count()
        print(f"總記錄數: {total_records}")
        
        # 檢查空值
        null_counts = df.select([
            col(c).isNull().cast("int").alias(c) for c in df.columns
        ]).agg(*[
            spark_sum(col(c)).alias(f"{c}_nulls") for c in df.columns
        ]).collect()[0]
        
        print("\n空值統計:")
        for col_name in df.columns:
            null_count = null_counts[f"{col_name}_nulls"]
            if null_count > 0:
                print(f"  {col_name}: {null_count} ({null_count/total_records*100:.2f}%)")
        
        # 檢查異常值
        print("\n異常值檢查:")
        
        # 狀態碼分布
        status_dist = df.groupBy("status").count().orderBy(desc("count"))
        print("狀態碼分布:")
        status_dist.show()
        
        # 異常狀態碼
        error_logs = df.filter(col("status") >= 400)
        error_count = error_logs.count()
        print(f"錯誤日誌數量: {error_count} ({error_count/total_records*100:.2f}%)")
        
        # 請求大小異常
        size_stats = df.select("size").describe()
        print("請求大小統計:")
        size_stats.show()
        
        return df
    
    def basic_analysis(self, df):
        """基本分析"""
        
        print("\n📈 基本分析")
        print("=" * 20)
        
        # 1. 訪問量統計
        print("1. 訪問量統計")
        
        # 總訪問量
        total_requests = df.count()
        print(f"總請求數: {total_requests}")
        
        # 每日訪問量
        daily_requests = df.groupBy("date") \
                           .agg(count("*").alias("requests")) \
                           .orderBy("date")
        
        print("每日訪問量:")
        daily_requests.show()
        
        # 每小時訪問量
        hourly_requests = df.groupBy("hour") \
                            .agg(count("*").alias("requests")) \
                            .orderBy("hour")
        
        print("每小時訪問量:")
        hourly_requests.show()
        
        # 2. 熱門頁面
        print("\n2. 熱門頁面")
        
        top_pages = df.groupBy("path") \
                      .agg(count("*").alias("requests")) \
                      .orderBy(desc("requests"))
        
        print("熱門頁面 Top 10:")
        top_pages.show(10)
        
        # 3. 用戶行為分析
        print("\n3. 用戶行為分析")
        
        # 獨立訪客
        unique_visitors = df.select("ip").distinct().count()
        print(f"獨立訪客數: {unique_visitors}")
        
        # 最活躍用戶
        top_users = df.groupBy("ip") \
                      .agg(count("*").alias("requests")) \
                      .orderBy(desc("requests"))
        
        print("最活躍用戶 Top 10:")
        top_users.show(10)
        
        # 4. 瀏覽器分析
        print("\n4. 瀏覽器分析")
        
        # 提取瀏覽器信息
        browser_df = df.withColumn("browser", 
                                  when(col("user_agent").contains("Chrome"), "Chrome")
                                  .when(col("user_agent").contains("Firefox"), "Firefox")
                                  .when(col("user_agent").contains("Safari"), "Safari")
                                  .when(col("user_agent").contains("Edge"), "Edge")
                                  .otherwise("Other"))
        
        browser_stats = browser_df.groupBy("browser") \
                                  .agg(count("*").alias("requests")) \
                                  .orderBy(desc("requests"))
        
        print("瀏覽器分布:")
        browser_stats.show()
        
        return df
    
    def advanced_analysis(self, df):
        """進階分析"""
        
        print("\n🔍 進階分析")
        print("=" * 20)
        
        # 1. 錯誤分析
        print("1. 錯誤分析")
        
        # 錯誤日誌統計
        error_analysis = df.filter(col("status") >= 400) \
                           .groupBy("status", "path") \
                           .agg(count("*").alias("error_count")) \
                           .orderBy(desc("error_count"))
        
        print("錯誤統計:")
        error_analysis.show(20)
        
        # 404 錯誤分析
        not_found = df.filter(col("status") == 404) \
                      .groupBy("path") \
                      .agg(count("*").alias("not_found_count")) \
                      .orderBy(desc("not_found_count"))
        
        print("404 錯誤頁面:")
        not_found.show(10)
        
        # 2. 性能分析
        print("\n2. 性能分析")
        
        # 響應大小統計
        size_analysis = df.groupBy("status") \
                          .agg(avg("size").alias("avg_size"),
                               spark_max("size").alias("max_size"),
                               spark_min("size").alias("min_size")) \
                          .orderBy("status")
        
        print("響應大小統計:")
        size_analysis.show()
        
        # 大請求分析
        large_requests = df.filter(col("size") > 5000) \
                           .groupBy("path") \
                           .agg(count("*").alias("large_request_count"),
                                avg("size").alias("avg_size")) \
                           .orderBy(desc("large_request_count"))
        
        print("大請求分析:")
        large_requests.show(10)
        
        # 3. 時間序列分析
        print("\n3. 時間序列分析")
        
        # 按小時的流量模式
        traffic_pattern = df.groupBy("hour", "day_of_week") \
                            .agg(count("*").alias("requests")) \
                            .orderBy("day_of_week", "hour")
        
        print("流量模式（按小時和星期）:")
        traffic_pattern.show(50)
        
        # 4. 用戶會話分析
        print("\n4. 用戶會話分析")
        
        # 用戶會話統計（簡化版）
        user_sessions = df.groupBy("ip") \
                          .agg(count("*").alias("page_views"),
                               spark_max("timestamp").alias("last_visit"),
                               spark_min("timestamp").alias("first_visit")) \
                          .withColumn("session_duration", 
                                    col("last_visit").cast("long") - col("first_visit").cast("long"))
        
        print("用戶會話統計:")
        user_sessions.show(10)
        
        return df
    
    def real_time_monitoring(self, df):
        """實時監控指標"""
        
        print("\n📊 實時監控指標")
        print("=" * 25)
        
        # 1. 關鍵指標
        metrics = df.agg(
            count("*").alias("total_requests"),
            (spark_sum(when(col("status") >= 400, 1).otherwise(0)) * 100.0 / count("*")).alias("error_rate"),
            (spark_sum(when(col("status") >= 500, 1).otherwise(0)) * 100.0 / count("*")).alias("server_error_rate"),
            avg("size").alias("avg_response_size"),
            spark_max("size").alias("max_response_size")
        )
        
        print("關鍵指標:")
        metrics.show()
        
        # 2. 異常檢測
        print("\n異常檢測:")
        
        # 高錯誤率頁面
        high_error_pages = df.groupBy("path") \
                             .agg(count("*").alias("total_requests"),
                                  spark_sum(when(col("status") >= 400, 1).otherwise(0)).alias("error_requests")) \
                             .withColumn("error_rate", col("error_requests") * 100.0 / col("total_requests")) \
                             .filter(col("error_rate") > 10) \
                             .orderBy(desc("error_rate"))
        
        print("高錯誤率頁面:")
        high_error_pages.show(10)
        
        # 3. 安全分析
        print("\n安全分析:")
        
        # 可疑IP（高頻訪問）
        suspicious_ips = df.groupBy("ip") \
                           .agg(count("*").alias("requests")) \
                           .filter(col("requests") > 100) \
                           .orderBy(desc("requests"))
        
        print("可疑 IP（高頻訪問）:")
        suspicious_ips.show(10)
        
        # 攻擊模式檢測
        attack_patterns = df.filter(
            col("path").rlike(r".*(\.\.|script|sql|union|select|drop|delete|insert|update).*") |
            col("status").isin(401, 403)
        ).groupBy("ip", "path") \
         .agg(count("*").alias("attack_attempts")) \
         .orderBy(desc("attack_attempts"))
        
        print("潛在攻擊模式:")
        attack_patterns.show(10)
        
        return df
    
    def create_dashboard_data(self, df):
        """創建儀表板數據"""
        
        print("\n📊 儀表板數據")
        print("=" * 20)
        
        # 1. 概覽指標
        overview = df.agg(
            count("*").alias("total_requests"),
            col("ip").countDistinct().alias("unique_visitors"),
            (spark_sum(when(col("status") >= 400, 1).otherwise(0)) * 100.0 / count("*")).alias("error_rate"),
            avg("size").alias("avg_response_size")
        )
        
        print("概覽指標:")
        overview.show()
        
        # 2. 時間序列數據
        time_series = df.groupBy("date", "hour") \
                        .agg(count("*").alias("requests"),
                             spark_sum(when(col("status") >= 400, 1).otherwise(0)).alias("errors")) \
                        .orderBy("date", "hour")
        
        print("時間序列數據:")
        time_series.show(20)
        
        # 3. 導出數據
        dashboard_path = os.path.join(self.temp_dir, "dashboard_data")
        
        # 導出各種格式的數據
        time_series.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{dashboard_path}/time_series")
        overview.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{dashboard_path}/overview")
        
        print(f"儀表板數據已導出到: {dashboard_path}")
        
        return df
    
    def generate_report(self, df):
        """生成分析報告"""
        
        print("\n📄 分析報告")
        print("=" * 20)
        
        # 收集關鍵指標
        total_requests = df.count()
        unique_visitors = df.select("ip").distinct().count()
        error_rate = df.filter(col("status") >= 400).count() / total_requests * 100
        
        # 熱門頁面
        top_pages = df.groupBy("path") \
                      .agg(count("*").alias("requests")) \
                      .orderBy(desc("requests")) \
                      .limit(5) \
                      .collect()
        
        # 時間範圍
        time_range = df.select(
            spark_min("timestamp").alias("start_time"),
            spark_max("timestamp").alias("end_time")
        ).collect()[0]
        
        # 生成報告
        report = {
            "summary": {
                "total_requests": total_requests,
                "unique_visitors": unique_visitors,
                "error_rate": round(error_rate, 2),
                "analysis_period": {
                    "start": str(time_range["start_time"]),
                    "end": str(time_range["end_time"])
                }
            },
            "top_pages": [
                {"path": row["path"], "requests": row["requests"]} 
                for row in top_pages
            ],
            "recommendations": [
                "監控錯誤率較高的頁面",
                "優化響應時間較長的請求",
                "加強安全防護機制",
                "考慮實施CDN以減少伺服器負載"
            ]
        }
        
        # 儲存報告
        report_file = os.path.join(self.temp_dir, "analysis_report.json")
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        
        print(f"分析報告已生成: {report_file}")
        
        # 顯示報告摘要
        print("\n報告摘要:")
        print(f"  分析期間: {report['summary']['analysis_period']['start']} 至 {report['summary']['analysis_period']['end']}")
        print(f"  總請求數: {report['summary']['total_requests']:,}")
        print(f"  獨立訪客: {report['summary']['unique_visitors']:,}")
        print(f"  錯誤率: {report['summary']['error_rate']}%")
        print(f"  熱門頁面: {', '.join([page['path'] for page in report['top_pages'][:3]])}")
        
        return report
    
    def cleanup(self):
        """清理臨時文件"""
        import shutil
        shutil.rmtree(self.temp_dir)
        print(f"清理臨時目錄: {self.temp_dir}")

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("Log Analyzer") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("📊 日誌分析系統")
    print("=" * 40)
    
    # 初始化分析器
    analyzer = LogAnalyzer(spark)
    
    try:
        # 1. 生成示例數據
        print("\n1️⃣ 生成示例數據")
        log_file = analyzer.generate_sample_logs(50000)
        
        # 2. 解析日誌
        print("\n2️⃣ 解析日誌")
        parsed_logs = analyzer.parse_logs(log_file)
        
        # 3. 數據品質檢查
        parsed_logs = analyzer.data_quality_check(parsed_logs)
        
        # 4. 基本分析
        parsed_logs = analyzer.basic_analysis(parsed_logs)
        
        # 5. 進階分析
        parsed_logs = analyzer.advanced_analysis(parsed_logs)
        
        # 6. 實時監控
        parsed_logs = analyzer.real_time_monitoring(parsed_logs)
        
        # 7. 創建儀表板數據
        parsed_logs = analyzer.create_dashboard_data(parsed_logs)
        
        # 8. 生成報告
        report = analyzer.generate_report(parsed_logs)
        
        print("\n✅ 日誌分析完成")
        
        # 9. 展示部署建議
        print("\n🚀 部署建議")
        print("=" * 15)
        
        deployment_tips = [
            "1. 使用流式處理處理實時日誌",
            "2. 設置適當的分區策略以優化性能",
            "3. 實施數據保留策略",
            "4. 設置監控和警報機制",
            "5. 考慮使用 Elasticsearch 作為搜索引擎",
            "6. 實施數據安全和隱私保護措施",
            "7. 定期進行性能調優"
        ]
        
        for tip in deployment_tips:
            print(f"  {tip}")
            
    finally:
        # 10. 清理資源
        analyzer.cleanup()
        spark.stop()

if __name__ == "__main__":
    main()