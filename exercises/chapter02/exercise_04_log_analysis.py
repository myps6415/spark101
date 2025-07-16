#!/usr/bin/env python3
"""
第2章練習4：日誌分析
使用 RDD 進行服務器日誌分析練習
"""

import re
from datetime import datetime

from pyspark.sql import SparkSession


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("RDD日誌分析練習").master("local[*]").getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("WARN")

    print("=== 第2章練習4：RDD 日誌分析 ===")

    # 1. 創建模擬服務器日誌數據
    print("\n1. 創建模擬服務器日誌數據:")

    log_data = [
        '192.168.1.10 - - [15/Jan/2024:10:05:12 +0000] "GET /api/users HTTP/1.1" 200 1234',
        '192.168.1.11 - - [15/Jan/2024:10:05:15 +0000] "POST /api/login HTTP/1.1" 200 567',
        '192.168.1.12 - - [15/Jan/2024:10:05:18 +0000] "GET /api/products HTTP/1.1" 404 89',
        '192.168.1.10 - - [15/Jan/2024:10:05:21 +0000] "GET /api/orders HTTP/1.1" 200 2345',
        '192.168.1.13 - - [15/Jan/2024:10:05:24 +0000] "POST /api/register HTTP/1.1" 500 234',
        '192.168.1.11 - - [15/Jan/2024:10:05:27 +0000] "DELETE /api/users/123 HTTP/1.1" 200 45',
        '192.168.1.14 - - [15/Jan/2024:10:05:30 +0000] "GET /api/dashboard HTTP/1.1" 403 123',
        '192.168.1.12 - - [15/Jan/2024:10:05:33 +0000] "PUT /api/users/456 HTTP/1.1" 200 678',
        '192.168.1.15 - - [15/Jan/2024:10:05:36 +0000] "GET /static/css/style.css HTTP/1.1" 200 1023',
        '192.168.1.10 - - [15/Jan/2024:10:05:39 +0000] "GET /api/users HTTP/1.1" 200 1234',
        '192.168.1.16 - - [15/Jan/2024:10:05:42 +0000] "POST /api/payment HTTP/1.1" 500 345',
        '192.168.1.11 - - [15/Jan/2024:10:05:45 +0000] "GET /api/products/search HTTP/1.1" 200 4567',
        '192.168.1.17 - - [15/Jan/2024:10:05:48 +0000] "GET /api/invalid HTTP/1.1" 404 67',
        '192.168.1.12 - - [15/Jan/2024:10:05:51 +0000] "GET /api/orders HTTP/1.1" 200 2345',
        '192.168.1.18 - - [15/Jan/2024:10:05:54 +0000] "POST /api/login HTTP/1.1" 401 89',
        '192.168.1.13 - - [15/Jan/2024:10:05:57 +0000] "GET /api/users HTTP/1.1" 200 1234',
        '192.168.1.19 - - [15/Jan/2024:10:06:00 +0000] "GET /api/admin HTTP/1.1" 403 156',
        '192.168.1.14 - - [15/Jan/2024:10:06:03 +0000] "POST /api/logout HTTP/1.1" 200 23',
        '192.168.1.20 - - [15/Jan/2024:10:06:06 +0000] "GET /api/health HTTP/1.1" 200 45',
        '192.168.1.15 - - [15/Jan/2024:10:06:09 +0000] "GET /favicon.ico HTTP/1.1" 404 0',
    ]

    # 創建日誌 RDD
    log_rdd = sc.parallelize(log_data)

    print(f"總日誌條數: {log_rdd.count()}")
    print("前5條日誌:")
    for i, log in enumerate(log_rdd.take(5), 1):
        print(f"{i}. {log}")

    # 2. 日誌解析
    print("\n2. 日誌解析:")

    # 定義日誌解析函數
    def parse_log_line(line):
        # Apache Common Log Format 正則表達式
        pattern = r"^(\S+) \S+ \S+ \[([\w:/]+\s[+\-]\d{4})\] \"(\S+) (\S+) (\S+)\" (\d{3}) (\d+)$"
        match = re.match(pattern, line)

        if match:
            ip = match.group(1)
            timestamp = match.group(2)
            method = match.group(3)
            url = match.group(4)
            protocol = match.group(5)
            status_code = int(match.group(6))
            response_size = int(match.group(7))

            return {
                "ip": ip,
                "timestamp": timestamp,
                "method": method,
                "url": url,
                "protocol": protocol,
                "status_code": status_code,
                "response_size": response_size,
                "raw_line": line,
            }
        else:
            return None

    # 解析所有日誌行
    parsed_logs = log_rdd.map(parse_log_line).filter(lambda x: x is not None)

    print(f"成功解析的日誌條數: {parsed_logs.count()}")

    # 顯示解析結果示例
    print("\n解析後的日誌結構 (前3條):")
    for i, log in enumerate(parsed_logs.take(3), 1):
        print(
            f"{i}. IP: {log['ip']}, Method: {log['method']}, URL: {log['url']}, Status: {log['status_code']}"
        )

    # 3. 基本統計分析
    print("\n3. 基本統計分析:")

    # 3.1 狀態碼分布
    print("\n3.1 HTTP 狀態碼分布:")
    status_counts = (
        parsed_logs.map(lambda x: (x["status_code"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
    )

    for status, count in status_counts.collect():
        print(f"狀態碼 {status}: {count} 次")

    # 3.2 請求方法統計
    print("\n3.2 HTTP 請求方法統計:")
    method_counts = (
        parsed_logs.map(lambda x: (x["method"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    for method, count in method_counts.collect():
        print(f"{method}: {count} 次")

    # 3.3 響應大小統計
    print("\n3.3 響應大小統計:")
    response_sizes = parsed_logs.map(lambda x: x["response_size"])

    total_bytes = response_sizes.sum()
    avg_size = response_sizes.mean()
    max_size = response_sizes.max()
    min_size = response_sizes.min()

    print(f"總響應大小: {total_bytes:,} bytes")
    print(f"平均響應大小: {avg_size:.2f} bytes")
    print(f"最大響應大小: {max_size} bytes")
    print(f"最小響應大小: {min_size} bytes")

    # 4. IP 地址分析
    print("\n4. IP 地址分析:")

    # 4.1 訪問頻率最高的 IP
    print("\n4.1 訪問頻率最高的 IP 地址:")
    ip_counts = (
        parsed_logs.map(lambda x: (x["ip"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    top_ips = ip_counts.take(5)
    for ip, count in top_ips:
        print(f"{ip}: {count} 次訪問")

    # 4.2 每個 IP 的流量統計
    print("\n4.2 每個 IP 的流量統計:")
    ip_traffic = (
        parsed_logs.map(lambda x: (x["ip"], x["response_size"]))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    for ip, traffic in ip_traffic.take(5):
        print(f"{ip}: {traffic:,} bytes")

    # 5. URL 分析
    print("\n5. URL 分析:")

    # 5.1 最熱門的 URL
    print("\n5.1 最熱門的 URL:")
    url_counts = (
        parsed_logs.map(lambda x: (x["url"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    for url, count in url_counts.take(5):
        print(f"{url}: {count} 次")

    # 5.2 API 端點分析
    print("\n5.2 API 端點分析:")
    api_logs = parsed_logs.filter(lambda x: x["url"].startswith("/api/"))
    api_counts = (
        api_logs.map(lambda x: (x["url"], 1))
        .reduceByKey(lambda a, b: a + b)
        .sortBy(lambda x: x[1], ascending=False)
    )

    print("API 端點訪問統計:")
    for api, count in api_counts.collect():
        print(f"{api}: {count} 次")

    # 6. 錯誤分析
    print("\n6. 錯誤分析:")

    # 6.1 錯誤請求分析
    print("\n6.1 4xx 和 5xx 錯誤分析:")

    # 4xx 客戶端錯誤
    client_errors = parsed_logs.filter(lambda x: 400 <= x["status_code"] < 500)
    client_error_count = client_errors.count()

    # 5xx 服務器錯誤
    server_errors = parsed_logs.filter(lambda x: 500 <= x["status_code"] < 600)
    server_error_count = server_errors.count()

    total_requests = parsed_logs.count()

    print(
        f"4xx 客戶端錯誤: {client_error_count} 次 ({client_error_count/total_requests*100:.2f}%)"
    )
    print(
        f"5xx 服務器錯誤: {server_error_count} 次 ({server_error_count/total_requests*100:.2f}%)"
    )

    # 6.2 錯誤 URL 統計
    print("\n6.2 產生錯誤的 URL:")
    error_urls = (
        parsed_logs.filter(lambda x: x["status_code"] >= 400)
        .map(lambda x: (x["url"], x["status_code"]))
        .groupByKey()
        .mapValues(list)
    )

    for url, status_codes in error_urls.collect():
        print(f"{url}: {status_codes}")

    # 7. 時間分析
    print("\n7. 時間分析:")

    # 7.1 按分鐘統計請求量
    print("\n7.1 按分鐘統計請求量:")

    def extract_minute(timestamp_str):
        # 從時間戳中提取分鐘
        # 格式: 15/Jan/2024:10:05:12
        try:
            minute_part = timestamp_str.split(":")[1]  # 提取小時後的分鐘
            return minute_part
        except:
            return "unknown"

    minute_counts = (
        parsed_logs.map(lambda x: (extract_minute(x["timestamp"]), 1))
        .reduceByKey(lambda a, b: a + b)
        .sortByKey()
    )

    print("每分鐘請求量:")
    for minute, count in minute_counts.collect():
        print(f"第 {minute} 分鐘: {count} 個請求")

    # 8. 用戶行為分析
    print("\n8. 用戶行為分析:")

    # 8.1 用戶會話分析 (按 IP 分組)
    print("\n8.1 用戶會話分析:")

    def analyze_user_session(logs):
        logs_list = list(logs)
        session_info = {
            "request_count": len(logs_list),
            "unique_urls": len(set(log["url"] for log in logs_list)),
            "total_bytes": sum(log["response_size"] for log in logs_list),
            "error_count": len([log for log in logs_list if log["status_code"] >= 400]),
            "methods": list(set(log["method"] for log in logs_list)),
        }
        return session_info

    user_sessions = (
        parsed_logs.map(lambda x: (x["ip"], x))
        .groupByKey()
        .mapValues(analyze_user_session)
    )

    print("用戶會話統計 (前5個):")
    for ip, session in user_sessions.take(5):
        print(f"{ip}:")
        print(f"  - 請求次數: {session['request_count']}")
        print(f"  - 訪問頁面數: {session['unique_urls']}")
        print(f"  - 總流量: {session['total_bytes']} bytes")
        print(f"  - 錯誤次數: {session['error_count']}")
        print(f"  - 使用方法: {', '.join(session['methods'])}")

    # 9. 安全分析
    print("\n9. 安全分析:")

    # 9.1 可疑活動檢測
    print("\n9.1 可疑活動檢測:")

    # 檢測大量 404 錯誤 (可能是掃描攻擊)
    suspicious_404 = (
        parsed_logs.filter(lambda x: x["status_code"] == 404)
        .map(lambda x: (x["ip"], 1))
        .reduceByKey(lambda a, b: a + b)
        .filter(lambda x: x[1] >= 2)
    )  # 2次以上404錯誤

    print("可疑 404 掃描活動:")
    for ip, count in suspicious_404.collect():
        print(f"{ip}: {count} 次 404 錯誤")

    # 9.2 檢測異常大流量
    print("\n9.2 異常大流量檢測:")

    # 計算平均響應大小
    avg_response_size = parsed_logs.map(lambda x: x["response_size"]).mean()

    # 找出響應大小超過平均值5倍的請求
    large_responses = parsed_logs.filter(
        lambda x: x["response_size"] > avg_response_size * 5
    )

    print(f"平均響應大小: {avg_response_size:.2f} bytes")
    print("異常大響應:")
    for log in large_responses.collect():
        print(f"{log['ip']} -> {log['url']}: {log['response_size']} bytes")

    # 10. 性能指標
    print("\n10. 性能指標:")

    # 10.1 成功率統計
    success_requests = parsed_logs.filter(
        lambda x: 200 <= x["status_code"] < 300
    ).count()
    success_rate = success_requests / total_requests * 100

    print(f"總請求數: {total_requests}")
    print(f"成功請求數: {success_requests}")
    print(f"成功率: {success_rate:.2f}%")

    # 10.2 API 端點成功率
    print("\n10.2 API 端點成功率:")

    api_success_rates = (
        api_logs.map(
            lambda x: (x["url"], (1 if 200 <= x["status_code"] < 300 else 0, 1))
        )
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda x: (x[0] / x[1] * 100, x[1]))
    )

    for url, (success_rate, total) in api_success_rates.collect():
        print(f"{url}: {success_rate:.1f}% ({total} 次請求)")

    # 11. 日誌摘要報告
    print("\n11. 日誌摘要報告:")

    # 收集關鍵指標
    unique_ips = parsed_logs.map(lambda x: x["ip"]).distinct().count()
    unique_urls = parsed_logs.map(lambda x: x["url"]).distinct().count()
    peak_minute = minute_counts.max(key=lambda x: x[1])

    print("=== 日誌分析摘要 ===")
    print(f"- 分析時間範圍: 2024-01-15 10:05-10:06")
    print(f"- 總請求數: {total_requests}")
    print(f"- 唯一 IP 數: {unique_ips}")
    print(f"- 唯一 URL 數: {unique_urls}")
    print(f"- 總流量: {total_bytes:,} bytes")
    print(f"- 平均響應大小: {avg_size:.2f} bytes")
    print(f"- 成功率: {success_rate:.2f}%")
    print(f"- 客戶端錯誤: {client_error_count} 次")
    print(f"- 服務器錯誤: {server_error_count} 次")
    print(f"- 流量峰值: 第 {peak_minute[0]} 分鐘 ({peak_minute[1]} 個請求)")

    # 清理資源
    spark.stop()
    print("\n日誌分析練習完成！")


if __name__ == "__main__":
    main()
