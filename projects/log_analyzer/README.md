# 日誌分析系統

一個基於 Apache Spark 的網站日誌分析系統，用於分析網站訪問日誌、檢測安全威脅和異常行為。

## 功能特色

### 🔍 日誌解析
- 支援 Apache Common Log Format
- 自動解析 IP 地址、時間戳、請求方法、路徑、狀態碼等
- 智能處理解析失敗的記錄

### 🛡️ 安全威脅檢測
- **SQL 注入檢測**: 識別可能的 SQL 注入嘗試
- **目錄遍歷攻擊**: 檢測路徑遍歷攻擊模式
- **暴力破解檢測**: 識別對登入端點的暴力破解嘗試
- **異常高頻訪問**: 檢測可能的 DDoS 攻擊
- **敏感路徑監控**: 監控對管理頁面的訪問
- **可疑爬蟲檢測**: 識別異常的爬蟲行為

### 📊 流量模式分析
- 時間序列分析（每小時、每日流量）
- 熱門路徑分析
- HTTP 狀態碼分佈
- 用戶代理分析
- 響應時間分析

### 🚨 異常行為檢測
- 響應時間異常檢測
- 請求頻率異常檢測
- 錯誤模式分析
- 路徑訪問異常檢測

### 📈 可視化報告
- 流量趨勢圖表
- 狀態碼分佈圖
- 熱門路徑圖表
- 自動生成 JSON 格式安全報告

## 安裝需求

```bash
# Python 依賴
pip install pyspark pandas matplotlib seaborn

# 或使用 poetry（推薦）
poetry install
```

## 使用方法

### 基本使用

```bash
python log_analyzer.py --input-path /path/to/access.log --output-path /path/to/output
```

### 參數說明

- `--input-path`: 日誌文件路徑（必需）
- `--output-path`: 輸出目錄路徑（必需）
- `--app-name`: Spark 應用名稱（選填，默認為 LogAnalyzer）

### 日誌格式

系統支援標準的 Apache Common Log Format：

```
IP - - [timestamp] "method path protocol" status_code response_size "referer" "user_agent" response_time
```

範例：
```
192.168.1.100 - - [01/Jan/2024:12:00:00 +0000] "GET /index.html HTTP/1.1" 200 1234 "https://example.com" "Mozilla/5.0..." 0.123
```

## 輸出結果

### 1. 安全報告 (security_report.json)

```json
{
  "report_timestamp": "2024-01-01T12:00:00",
  "summary": {
    "total_requests": 100000,
    "unique_ips": 5000,
    "date_range": {
      "start": "2024-01-01T00:00:00",
      "end": "2024-01-01T23:59:59"
    }
  },
  "threats": {
    "sql_injection": {
      "count": 15,
      "severity": "high"
    },
    "brute_force": {
      "count": 5,
      "severity": "medium"
    }
  },
  "anomalies": {
    "slow_requests": {
      "count": 200,
      "severity": "medium"
    }
  },
  "recommendations": [
    "檢測到 SQL 注入嘗試，建議加強輸入驗證和使用參數化查詢",
    "檢測到暴力破解嘗試，建議實施帳戶鎖定策略"
  ]
}
```

### 2. 可視化圖表 (visualizations/)

- `hourly_traffic_analysis.png`: 每小時流量分析
- `status_code_distribution.png`: HTTP 狀態碼分佈
- `popular_paths.png`: 熱門路徑分析

## 範例用法

### 生成測試資料

```python
# 建立測試日誌文件
python -c "
import random
from datetime import datetime, timedelta

# 生成 1000 條測試日誌
with open('test_access.log', 'w') as f:
    for i in range(1000):
        timestamp = datetime.now() - timedelta(hours=random.randint(0, 24))
        ip = f'192.168.1.{random.randint(1, 100)}'
        method = random.choice(['GET', 'POST', 'PUT'])
        path = random.choice(['/', '/login', '/admin', '/api/users'])
        status = random.choice([200, 404, 500])
        size = random.randint(100, 10000)
        response_time = random.uniform(0.1, 2.0)
        
        log_line = f'{ip} - - [{timestamp.strftime(\"%d/%b/%Y:%H:%M:%S +0000\")}] \"{method} {path} HTTP/1.1\" {status} {size} \"-\" \"Mozilla/5.0\" {response_time:.3f}'
        f.write(log_line + '\\n')
"
```

### 執行分析

```bash
# 分析測試日誌
python log_analyzer.py --input-path test_access.log --output-path ./results

# 查看結果
ls -la results/
cat results/security_report.json
```

## 進階配置

### 自定義威脅檢測參數

```python
# 修改 LogAnalyzer 配置
analyzer = LogAnalyzer(spark)
analyzer.config.update({
    'suspicious_ips': ['192.168.1.100', '10.0.0.1'],
    'sensitive_paths': ['/admin', '/config', '/api/admin'],
    'error_threshold': 50,
    'high_freq_threshold': 500,
    'response_time_threshold': 1.5
})
```

### 自定義 Spark 配置

```python
spark = SparkSession.builder \
    .appName("LogAnalyzer") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "2") \
    .getOrCreate()
```

## 效能優化建議

### 1. 資料分區
```python
# 對於大型日誌文件，建議按時間分區
df = df.repartition(col("timestamp"))
```

### 2. 快取策略
```python
# 快取經常使用的資料
df.cache()
```

### 3. 資料壓縮
```bash
# 壓縮日誌文件以節省空間
gzip access.log
```

## 監控和告警

### 結合監控系統

```python
# 結合 Grafana 或其他監控系統
def send_alert(threat_type, count, severity):
    if severity == "high":
        # 發送即時警報
        send_notification(f"檢測到 {threat_type}: {count} 次")
```

### 自動化執行

```bash
# 設定 cron 任務每小時執行一次
0 * * * * /usr/bin/python /path/to/log_analyzer.py --input-path /var/log/apache2/access.log --output-path /var/log/analysis/
```

## 故障排除

### 常見問題

1. **記憶體不足**
   ```bash
   # 增加 Spark 記憶體配置
   export SPARK_EXECUTOR_MEMORY=4g
   export SPARK_DRIVER_MEMORY=2g
   ```

2. **日誌解析失敗**
   ```python
   # 檢查日誌格式是否正確
   df.filter(col("ip").isNull()).show()
   ```

3. **效能問題**
   ```python
   # 檢查分區數量
   print(f"分區數: {df.rdd.getNumPartitions()}")
   ```

## 擴展功能

### 1. 即時監控
```python
# 使用 Structured Streaming 進行即時分析
stream = spark \
    .readStream \
    .format("text") \
    .option("path", "/var/log/apache2/") \
    .load()
```

### 2. 機器學習異常檢測
```python
# 使用 MLlib 進行異常檢測
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler

# 特徵工程
assembler = VectorAssembler(inputCols=["request_count", "response_time"], outputCol="features")
features = assembler.transform(df)

# 聚類分析
kmeans = KMeans(k=3)
model = kmeans.fit(features)
```

### 3. 地理位置分析
```python
# 結合 IP 地理位置資料庫
def get_location(ip):
    # 使用 GeoIP 資料庫
    return lookup_ip_location(ip)
```

## 授權

本專案使用 MIT 授權條款。

## 貢獻

歡迎提交 Issues 和 Pull Requests！

## 聯絡方式

如有問題，請聯絡：[myps6415@gmail.com](mailto:myps6415@gmail.com)