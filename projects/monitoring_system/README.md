# 即時監控系統

一個基於 Apache Spark 的即時監控系統，用於監控系統指標、檢測異常行為並自動發送警報。

## 功能特色

### 📊 即時監控
- **系統指標收集**: CPU、記憶體、磁碟、網路延遲等
- **應用程式指標**: 響應時間、錯誤率、請求數等
- **多服務監控**: 支援監控多個服務和伺服器
- **即時處理**: 使用 Spark Streaming 進行即時資料處理

### 🚨 智能警報
- **閾值監控**: 可自訂各項指標的警報閾值
- **多級警報**: 支援 critical、high、medium 三級警報
- **警報抑制**: 避免重複警報的智能抑制機制
- **多通道通知**: 支援 Email、Slack、Webhook 等通知方式

### 🔍 異常檢測
- **統計異常檢測**: 基於移動平均和標準差的異常檢測
- **機器學習檢測**: 使用 K-means 聚類進行異常檢測
- **模式異常檢測**: 檢測多個指標同時異常的模式
- **趨勢分析**: 分析指標的長期趨勢變化

### 📈 可視化報告
- **即時儀表板**: 動態生成監控儀表板
- **歷史趨勢圖**: 展示指標的歷史變化趨勢
- **健康度評分**: 自動計算系統整體健康度
- **詳細報告**: 生成 JSON 格式的詳細監控報告

## 安裝需求

```bash
# Python 依賴
pip install pyspark pandas matplotlib seaborn numpy scikit-learn

# 或使用 poetry（推薦）
poetry install
```

## 快速開始

### 1. 準備配置文件

```bash
# 複製範例配置文件
cp config.json my_config.json

# 根據需要修改配置
vim my_config.json
```

### 2. 基本使用

```bash
# 單次監控執行
python monitoring_system.py --config my_config.json --mode single

# 持續監控模式
python monitoring_system.py --config my_config.json --mode continuous
```

## 配置說明

### 基本配置

```json
{
  "input_path": "./sample_metrics.json",
  "output_path": "./monitoring_output",
  "monitoring_interval": 60,
  "retention_days": 30,
  "enable_ml_detection": true
}
```

### 警報配置

```json
{
  "alert_config": {
    "cpu_threshold": 80.0,
    "memory_threshold": 85.0,
    "disk_threshold": 90.0,
    "network_latency_threshold": 100.0,
    "error_rate_threshold": 5.0,
    "response_time_threshold": 1000.0
  }
}
```

### 通知配置

```json
{
  "notification_channels": {
    "email": {
      "enabled": true,
      "smtp_server": "smtp.gmail.com",
      "smtp_port": 587,
      "username": "your_email@gmail.com",
      "password": "your_app_password"
    },
    "slack": {
      "enabled": true,
      "webhook_url": "https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK"
    }
  }
}
```

## 資料格式

### 指標資料格式

監控系統支援以下格式的指標資料：

```json
{
  "timestamp": "2024-01-01T12:00:00Z",
  "server_id": "server-001",
  "service_name": "web",
  "cpu_usage": 75.5,
  "memory_usage": 68.2,
  "disk_usage": 45.8,
  "network_latency": 25.3,
  "error_rate": 2.1,
  "request_count": 1250,
  "response_time": 180.5,
  "active_connections": 45,
  "thread_count": 20,
  "heap_usage": 60.3
}
```

### 支援的資料來源

- **JSON 文件**: 適用於日誌文件或批次處理
- **CSV 文件**: 適用於結構化資料
- **Parquet 文件**: 適用於大規模資料處理
- **Kafka**: 適用於即時資料流
- **資料庫**: 適用於現有監控系統整合

## 使用範例

### 1. 生成測試資料

```bash
# 生成範例指標資料
python -c "
import json
import random
from datetime import datetime, timedelta

# 生成 1000 條測試指標
metrics = []
for i in range(1000):
    timestamp = datetime.now() - timedelta(seconds=i*30)
    metrics.append({
        'timestamp': timestamp.isoformat(),
        'server_id': f'server-{random.randint(1, 10):03d}',
        'service_name': random.choice(['web', 'api', 'database']),
        'cpu_usage': random.uniform(20, 90),
        'memory_usage': random.uniform(30, 95),
        'disk_usage': random.uniform(40, 80),
        'network_latency': random.uniform(10, 150),
        'error_rate': random.uniform(0, 10),
        'request_count': random.randint(50, 500),
        'response_time': random.uniform(50, 2000),
        'active_connections': random.randint(10, 100),
        'thread_count': random.randint(5, 50),
        'heap_usage': random.uniform(20, 80)
    })

with open('sample_metrics.json', 'w') as f:
    for metric in metrics:
        f.write(json.dumps(metric) + '\\n')
"

echo "測試資料已生成: sample_metrics.json"
```

### 2. 執行監控

```bash
# 執行單次監控
python monitoring_system.py --config config.json --mode single

# 檢查結果
ls -la monitoring_output/
```

### 3. 查看監控報告

```bash
# 查看最新的監控報告
cat monitoring_output/monitoring_report_*.json | jq '.'

# 查看生成的圖表
open monitoring_output/monitoring_dashboard.png
```

## 進階功能

### 1. 自訂異常檢測

```python
# 自訂異常檢測邏輯
class CustomAnomalyDetector:
    def detect_business_anomalies(self, metrics_df):
        # 業務邏輯相關的異常檢測
        # 例如：訂單量突然下降、用戶活躍度異常等
        pass
    
    def detect_seasonal_anomalies(self, metrics_df):
        # 季節性異常檢測
        # 考慮時間因素的異常檢測
        pass
```

### 2. 整合外部系統

```python
# 整合 Grafana
def export_to_grafana(metrics_df):
    # 將指標資料匯出到 Grafana
    pass

# 整合 Prometheus
def export_to_prometheus(metrics_df):
    # 將指標資料匯出到 Prometheus
    pass
```

### 3. 機器學習模型

```python
# 使用 LSTM 進行時間序列預測
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler

def train_prediction_model(historical_data):
    # 訓練預測模型
    assembler = VectorAssembler(inputCols=["cpu_usage", "memory_usage"], outputCol="features")
    feature_data = assembler.transform(historical_data)
    
    lr = LinearRegression(featuresCol="features", labelCol="response_time")
    model = lr.fit(feature_data)
    
    return model
```

## 警報規則

### 1. 基本警報規則

```python
# CPU 使用率警報
if cpu_usage > 80:
    severity = "high"
    message = f"CPU 使用率過高: {cpu_usage}%"

# 記憶體使用率警報
if memory_usage > 85:
    severity = "high"
    message = f"記憶體使用率過高: {memory_usage}%"

# 響應時間警報
if response_time > 1000:
    severity = "medium"
    message = f"響應時間過長: {response_time}ms"
```

### 2. 複合警報規則

```python
# 系統負載過高警報
if cpu_usage > 80 and memory_usage > 80 and response_time > 500:
    severity = "critical"
    message = "系統負載過高，多個指標異常"

# 服務不可用警報
if error_rate > 50 and response_time > 5000:
    severity = "critical"
    message = "服務可能不可用"
```

### 3. 趨勢警報

```python
# CPU 使用率持續上升警報
if cpu_trend == "increasing" and cpu_usage > 70:
    severity = "medium"
    message = "CPU 使用率持續上升，需要關注"
```

## 效能優化

### 1. Spark 配置優化

```python
spark = SparkSession.builder \
    .appName("MonitoringSystem") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.cores", "2") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()
```

### 2. 資料分區策略

```python
# 按時間分區
df = df.repartition(col("timestamp"))

# 按服務分區
df = df.repartition(col("service_name"))
```

### 3. 快取策略

```python
# 快取經常使用的資料
metrics_df.cache()

# 快取中間結果
processed_df.persist(StorageLevel.MEMORY_AND_DISK)
```

## 部署指南

### 1. 本地部署

```bash
# 克隆專案
git clone https://github.com/your-org/monitoring-system.git
cd monitoring-system

# 安裝依賴
pip install -r requirements.txt

# 啟動監控
python monitoring_system.py --mode continuous
```

### 2. Docker 部署

```dockerfile
FROM python:3.8-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "monitoring_system.py", "--mode", "continuous"]
```

```bash
# 建立 Docker 映像
docker build -t monitoring-system .

# 執行容器
docker run -d -v $(pwd)/config.json:/app/config.json monitoring-system
```

### 3. Kubernetes 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: monitoring-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: monitoring-system
  template:
    metadata:
      labels:
        app: monitoring-system
    spec:
      containers:
      - name: monitoring-system
        image: monitoring-system:latest
        resources:
          requests:
            memory: "2Gi"
            cpu: "1"
          limits:
            memory: "4Gi"
            cpu: "2"
        volumeMounts:
        - name: config
          mountPath: /app/config.json
          subPath: config.json
      volumes:
      - name: config
        configMap:
          name: monitoring-config
```

## 監控最佳實踐

### 1. 指標選擇
- 選擇關鍵業務指標
- 避免監控過多無用指標
- 考慮指標的相關性

### 2. 警報設定
- 設定合理的警報閾值
- 避免警報風暴
- 建立警報升級機制

### 3. 資料保留
- 根據需求設定資料保留期限
- 考慮資料壓縮和歸檔
- 定期清理歷史資料

### 4. 系統維護
- 定期檢查系統健康狀態
- 更新警報規則
- 優化查詢效能

## 故障排除

### 常見問題

1. **記憶體不足**
   ```bash
   # 增加 Spark 記憶體配置
   export SPARK_EXECUTOR_MEMORY=4g
   export SPARK_DRIVER_MEMORY=2g
   ```

2. **資料載入失敗**
   ```python
   # 檢查資料格式
   df.printSchema()
   df.show(5)
   ```

3. **警報未觸發**
   ```python
   # 檢查警報配置
   print(f"當前 CPU 使用率: {current_cpu}")
   print(f"CPU 警報閾值: {cpu_threshold}")
   ```

### 日誌分析

```bash
# 查看應用程式日誌
tail -f monitoring_system.log

# 查看 Spark 日誌
ls -la $SPARK_HOME/logs/
```

## 擴展功能

### 1. 新增指標類型

```python
# 新增自訂指標
class CustomMetric:
    def __init__(self, name, value, unit):
        self.name = name
        self.value = value
        self.unit = unit
    
    def to_dict(self):
        return {
            'name': self.name,
            'value': self.value,
            'unit': self.unit,
            'timestamp': datetime.now().isoformat()
        }
```

### 2. 新增通知通道

```python
# 新增 Teams 通知
class TeamsNotifier:
    def __init__(self, webhook_url):
        self.webhook_url = webhook_url
    
    def send_notification(self, message):
        # 發送 Teams 通知
        pass
```

### 3. 新增資料來源

```python
# 新增 InfluxDB 資料來源
class InfluxDBSource:
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
    
    def read_metrics(self, query):
        # 從 InfluxDB 讀取指標
        pass
```

## 授權

本專案使用 MIT 授權條款。

## 貢獻

歡迎提交 Issues 和 Pull Requests！

## 聯絡方式

如有問題，請聯絡：[myps6415@gmail.com](mailto:myps6415@gmail.com)