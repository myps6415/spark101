# Spark 101 配置文件

這個目錄包含了 Spark 101 教學專案的配置文件，幫助學習者了解和自定義 Spark 應用程式的設定。

## 配置文件說明

### spark-defaults.conf
這是 Spark 的主要配置文件，包含了：

- **基本設定**：應用程式名稱、主節點設定、記憶體配置
- **性能優化**：自適應查詢執行、動態資源配置
- **日誌和監控**：事件日誌、Web UI 設定
- **序列化**：Kryo 序列化器設定
- **Python 環境**：PySpark 相關設定

### log4j.properties
日誌配置文件，控制 Spark 應用程式的日誌輸出：

- **日誌級別**：設定不同組件的日誌級別
- **輸出格式**：控制日誌的顯示格式
- **第三方函式庫**：減少第三方函式庫的冗餘日誌
- **學習優化**：保留重要的學習資訊

## 使用方法

### 1. 自動載入配置
```bash
# 設定環境變數
export SPARK_CONF_DIR=/path/to/spark101/conf

# 或者在啟動 Spark 時指定
spark-submit --conf spark.driver.extraJavaOptions="-Dlog4j.configuration=file:conf/log4j.properties"
```

### 2. 程式中使用配置
```python
from pyspark.sql import SparkSession

# 創建 SparkSession 時載入配置
spark = SparkSession.builder \
    .config("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:conf/log4j.properties") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()
```

### 3. Jupyter Notebook 中使用
```python
import os
import findspark

# 設定 Spark 配置目錄
os.environ['SPARK_CONF_DIR'] = '/path/to/spark101/conf'

# 初始化 Spark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark101").getOrCreate()
```

## 配置說明

### 記憶體設定
- `spark.executor.memory`: 執行器記憶體，預設 2g
- `spark.driver.memory`: 驅動程式記憶體，預設 1g
- `spark.driver.maxResultSize`: 結果最大大小，預設 1g

### 性能優化
- `spark.sql.adaptive.enabled`: 啟用自適應查詢執行
- `spark.sql.adaptive.coalescePartitions.enabled`: 啟用分區合併
- `spark.serializer`: 使用 Kryo 序列化器提高性能

### 本地開發設定
- `spark.master`: 設定為 `local[*]` 使用所有可用 CPU 核心
- `spark.ui.port`: Web UI 埠號，預設 4040
- `spark.sql.warehouse.dir`: 資料倉庫目錄

## 自訂配置

### 開發環境
```properties
# 開發環境設定
spark.master                      local[2]
spark.executor.memory             1g
spark.driver.memory               512m
spark.sql.shuffle.partitions      10
```

### 生產環境
```properties
# 生產環境設定
spark.master                      yarn
spark.executor.instances          10
spark.executor.memory             4g
spark.driver.memory               2g
spark.sql.shuffle.partitions      400
```

### 學習環境
```properties
# 學習環境設定（更多日誌輸出）
spark.master                      local[*]
spark.executor.memory             2g
log4j.logger.org.apache.spark.sql.execution=DEBUG
log4j.logger.org.apache.spark.sql.catalyst.optimizer=DEBUG
```

## 常見問題

### 1. 記憶體不足
如果遇到記憶體不足錯誤，調整以下設定：
```properties
spark.executor.memory             4g
spark.driver.memory               2g
spark.driver.maxResultSize        2g
```

### 2. 日誌過多
減少日誌輸出：
```properties
log4j.rootCategory=ERROR, console
log4j.logger.org.apache.spark=WARN
```

### 3. 性能問題
啟用更多優化：
```properties
spark.sql.adaptive.enabled        true
spark.sql.adaptive.coalescePartitions.enabled true
spark.sql.execution.arrow.pyspark.enabled true
```

## 學習建議

1. **理解配置**：閱讀每個配置項目的說明
2. **實驗不同設定**：嘗試不同的記憶體和核心設定
3. **監控性能**：使用 Spark UI 觀察配置的影響
4. **調整日誌級別**：根據需要調整日誌的詳細程度

## 參考資料

- [Spark Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Spark Monitoring](https://spark.apache.org/docs/latest/monitoring.html)
- [Spark Tuning Guide](https://spark.apache.org/docs/latest/tuning.html)