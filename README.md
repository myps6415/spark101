# Spark 101 教學課程

🔥 從零開始學習 Apache Spark 的完整教學資源

## 🎯 學習目標

- 掌握 Apache Spark 的核心概念和架構
- 熟練使用 Spark Core, DataFrame, SQL 等 API
- 學會 Spark Streaming 即時數據處理
- 了解 MLlib 機器學習應用
- 掌握性能調優技巧
- 完成實戰項目

## 📚 課程大綱

### 基礎篇

#### 第1章：Spark 基礎概念
- 什麼是 Apache Spark
- 分散式計算概念
- Spark vs Hadoop MapReduce
- 安裝和環境設置
- 第一個 Spark 程式

#### 第2章：Spark Core 基本操作
- RDD (Resilient Distributed Dataset) 概念
- 創建和操作 RDD
- Transformations vs Actions
- 並行化和分區
- 容錯機制

#### 第3章：DataFrame 和 Dataset API
- DataFrame 基本操作
- Schema 定義
- 常用函數和操作
- 資料讀寫操作

### 進階篇

#### 第4章：Spark SQL
- SQL 查詢語法
- 臨時視圖 (Temporary Views)
- 內建函數
- 複雜查詢操作
- 性能調優

#### 第5章：Spark Streaming
- 流式處理概念
- DStream 操作
- 結構化流處理 (Structured Streaming)
- 實時數據處理
- 視窗操作

#### 第6章：MLlib 機器學習
- 機器學習基礎
- 特徵工程
- 模型訓練和評估
- 管道(Pipeline)
- 常用演算法

### 實戰篇

#### 第7章：性能調優
- 記憶體管理
- 執行器配置
- 資料序列化
- 快取策略
- 分區策略

#### 第8章：實戰項目
- 日誌分析系統
- 推薦系統
- 即時監控系統
- 大數據 ETL

## 🛠️ 環境要求

- Java 8 或更高版本
- Python 3.6+ (如果使用 PySpark)
- Scala 2.12+ (如果使用 Scala)
- Apache Spark 3.0+
- 至少 4GB RAM

## 📦 安裝指南

### 使用 Poetry (推薦)

1. 安裝 Poetry
```bash
curl -sSL https://install.python-poetry.org | python3 -
```

2. 克隆專案並安裝依賴
```bash
git clone <your-repo-url>
cd spark101
poetry install
```

3. 激活虛擬環境
```bash
poetry shell
```

4. 初始化環境（可選）
```bash
# 使用 Makefile（推薦）
make bootstrap

# 或直接運行腳本
python scripts/bootstrap.py
```

### 傳統安裝方式

1. 安裝 Java
```bash
java -version
```

2. 下載 Spark
```bash
wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar -xzf spark-3.5.0-bin-hadoop3.tgz
```

3. 設定環境變數
```bash
export SPARK_HOME=/path/to/spark-3.5.0-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin
```

4. 安裝 Python 依賴
```bash
pip install pyspark jupyter pandas numpy matplotlib seaborn plotly
```

### 使用 Docker

```bash
docker run -it --rm \
  -p 8888:8888 \
  jupyter/pyspark-notebook
```

## 🚀 快速開始

### 使用 Poetry

1. 運行第一個範例
```bash
# 使用 Makefile（推薦）
make run-examples

# 或直接運行
poetry run python examples/chapter01/hello_spark.py
```

2. 啟動 Jupyter Notebook
```bash
# 使用 Makefile（推薦）
make jupyter

# 或直接運行
poetry run jupyter notebook
```

3. 啟動 PySpark Shell
```bash
poetry run pyspark
```

### 常用 Makefile 命令

```bash
make help         # 顯示所有可用命令
make install      # 安裝依賴
make dev          # 設置開發環境
make bootstrap    # 初始化環境
make test         # 運行測試
make format       # 格式化代碼
make lint         # 代碼檢查
make clean        # 清理環境
```

### 傳統方式

1. 啟動 Spark Shell
```bash
spark-shell  # Scala
pyspark      # Python
```

2. 第一個程式
```python
# 創建 SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Spark101").getOrCreate()

# 創建 DataFrame
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# 顯示結果
df.show()
```

## 📂 專案結構

```
spark101/
├── README.md
├── pyproject.toml          # Poetry 配置文件
├── Makefile               # 專案管理命令
├── scripts/
│   └── bootstrap.py       # 環境初始化腳本
├── examples/
│   ├── chapter01/
│   ├── chapter02/
│   ├── ...
│   └── chapter08/
├── datasets/
│   ├── sample_data.csv
│   └── ...
├── notebooks/
│   ├── 01_spark_basics.ipynb
│   └── ...
└── projects/
    ├── log_analyzer/
    ├── recommendation_system/
    └── ...
```

## 🎓 學習建議

1. **循序漸進**：按照章節順序學習
2. **動手實作**：每個概念都要親自寫程式
3. **練習為主**：理論結合實際操作
4. **查看文檔**：養成查閱官方文檔的習慣
5. **做筆記**：記錄重要概念和踩過的坑

## 📚 參考資源

- [Apache Spark 官方文檔](https://spark.apache.org/docs/latest/)
- [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Scala API Reference](https://spark.apache.org/docs/latest/api/scala/)

## 💡 常見問題

### Q: 我需要什麼基礎知識？
A: 建議具備：
- 基本程式設計能力 (Python/Scala/Java)
- 資料庫和 SQL 基礎
- 對分散式系統有基本了解

### Q: 學習 Spark 需要多長時間？
A: 根據個人基礎：
- 基礎篇：2-3 週
- 進階篇：3-4 週
- 實戰篇：2-3 週

### Q: 我應該選擇哪種語言？
A: 建議：
- **Python**: 適合數據科學和機器學習
- **Scala**: 性能最好，與 Spark 原生語言相同
- **Java**: 企業級開發

## 🤝 貢獻

歡迎提交 Issue 和 Pull Request！

## 📄 授權

MIT License

---

⭐ 如果這個教學對你有幫助，請給個 star！