# 🔥 Spark 101 教學課程

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Apache Spark 3.4+](https://img.shields.io/badge/Spark-3.4+-orange.svg)](https://spark.apache.org/)
[![Poetry](https://img.shields.io/badge/dependency--manager-poetry-blue)](https://python-poetry.org/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)](./tests/)

> 🎯 **從零開始學習 Apache Spark 的完整教學資源**

一個全面、實戰導向的 Apache Spark 學習項目，包含完整的教學內容、實作練習、企業級項目和測試套件。

## 📊 專案特色

- ✅ **32個實作練習** - 每章4個循序漸進的練習
- ✅ **8個互動式 Notebook** - 豐富的學習體驗
- ✅ **3個企業級實戰項目** - 日誌分析、監控系統、推薦系統
- ✅ **完整測試套件** - 10個測試文件，全面覆蓋
- ✅ **中文文檔** - 適合華語學習者
- ✅ **自動化工具** - 一鍵環境設置和項目管理

## 🎯 學習目標

- 💪 掌握 Apache Spark 的核心概念和架構
- 🔧 熟練使用 Spark Core, DataFrame, SQL 等 API
- 📊 學會 Spark Streaming 即時數據處理
- 🤖 了解 MLlib 機器學習應用
- ⚡ 掌握性能調優技巧
- 🚀 完成企業級實戰項目

## 📚 課程大綱

### 🌟 基礎篇

| 章節 | 主題 | 範例 | 練習 | 重點內容 |
|------|------|------|------|----------|
| **第1章** | Spark 基礎概念 | 1 | 4 | 環境設置、第一個程式、核心架構 |
| **第2章** | RDD 基本操作 | 1 | 4 | RDD概念、轉換操作、行動操作 |
| **第3章** | DataFrame API | 3 | 4 | Schema定義、數據讀寫、基本操作 |

### 🔥 進階篇

| 章節 | 主題 | 範例 | 練習 | 重點內容 |
|------|------|------|------|----------|
| **第4章** | Spark SQL | 2 | 4 | SQL查詢、視窗函數、複雜分析 |
| **第5章** | Spark Streaming | 2 | 4 | 結構化流、實時處理、狀態管理 |
| **第6章** | MLlib 機器學習 | 1 | 4 | 特徵工程、模型訓練、管道構建 |

### 🚀 實戰篇

| 章節 | 主題 | 範例 | 練習 | 重點內容 |
|------|------|------|------|----------|
| **第7章** | 性能調優 | 1 | 4 | 分區策略、緩存優化、資源管理 |
| **第8章** | 實戰項目 | 2 | 4 | 日誌分析、推薦系統、綜合平台 |

## 🛠️ 環境要求

### 系統需求
- **Java**: 8 或 11 (推薦 OpenJDK 11)
- **Python**: 3.8+ 
- **記憶體**: 至少 4GB RAM
- **作業系統**: Linux, macOS, Windows

### 主要依賴
- **Apache Spark**: 3.4.1
- **Python 套件**: pandas, numpy, jupyter
- **開發工具**: pytest, poetry

## 📦 快速安裝

### 方法一：Poetry (推薦)

```bash
# 1. 克隆專案
git clone https://github.com/myps6415/spark101.git
cd spark101

# 2. 安裝 Poetry (如果尚未安裝)
curl -sSL https://install.python-poetry.org | python3 -

# 3. 安裝依賴並初始化環境
make bootstrap
# 或分步執行：
# poetry install
# poetry run python scripts/bootstrap.py

# 4. 激活環境
poetry shell
```

### 方法二：傳統 pip 安裝

```bash
# 1. 克隆專案
git clone https://github.com/myps6415/spark101.git
cd spark101

# 2. 建立虛擬環境
python -m venv venv
source venv/bin/activate  # Linux/macOS
# 或 venv\Scripts\activate  # Windows

# 3. 安裝依賴
pip install -r requirements.txt

# 4. 測試環境
python test_spark_setup.py
```

### 方法三：Docker (推薦，統一環境)

#### 🐳 完整環境（包含所有服務）
```bash
# 1. 克隆專案
git clone https://github.com/myps6415/spark101.git
cd spark101

# 2. 啟動完整環境
docker-compose up -d

# 3. 訪問 Jupyter Notebook
open http://localhost:8888
# Token: spark101
```

#### 🔧 開發環境（輕量級）
```bash
# 啟動開發環境
docker-compose -f docker-compose.dev.yml up -d

# 查看狀態
docker-compose -f docker-compose.dev.yml ps

# 進入容器
docker-compose -f docker-compose.dev.yml exec spark101-dev bash
```

#### 🛠️ VS Code 開發容器
```bash
# 1. 安裝 VS Code 和 Remote-Containers 擴展
# 2. 在 VS Code 中開啟專案
# 3. 按 Ctrl+Shift+P，選擇 "Remote-Containers: Reopen in Container"
# 4. 選擇 "From 'docker-compose.yml'"
```

#### 🎯 Docker 環境說明
- **🔥 Spark101 主環境**: Jupyter Notebook + PySpark 3.4.1 + Java 11
- **🗄️ PostgreSQL**: 用於資料庫相關練習
- **🔴 Redis**: 用於快取和流處理練習
- **📊 Kafka**: 用於流處理和訊息佇列練習
- **💾 Minio**: S3 相容的物件儲存
- **📈 Prometheus + Grafana**: 監控和視覺化
- **🖥️ 埠號對應**:
  - Jupyter Notebook: http://localhost:8888
  - Spark UI: http://localhost:4040
  - Grafana: http://localhost:3000
  - Prometheus: http://localhost:9090
  - Minio: http://localhost:9000

#### 🚀 一鍵啟動腳本
```bash
# 快速啟動開發環境
chmod +x scripts/setup_java_env.sh
./scripts/setup_java_env.sh

# 或者使用 Docker
make docker-up
```

> 📖 **詳細的 Docker 使用指南**: [DOCKER_GUIDE.md](./DOCKER_GUIDE.md) - 包含完整的 Docker 環境設置、故障排除和最佳實踐

## 🚀 快速開始

### 運行第一個範例

```bash
# 使用 Makefile (推薦)
make run-examples

# 或直接運行
poetry run python examples/chapter01/hello_spark.py
```

### 啟動 Jupyter Notebook

```bash
# 啟動 Jupyter
make jupyter

# 或直接運行
poetry run jupyter notebook notebooks/
```

### 運行測試

```bash
# 運行所有測試
make test

# 運行特定章節測試
make test-chapter01

# 運行特定測試文件
poetry run pytest tests/test_chapter01.py -v
```

## 🗂️ 專案結構

```
spark101/
├── 📄 README.md                    # 專案說明
├── ⚙️ pyproject.toml              # Poetry 配置
├── 🔧 Makefile                    # 自動化命令
├── 📦 requirements.txt            # pip 依賴列表
├── 🛠️ setup_env.py               # 環境設置腳本
├── 🧪 test_spark_setup.py        # 環境測試腳本
├── 📁 scripts/
│   └── 🚀 bootstrap.py           # 初始化腳本
├── 📚 examples/                   # 教學範例 (12個檔案)
│   ├── 📖 chapter01/             # 基礎概念
│   ├── 📖 chapter02/             # RDD 操作
│   ├── 📖 chapter03/             # DataFrame API
│   ├── 📖 chapter04/             # Spark SQL
│   ├── 📖 chapter05/             # Streaming
│   ├── 📖 chapter06/             # MLlib
│   ├── 📖 chapter07/             # 性能調優
│   └── 📖 chapter08/             # 實戰項目
├── 💪 exercises/                  # 實作練習 (32個檔案)
│   ├── 📝 chapter01/             # 4個基礎練習
│   ├── 📝 chapter02/             # 4個RDD練習
│   ├── 📝 chapter03/             # 4個DataFrame練習
│   ├── 📝 chapter04/             # 4個SQL練習
│   ├── 📝 chapter05/             # 4個Streaming練習
│   ├── 📝 chapter06/             # 4個MLlib練習
│   ├── 📝 chapter07/             # 4個調優練習
│   └── 📝 chapter08/             # 4個項目練習
├── 📓 notebooks/                  # Jupyter 筆記本 (8個檔案)
│   ├── 01_spark_basics.ipynb
│   ├── 02_rdd_operations.ipynb
│   ├── 03_dataframe_operations.ipynb
│   ├── 04_spark_sql.ipynb
│   ├── 05_streaming.ipynb
│   ├── 06_mllib.ipynb
│   ├── 07_performance_tuning.ipynb
│   └── 08_projects.ipynb
├── 📊 datasets/                   # 示例數據集
│   ├── employees_large.csv
│   ├── sales_data.json
│   ├── sample_data.csv
│   └── server_logs.txt
├── 🏗️ projects/                   # 企業級實戰項目
│   ├── 📈 log_analyzer/          # 日誌分析系統
│   ├── 📊 monitoring_system/     # 監控系統
│   └── 🎯 recommendation_system/ # 推薦系統
├── 🧪 tests/                      # 測試套件 (10個檔案)
│   ├── conftest.py               # 測試配置
│   ├── test_chapter01.py         # 第1章測試
│   ├── test_chapter02.py         # 第2章測試
│   ├── test_chapter03.py         # 數據IO測試
│   ├── test_chapter04.py         # SQL測試
│   ├── test_chapter05.py         # Streaming測試
│   ├── test_chapter06.py         # MLlib測試
│   ├── test_chapter07.py         # 性能測試
│   ├── test_chapter08.py         # 項目測試
│   ├── test_dataframe_operations.py
│   └── test_projects.py          # 集成測試
└── ⚙️ conf/                       # 配置文件
    ├── spark-defaults.conf       # Spark 配置
    └── log4j.properties          # 日誌配置
```

## 🔧 專案管理命令

使用 Makefile 簡化日常操作：

```bash
# 📚 環境管理
make install      # 安裝依賴
make dev          # 設置開發環境  
make bootstrap    # 完整初始化
make clean        # 清理環境

# 🚀 運行範例
make run-examples       # 運行第1章範例
make run-chapter01      # 運行第1章範例
make run-chapter02      # 運行第2章範例
make validate          # 驗證所有範例

# 🧪 測試
make test              # 運行所有測試
make test-chapter01    # 運行第1章測試
make test-dataframe    # 運行DataFrame測試

# 📓 Jupyter
make jupyter           # 啟動 Jupyter Notebook
make jupyterlab        # 啟動 JupyterLab

# 🛠️ 開發工具
make format           # 格式化代碼
make lint            # 代碼檢查
make check-env       # 檢查環境
make stats           # 專案統計

make help            # 顯示所有命令
```

## 🎓 學習路徑建議

### 🚶‍♂️ 初學者 (4-6週)
1. **第1-2章**: 了解基礎概念和RDD操作
2. **第3章**: 掌握DataFrame API的使用
3. **第4章**: 學習Spark SQL基本查詢
4. **實作**: 完成基礎練習並運行測試

### 🏃‍♂️ 進階學習者 (3-4週)
1. **第5章**: 深入Streaming實時處理
2. **第6章**: 掌握MLlib機器學習
3. **第7章**: 學習性能調優技巧
4. **實戰**: 完成企業級項目練習

### 🚀 專家級 (2-3週)
1. **第8章**: 完成綜合實戰項目
2. **深度**: 研讀源碼和進階配置
3. **貢獻**: 參與開源項目貢獻
4. **分享**: 寫技術文章或做分享

## 🏆 企業級實戰項目

### 1. 📈 日誌分析系統
- **功能**: 實時日誌監控、異常檢測、性能分析
- **技術**: Spark Streaming, 正規表達式, 統計分析
- **應用**: 網站監控、安全分析、運維告警

### 2. 📊 監控系統
- **功能**: 系統指標收集、閾值告警、趨勢分析
- **技術**: 時間序列分析, 異常檢測, 實時儀表板
- **應用**: 服務器監控、應用性能管理

### 3. 🎯 推薦系統
- **功能**: 協同過濾、內容推薦、個性化排序
- **技術**: ALS算法, 機器學習管道, A/B測試
- **應用**: 電商推薦、內容推薦、廣告投放

### 4. 🏢 綜合數據平台
- **功能**: 批流一體、機器學習、監控告警
- **技術**: 端到端數據管道、模型部署、自動化運維
- **應用**: 企業級數據平台、智能決策系統

## 🧪 測試與品質保證

### 測試覆蓋範圍
- ✅ **單元測試**: 核心功能測試
- ✅ **集成測試**: 端到端流程測試  
- ✅ **性能測試**: 大數據處理測試
- ✅ **項目測試**: 實戰項目功能測試

### 運行測試
```bash
# 運行所有測試
pytest tests/ -v

# 運行特定測試
pytest tests/test_chapter01.py -v

# 測試覆蓋率
pytest tests/ --cov=. --cov-report=html
```

## ❓ 常見問題

<details>
<summary><strong>Q: 我需要什麼基礎知識？</strong></summary>

**建議具備：**
- 基本程式設計能力 (Python/Scala/Java)
- 資料庫和 SQL 基礎
- 對分散式系統有基本了解
- Linux 命令列基本操作

**推薦學習資源：**
- Python: [廖雪峰的Python教程](https://www.liaoxuefeng.com/wiki/1016959663602400)
- SQL: [W3Schools SQL Tutorial](https://www.w3schools.com/sql/)
- 分散式系統: [MIT 6.824](https://pdos.csail.mit.edu/6.824/)

</details>

<details>
<summary><strong>Q: 學習 Spark 需要多長時間？</strong></summary>

**根據個人基礎：**
- **基礎篇** (第1-3章): 2-3 週
- **進階篇** (第4-6章): 3-4 週  
- **實戰篇** (第7-8章): 2-3 週
- **總計**: 7-10 週的持續學習

**學習建議：**
- 每天投入 1-2 小時
- 理論學習 + 動手實作
- 參與開源項目實踐

</details>

<details>
<summary><strong>Q: 我應該選擇哪種語言？</strong></summary>

**語言選擇建議：**
- **Python (PySpark)**: 適合數據科學、機器學習、快速原型開發
- **Scala**: 性能最佳、與Spark原生語言相同、適合大規模生產
- **Java**: 企業級開發、團隊協作、穩定可靠
- **R (SparkR)**: 統計分析專業、學術研究

**本課程主要使用 Python，因為：**
- 學習曲線平緩
- 生態系統豐富
- 社區支持良好
- 與數據科學工具整合度高

</details>

<details>
<summary><strong>Q: 如何處理安裝問題？</strong></summary>

**常見問題解決：**

1. **Java 版本問題**:
   ```bash
   # 檢查Java版本
   java -version
   
   # 安裝OpenJDK 11 (推薦)
   # macOS
   brew install openjdk@11
   
   # Ubuntu
   sudo apt install openjdk-11-jdk
   
   # 設置JAVA_HOME
   export JAVA_HOME=$(/usr/libexec/java_home -v 11)
   ```

2. **Poetry 安裝問題**:
   ```bash
   # 重新安裝Poetry
   curl -sSL https://install.python-poetry.org | python3 -
   
   # 添加到PATH
   export PATH="$HOME/.local/bin:$PATH"
   ```

3. **依賴衝突**:
   ```bash
   # 清理並重新安裝
   poetry env remove --all
   poetry install
   ```

</details>

## 📚 學習資源

### 官方文檔
- [Apache Spark 官方文檔](https://spark.apache.org/docs/latest/)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

### 推薦書籍
- 📖 [Learning Spark, 2nd Edition](https://www.oreilly.com/library/view/learning-spark-2nd/9781492050032/)
- 📖 [Spark: The Definitive Guide](https://www.oreilly.com/library/view/spark-the-definitive/9781491912201/)
- 📖 [High Performance Spark](https://www.oreilly.com/library/view/high-performance-spark/9781491943199/)

### 線上課程
- 🎓 [Databricks Academy](https://academy.databricks.com/)
- 🎓 [edX: Introduction to Apache Spark](https://www.edx.org/course/introduction-apache-spark-uc-berkeleyx-cs105x)
- 🎓 [Coursera: Big Data Specialization](https://www.coursera.org/specializations/big-data)

### 社群資源
- 💬 [Apache Spark 用戶郵件列表](https://spark.apache.org/community.html)
- 💬 [Stack Overflow - Apache Spark](https://stackoverflow.com/questions/tagged/apache-spark)
- 💬 [Reddit - r/apachespark](https://www.reddit.com/r/apachespark/)

## 🤝 貢獻指南

我們歡迎各種形式的貢獻！

### 如何貢獻
1. **報告問題**: [提交 Issue](https://github.com/myps6415/spark101/issues)
2. **改進文檔**: 修復錯字、改善說明
3. **新增內容**: 添加新的練習或範例
4. **優化代碼**: 提升性能和可讀性
5. **分享經驗**: 提供學習心得和最佳實踐

### 貢獻流程
```bash
# 1. Fork 專案
git clone https://github.com/your-username/spark101.git

# 2. 創建特性分支
git checkout -b feature/your-feature-name

# 3. 提交更改
git commit -m "Add: 描述你的更改"

# 4. 推送分支
git push origin feature/your-feature-name

# 5. 提交 Pull Request
```

### 貢獻類型
- 🐛 **Bug 修復**: 修復代碼問題
- ✨ **新功能**: 添加新的教學內容
- 📚 **文檔改進**: 完善說明和教程
- 🎨 **代碼優化**: 提升代碼品質
- 🧪 **測試增強**: 增加測試覆蓋

## 📊 專案統計

| 項目 | 數量 | 說明 |
|------|------|------|
| 📚 **教學章節** | 8章 | 從基礎到進階的完整內容 |
| 📝 **實作練習** | 32個 | 每章4個循序漸進的練習 |
| 📓 **Jupyter Notebooks** | 8個 | 互動式學習體驗 |
| 🏗️ **實戰項目** | 3+1個 | 企業級應用場景 |
| 🧪 **測試文件** | 10個 | 全面的品質保證 |
| 📊 **數據集** | 4個 | 真實業務場景數據 |
| 📖 **範例程式** | 12個 | 核心概念演示 |
| 📄 **文檔頁面** | 100+ | 詳細的學習指南 |

## 🔄 更新日誌

### v1.0.0 (2024-01-20)
- ✨ 完整的8章教學內容
- ✨ 32個實作練習
- ✨ 3個企業級實戰項目
- ✨ 完整測試套件
- ✨ 自動化環境設置
- ✨ 詳細的中文文檔

### 計劃中的功能
- 🚀 GitHub Actions CI/CD
- 🚀 Docker 容器化部署
- 🚀 雲端平台整合 (AWS/Azure/GCP)
- 🚀 更多實戰項目案例
- 🚀 視頻教學內容
- 🚀 多語言支持

## 📜 授權協議

本專案採用 [MIT License](LICENSE) 開源協議。

```
MIT License

Copyright (c) 2024 JohnTung

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
```

## 💝 致謝

感謝以下資源和項目的啟發：
- [Apache Spark](https://spark.apache.org/) 開源社群
- [Databricks](https://databricks.com/) 的優秀教學資源
- 所有貢獻者和學習者的回饋

## 🌟 支持專案

如果這個專案對你有幫助，請考慮：

- ⭐ **給專案加星**: 在 GitHub 上點擊 Star
- 🔄 **分享專案**: 推薦給朋友和同事
- 🐛 **回報問題**: 幫助我們改進專案
- 💡 **提供建議**: 分享你的學習經驗
- 🤝 **參與貢獻**: 一起完善這個專案

---

<div align="center">

**🎓 開始你的 Apache Spark 學習之旅！**

[📚 查看教學](./examples/) | [💪 開始練習](./exercises/) | [🚀 實戰項目](./projects/) | [🧪 運行測試](./tests/)

**Made with ❤️ by [JohnTung](https://github.com/myps6415)**

⭐ 如果這個教學對你有幫助，請給個 star！

</div>