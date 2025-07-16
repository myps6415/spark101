# 🐳 Spark101 Docker 使用指南

## 📋 概述

為了解決不同開發環境的相容性問題，Spark101 提供了完整的 Docker 化解決方案。無論你使用的是 Windows、macOS 還是 Linux，都可以獲得一致的開發體驗。

## 🎯 Docker 環境優勢

### ✅ 統一環境
- **Java 版本**: 統一使用 OpenJDK 11
- **Python 版本**: 統一使用 Python 3.11
- **Spark 版本**: 統一使用 PySpark 3.4.1
- **依賴管理**: 所有依賴都已預先安裝和配置

### ✅ 即開即用
- **一鍵啟動**: 無需複雜的環境配置
- **完整工具鏈**: 包含 Jupyter、監控、資料庫等
- **自動化腳本**: 提供豐富的管理命令

### ✅ 學習友好
- **互動式環境**: Jupyter Notebook 和 JupyterLab
- **視覺化監控**: Spark UI、Grafana 儀表板
- **實戰練習**: 完整的資料處理生態系統

## 🚀 快速開始

### 1. 準備工作

```bash
# 確保已安裝 Docker 和 Docker Compose
docker --version
docker-compose --version

# 克隆專案
git clone https://github.com/myps6415/spark101.git
cd spark101
```

### 2. 選擇環境

#### 🔥 方案一：完整環境（推薦學習）
```bash
# 啟動完整環境（包含監控、資料庫、Kafka 等）
make docker-up
# 或者
docker-compose up -d
```

#### 🔧 方案二：開發環境（推薦日常開發）
```bash
# 啟動輕量級開發環境
make docker-up-dev
# 或者
docker-compose -f docker-compose.dev.yml up -d
```

#### 🛠️ 方案三：VS Code 開發容器
```bash
# 在 VS Code 中開啟專案
code .

# 按 Ctrl+Shift+P，選擇：
# "Remote-Containers: Reopen in Container"
```

### 3. 訪問服務

| 服務 | 地址 | 用途 | 認證 |
|------|------|------|------|
| 🔥 **Jupyter Notebook** | http://localhost:8888 | 主要學習環境 | Token: `spark101` |
| ⚡ **Spark UI** | http://localhost:4040 | Spark 任務監控 | 無 |
| 📊 **Grafana** | http://localhost:3000 | 系統監控儀表板 | admin/spark101 |
| 📈 **Prometheus** | http://localhost:9090 | 指標收集 | 無 |
| 💾 **Minio** | http://localhost:9000 | 物件儲存 | spark101/spark101pass |
| 🗄️ **PostgreSQL** | localhost:5432 | 資料庫 | spark101/spark101pass |
| 🔴 **Redis** | localhost:6379 | 快取 | 無 |
| 📊 **Kafka** | localhost:9092 | 訊息佇列 | 無 |

## 🔧 管理命令

### 環境管理
```bash
# 啟動服務
make docker-up          # 完整環境
make docker-up-dev      # 開發環境

# 停止服務
make docker-down        # 停止完整環境
make docker-down-dev    # 停止開發環境

# 重啟服務
make docker-restart     # 重啟完整環境
make docker-restart-dev # 重啟開發環境

# 查看狀態
docker-compose ps
docker-compose -f docker-compose.dev.yml ps
```

### 映像管理
```bash
# 構建映像
make docker-build       # 構建標準映像
make docker-build-dev   # 構建開發映像
make docker-build-prod  # 構建生產映像

# 清理資源
make docker-clean       # 清理所有 Docker 資源
docker system prune -f  # 清理系統資源
```

### 容器操作
```bash
# 進入容器
make docker-shell       # 進入主容器
make docker-shell-dev   # 進入開發容器

# 查看日誌
make docker-logs        # 查看完整環境日誌
make docker-logs-dev    # 查看開發環境日誌

# 執行測試
make docker-test        # 在 Docker 中運行測試
make docker-test-dev    # 在開發環境中運行測試
```

### 常用容器操作
```bash
# 進入主容器
docker-compose exec spark101 bash

# 運行 Python 腳本
docker-compose exec spark101 python examples/chapter01/hello_spark.py

# 運行測試
docker-compose exec spark101 pytest tests/test_chapter01.py -v

# 安裝額外套件
docker-compose exec spark101 pip install pandas

# 查看容器資源使用
docker stats
```

## 📚 學習工作流程

### 1. 環境準備
```bash
# 啟動環境
make docker-up-dev

# 等待服務啟動（約 30-60 秒）
docker-compose -f docker-compose.dev.yml logs -f spark101-dev
```

### 2. 開始學習
```bash
# 方式一：使用 Jupyter Notebook
open http://localhost:8888
# Token: spark101

# 方式二：命令行模式
make docker-shell-dev
python examples/chapter01/hello_spark.py
```

### 3. 實戰練習
```bash
# 運行練習
docker-compose exec spark101-dev python exercises/chapter01/exercise_01_basic_spark.py

# 運行測試
docker-compose exec spark101-dev pytest tests/test_chapter01.py -v

# 查看 Spark UI
open http://localhost:4040
```

### 4. 監控和調優
```bash
# 啟動完整環境（如果需要監控）
make docker-up

# 查看監控儀表板
open http://localhost:3000  # Grafana
open http://localhost:9090  # Prometheus
```

## 🛠️ 開發指南

### VS Code 開發容器設置

1. **安裝擴展**
   - Remote - Containers
   - Python
   - Jupyter

2. **開啟專案**
   ```bash
   code .
   ```

3. **重新開啟在容器中**
   - 按 `Ctrl+Shift+P`
   - 選擇 "Remote-Containers: Reopen in Container"
   - 選擇 "From 'docker-compose.yml'"

4. **開始開發**
   - 自動安裝所有擴展
   - 自動配置 Python 環境
   - 自動設置 Jupyter 核心

### 自定義配置

#### 修改 Jupyter 配置
```bash
# 編輯 Jupyter 配置
vim docker/jupyter_notebook_config.py

# 重新構建映像
make docker-build-dev
```

#### 添加新的 Python 套件
```bash
# 方式一：在容器中臨時安裝
docker-compose exec spark101-dev pip install package_name

# 方式二：修改 pyproject.toml 並重新構建
vim pyproject.toml
make docker-build-dev
```

#### 修改環境變數
```bash
# 編輯 docker-compose.yml
vim docker-compose.dev.yml

# 重新啟動
make docker-restart-dev
```

## 🚨 故障排除

### 常見問題

#### 1. 容器啟動失敗
```bash
# 查看詳細日誌
docker-compose logs spark101-dev

# 檢查埠號衝突
netstat -tlnp | grep 8888

# 重新構建映像
make docker-build-dev
```

#### 2. Jupyter 無法訪問
```bash
# 確認服務狀態
docker-compose ps

# 檢查容器日誌
docker-compose logs spark101-dev

# 重新啟動服務
make docker-restart-dev
```

#### 3. Spark 任務失敗
```bash
# 檢查 Java 版本
docker-compose exec spark101-dev java -version

# 檢查 Spark 配置
docker-compose exec spark101-dev python -c "import pyspark; print(pyspark.__version__)"

# 查看 Spark UI
open http://localhost:4040
```

#### 4. 記憶體不足
```bash
# 調整 Docker 記憶體限制
# 編輯 docker-compose.yml
services:
  spark101-dev:
    mem_limit: 4g
    memswap_limit: 4g
```

#### 5. 磁碟空間不足
```bash
# 清理 Docker 資源
make docker-clean

# 清理系統資源
docker system prune -a -f

# 清理 volumes
docker volume prune -f
```

### 效能優化

#### 1. 調整資源限制
```yaml
# docker-compose.yml
services:
  spark101-dev:
    mem_limit: 4g
    cpus: 2.0
    environment:
      - SPARK_WORKER_MEMORY=2g
      - SPARK_WORKER_CORES=2
```

#### 2. 優化映像大小
```bash
# 使用 multi-stage build
# 已在 Dockerfile 中實現

# 清理不必要的檔案
# 已在 .dockerignore 中配置
```

#### 3. 網路優化
```yaml
# docker-compose.yml
networks:
  spark101-network:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.0.0/16
```

## 📊 監控和日誌

### 系統監控
```bash
# 查看容器資源使用
docker stats

# 查看系統資源
docker system df

# 查看網路狀態
docker network ls
```

### 應用監控
```bash
# Spark UI
open http://localhost:4040

# Grafana 儀表板
open http://localhost:3000

# Prometheus 指標
open http://localhost:9090
```

### 日誌管理
```bash
# 查看實時日誌
docker-compose logs -f spark101-dev

# 查看特定服務日誌
docker-compose logs postgres-dev

# 日誌輪轉（在生產環境中）
docker-compose logs --tail=100 spark101-dev
```

## 🔐 安全考慮

### 開發環境安全
- 使用預設 token 和密碼（僅限開發）
- 所有服務僅綁定到 localhost
- 不要在生產環境中使用開發配置

### 生產環境部署
```bash
# 使用生產映像
make docker-build-prod

# 設置環境變數
export JUPYTER_TOKEN=your-secure-token
export POSTGRES_PASSWORD=your-secure-password

# 使用 HTTPS
# 配置反向代理（nginx、traefik）
```

## 📈 進階使用

### 叢集部署
```bash
# 使用 Docker Swarm
docker swarm init
docker stack deploy -c docker-compose.yml spark101

# 使用 Kubernetes
kubectl apply -f k8s/
```

### 持續整合
```yaml
# .github/workflows/docker.yml
name: Docker Build and Test
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Build and test
        run: |
          make docker-build-dev
          make docker-test-dev
```

### 擴展功能
```bash
# 添加新服務
vim docker-compose.yml

# 添加新映像
vim Dockerfile

# 添加新腳本
vim docker/start-notebook.sh
```

## 🎓 最佳實踐

### 1. 開發工作流程
```bash
# 每日工作流程
make docker-up-dev           # 啟動環境
make docker-shell-dev        # 進入開發
# ... 開發工作 ...
make docker-down-dev         # 停止環境
```

### 2. 版本控制
```bash
# 不要提交到版本控制
echo "data/" >> .gitignore
echo "logs/" >> .gitignore
echo ".env" >> .gitignore
```

### 3. 備份和恢復
```bash
# 備份資料
docker-compose exec postgres-dev pg_dump -U spark101 spark101 > backup.sql

# 恢復資料
docker-compose exec -T postgres-dev psql -U spark101 spark101 < backup.sql
```

## 🤝 貢獻指南

### 改進 Docker 配置
1. Fork 專案
2. 修改 Docker 相關文件
3. 測試修改
4. 提交 Pull Request

### 報告問題
請在 GitHub Issues 中報告 Docker 相關問題，並提供：
- 作業系統信息
- Docker 版本
- 錯誤日誌
- 重現步驟

---

🎉 **恭喜！你已經掌握了 Spark101 的 Docker 使用方法。開始你的 Spark 學習之旅吧！**

💡 **提示**：建議先從開發環境開始，熟悉後再使用完整環境進行進階練習。