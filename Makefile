# Spark 101 專案管理

.PHONY: install dev test clean run-examples jupyter format lint help validate setup-data

# 安裝依賴
install:
	@echo "📦 安裝專案依賴..."
	poetry install

# 開發環境設置
dev: install
	@echo "🔧 設置開發環境..."
	poetry install --with dev

# 環境初始化
bootstrap: dev
	@echo "🔥 初始化 Spark 101 環境..."
	poetry run python scripts/bootstrap.py

# 設置數據集
setup-data:
	@echo "📊 設置示例數據集..."
	@mkdir -p datasets
	@echo "數據集準備完成"

# 運行所有測試
test:
	@echo "🧪 運行所有測試..."
	poetry run pytest tests/ -v

# 運行特定章節測試
test-chapter01:
	@echo "🧪 運行第1章測試..."
	poetry run pytest tests/test_chapter01.py -v

test-chapter02:
	@echo "🧪 運行第2章測試..."
	poetry run pytest tests/test_chapter02.py -v

test-dataframe:
	@echo "🧪 運行 DataFrame 測試..."
	poetry run pytest tests/test_dataframe_operations.py -v

# 驗證所有範例
validate:
	@echo "✅ 驗證所有範例..."
	poetry run python examples/chapter01/hello_spark.py
	poetry run python examples/chapter02/rdd_basics.py
	poetry run python examples/chapter03/dataframe_basics.py
	@echo "所有範例驗證完成"

# 運行範例
run-examples: run-chapter01

run-chapter01:
	@echo "🚀 運行第1章範例..."
	poetry run python examples/chapter01/hello_spark.py

run-chapter02:
	@echo "🚀 運行第2章範例..."
	poetry run python examples/chapter02/rdd_basics.py

run-chapter03:
	@echo "🚀 運行第3章範例..."
	poetry run python examples/chapter03/dataframe_basics.py

run-chapter04:
	@echo "🚀 運行第4章範例..."
	poetry run python examples/chapter04/spark_sql_basics.py

run-chapter05:
	@echo "🚀 運行第5章範例..."
	poetry run python examples/chapter05/streaming_basics.py

run-chapter06:
	@echo "🚀 運行第6章範例..."
	poetry run python examples/chapter06/mllib_basics.py

run-chapter07:
	@echo "🚀 運行第7章範例..."
	poetry run python examples/chapter07/performance_tuning.py

run-chapter08:
	@echo "🚀 運行第8章範例..."
	poetry run python examples/chapter08/log_analyzer.py

# 啟動 Jupyter
jupyter:
	@echo "📓 啟動 Jupyter Notebook..."
	poetry run jupyter notebook notebooks/

# 啟動 JupyterLab
jupyterlab:
	@echo "📓 啟動 JupyterLab..."
	poetry run jupyter lab notebooks/

# 轉換 Python 腳本為 Notebooks
notebooks:
	@echo "📔 轉換 Python 腳本為 Notebooks..."
	@echo "請使用現有的 notebooks 目錄"

# 代碼格式化
format:
	@echo "🎨 格式化代碼..."
	poetry run black .
	poetry run isort .

# 代碼檢查
lint:
	@echo "🔍 代碼檢查..."
	poetry run flake8 . --max-line-length=88 --extend-ignore=E203,W503
	poetry run mypy . --ignore-missing-imports

# 清理環境
clean:
	@echo "🧹 清理環境..."
	rm -rf .pytest_cache __pycache__ **/__pycache__
	rm -rf .mypy_cache
	rm -rf dist/ build/
	find . -name "*.pyc" -delete
	find . -name "*.pyo" -delete

# 深度清理
clean-all: clean
	@echo "🧹 深度清理環境..."
	poetry env remove --all
	rm -rf .venv

# 檢查環境
check-env:
	@echo "🔍 檢查環境..."
	poetry run python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')"
	poetry run python -c "import sys; print(f'Python version: {sys.version}')"

# 生成需求文件
requirements:
	@echo "📋 生成需求文件..."
	poetry export -f requirements.txt --output requirements.txt --without-hashes

# 文檔生成
docs:
	@echo "📖 生成文檔..."
	@echo "文檔已包含在 README.md 和各章節中"

# 性能基準測試
benchmark:
	@echo "⚡ 運行性能基準測試..."
	poetry run python examples/chapter07/performance_tuning.py

# 安全掃描
security:
	@echo "🔒 安全掃描..."
	poetry run safety check

# 項目統計
stats:
	@echo "📊 項目統計..."
	@echo "Python 文件數："
	@find . -name "*.py" -not -path "./.venv/*" | wc -l
	@echo "Jupyter Notebook 文件數："
	@find . -name "*.ipynb" | wc -l
	@echo "代碼行數："
	@find . -name "*.py" -not -path "./.venv/*" -exec wc -l {} + | tail -1

# 全面檢查
check-all: lint test validate
	@echo "✅ 全面檢查完成"

# 幫助
# Docker 相關命令
docker-build:
	@echo "🐳 構建 Docker 映像..."
	docker build -t spark101:latest .

docker-build-dev:
	@echo "🐳 構建開發環境 Docker 映像..."
	docker build -t spark101:dev --target development .

docker-build-prod:
	@echo "🐳 構建生產環境 Docker 映像..."
	docker build -t spark101:prod --target production .

docker-up:
	@echo "🐳 啟動完整 Docker 環境..."
	docker-compose up -d

docker-up-dev:
	@echo "🐳 啟動開發 Docker 環境..."
	docker-compose -f docker-compose.dev.yml up -d

docker-down:
	@echo "🐳 停止 Docker 環境..."
	docker-compose down

docker-down-dev:
	@echo "🐳 停止開發 Docker 環境..."
	docker-compose -f docker-compose.dev.yml down

docker-logs:
	@echo "🐳 查看 Docker 日誌..."
	docker-compose logs -f

docker-logs-dev:
	@echo "🐳 查看開發 Docker 日誌..."
	docker-compose -f docker-compose.dev.yml logs -f

docker-shell:
	@echo "🐳 進入 Docker 容器..."
	docker-compose exec spark101 bash

docker-shell-dev:
	@echo "🐳 進入開發 Docker 容器..."
	docker-compose -f docker-compose.dev.yml exec spark101-dev bash

docker-test:
	@echo "🐳 在 Docker 中運行測試..."
	docker-compose exec spark101 python test_spark_setup.py

docker-test-dev:
	@echo "🐳 在開發 Docker 中運行測試..."
	docker-compose -f docker-compose.dev.yml exec spark101-dev python test_spark_setup.py

docker-clean:
	@echo "🐳 清理 Docker 資源..."
	docker-compose down -v --rmi local --remove-orphans
	docker system prune -f

docker-restart:
	@echo "🐳 重啟 Docker 環境..."
	docker-compose restart

docker-restart-dev:
	@echo "🐳 重啟開發 Docker 環境..."
	docker-compose -f docker-compose.dev.yml restart

help:
	@echo "Spark 101 專案管理命令："
	@echo ""
	@echo "🚀 快速開始："
	@echo "  install        - 安裝依賴"
	@echo "  dev            - 設置開發環境"
	@echo "  bootstrap      - 初始化環境"
	@echo "  setup-data     - 設置示例數據集"
	@echo ""
	@echo "🏃 運行範例："
	@echo "  run-examples   - 運行第1章範例"
	@echo "  run-chapter01  - 運行第1章範例"
	@echo "  run-chapter02  - 運行第2章範例"
	@echo "  run-chapter03  - 運行第3章範例"
	@echo "  run-chapter04  - 運行第4章範例"
	@echo "  run-chapter05  - 運行第5章範例"
	@echo "  run-chapter06  - 運行第6章範例"
	@echo "  run-chapter07  - 運行第7章範例"
	@echo "  run-chapter08  - 運行第8章範例"
	@echo "  validate       - 驗證所有範例"
	@echo ""
	@echo "🧪 測試："
	@echo "  test           - 運行所有測試"
	@echo "  test-chapter01 - 運行第1章測試"
	@echo "  test-chapter02 - 運行第2章測試"
	@echo "  test-dataframe - 運行 DataFrame 測試"
	@echo ""
	@echo "📓 Jupyter："
	@echo "  jupyter        - 啟動 Jupyter Notebook"
	@echo "  jupyterlab     - 啟動 JupyterLab"
	@echo "  notebooks      - 轉換腳本為 Notebooks"
	@echo ""
	@echo "🐳 Docker："
	@echo "  docker-build   - 構建 Docker 映像"
	@echo "  docker-up      - 啟動完整 Docker 環境"
	@echo "  docker-up-dev  - 啟動開發 Docker 環境"
	@echo "  docker-down    - 停止 Docker 環境"
	@echo "  docker-shell   - 進入 Docker 容器"
	@echo "  docker-test    - 在 Docker 中運行測試"
	@echo "  docker-logs    - 查看 Docker 日誌"
	@echo "  docker-clean   - 清理 Docker 資源"
	@echo ""
	@echo "🛠️ 開發工具："
	@echo "  format         - 格式化代碼"
	@echo "  lint           - 代碼檢查"
	@echo "  check-env      - 檢查環境"
	@echo "  check-all      - 全面檢查"
	@echo ""
	@echo "🧹 清理："
	@echo "  clean          - 清理臨時文件"
	@echo "  clean-all      - 深度清理"
	@echo ""
	@echo "📊 其他："
	@echo "  requirements   - 生成需求文件"
	@echo "  docs           - 生成文檔"
	@echo "  benchmark      - 性能基準測試"
	@echo "  security       - 安全掃描"
	@echo "  stats          - 項目統計"