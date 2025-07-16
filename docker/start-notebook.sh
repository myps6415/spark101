#!/bin/bash
# Jupyter Notebook 啟動腳本

set -e

# 顯示啟動信息
echo "🚀 啟動 Spark101 Jupyter 環境..."
echo "📊 Java 版本: $(java -version 2>&1 | head -1)"
echo "🐍 Python 版本: $(python --version)"
echo "⚡ Spark 版本: $(python -c 'import pyspark; print(pyspark.__version__)')"

# 設定環境變數
export JAVA_HOME=${JAVA_HOME:-/usr/lib/jvm/java-11-openjdk-amd64}
export SPARK_HOME=${SPARK_HOME:-/opt/spark101/.venv/lib/python3.11/site-packages/pyspark}
export PYTHONPATH=/opt/spark101:$PYTHONPATH
export PYSPARK_PYTHON=python
export PYSPARK_DRIVER_PYTHON=python

# 創建必要的目錄
mkdir -p /opt/spark101/logs
mkdir -p /opt/spark101/spark-warehouse
mkdir -p /opt/spark101/tmp

# 設定權限
chmod -R 755 /opt/spark101/logs
chmod -R 755 /opt/spark101/spark-warehouse

# 測試 Spark 環境
echo "🧪 測試 Spark 環境..."
python -c "
import pyspark
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('DockerTest').master('local[*]').getOrCreate()
    print('✅ Spark 環境測試成功')
    spark.stop()
except Exception as e:
    print(f'❌ Spark 環境測試失敗: {e}')
"

# 啟動 Jupyter Notebook
echo "📓 啟動 Jupyter Notebook..."
echo "🌐 訪問地址: http://localhost:8888"
echo "🔐 Token: ${JUPYTER_TOKEN:-spark101}"
echo "📂 工作目錄: /opt/spark101"

# 檢查是否要啟動 Lab 或 Notebook
if [ "${JUPYTER_ENABLE_LAB:-no}" = "yes" ]; then
    echo "🔬 啟動 JupyterLab..."
    exec jupyter lab \
        --ip=0.0.0.0 \
        --port=8888 \
        --no-browser \
        --allow-root \
        --NotebookApp.token="${JUPYTER_TOKEN:-spark101}" \
        --NotebookApp.notebook_dir="/opt/spark101"
else
    echo "📔 啟動 Jupyter Notebook..."
    exec jupyter notebook \
        --ip=0.0.0.0 \
        --port=8888 \
        --no-browser \
        --allow-root \
        --NotebookApp.token="${JUPYTER_TOKEN:-spark101}" \
        --NotebookApp.notebook_dir="/opt/spark101"
fi