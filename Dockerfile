# Spark101 Docker Image
# 基於官方 Python 3.11 和 OpenJDK 11 構建統一的 Spark 學習環境

# 使用多階段構建來優化映像大小
FROM python:3.11-slim as base

# 設定環境變數
ENV DEBIAN_FRONTEND=noninteractive \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# 安裝系統依賴
RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    curl \
    wget \
    procps \
    net-tools \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 設定 Java 環境
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# 驗證 Java 安裝
RUN java -version

# 建立工作目錄
WORKDIR /opt/spark101

# 複製 Poetry 相關文件
COPY pyproject.toml poetry.lock* ./

# 安裝 Poetry
RUN pip install poetry==1.7.1

# 配置 Poetry
RUN poetry config virtualenvs.create false \
    && poetry config virtualenvs.in-project false

# 安裝 Python 依賴
RUN poetry install --no-dev --no-interaction --no-ansi

# 複製專案文件
COPY . .

# 設定權限
RUN chmod +x scripts/*.py scripts/*.sh 2>/dev/null || true

# 創建 Spark 工作目錄
RUN mkdir -p /opt/spark101/spark-warehouse \
    && mkdir -p /opt/spark101/logs \
    && mkdir -p /opt/spark101/tmp

# 設定環境變數
ENV SPARK_HOME=/opt/spark101/.venv/lib/python3.11/site-packages/pyspark \
    PYTHONPATH=/opt/spark101:$PYTHONPATH \
    PYSPARK_PYTHON=python \
    PYSPARK_DRIVER_PYTHON=python \
    SPARK_LOCAL_IP=0.0.0.0

# 健康檢查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import pyspark; print('Spark OK')" || exit 1

# 暴露端口
EXPOSE 4040 4041 8080 8081 8888

# 默認命令
CMD ["python", "test_spark_setup.py"]

# =============================================================================
# 開發環境階段
# =============================================================================
FROM base as development

# 安裝開發依賴
RUN poetry install --no-interaction --no-ansi

# 安裝 Jupyter 擴展
RUN pip install jupyter-contrib-nbextensions \
    && jupyter contrib nbextension install --system

# 設定 Jupyter 配置
RUN mkdir -p /root/.jupyter
COPY docker/jupyter_notebook_config.py /root/.jupyter/

# 創建啟動腳本
COPY docker/start-notebook.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/start-notebook.sh

# 開發環境默認命令
CMD ["start-notebook.sh"]

# =============================================================================
# 生產環境階段
# =============================================================================
FROM base as production

# 移除不必要的文件
RUN rm -rf tests/ \
    && rm -rf .git* \
    && rm -rf docker/ \
    && find . -name "*.pyc" -delete \
    && find . -name "__pycache__" -delete

# 創建非 root 用戶
RUN useradd -m -s /bin/bash spark101 \
    && chown -R spark101:spark101 /opt/spark101

USER spark101

# 生產環境默認命令
CMD ["python", "-m", "http.server", "8000"]