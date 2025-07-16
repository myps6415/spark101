# Jupyter Notebook 配置文件
# 為 Spark101 優化的 Jupyter 配置

import os

# 基本配置
c.NotebookApp.ip = '0.0.0.0'
c.NotebookApp.port = 8888
c.NotebookApp.open_browser = False
c.NotebookApp.token = os.environ.get('JUPYTER_TOKEN', 'spark101')
c.NotebookApp.password = ''

# 允許 root 用戶運行
c.NotebookApp.allow_root = True

# 設定工作目錄
c.NotebookApp.notebook_dir = '/opt/spark101'

# 啟用擴展
c.NotebookApp.nbserver_extensions = {
    'jupyter_nbextensions_configurator': True,
}

# 內核配置
c.NotebookApp.kernel_spec_manager_class = 'jupyter_client.kernelspec.KernelSpecManager'

# 設定日誌級別
c.NotebookApp.log_level = 'INFO'

# 會話配置
c.NotebookApp.shutdown_no_activity_timeout = 60 * 60  # 1 小時
c.NotebookApp.terminals_enabled = True

# 文件上傳限制
c.NotebookApp.max_buffer_size = 1024 * 1024 * 1024  # 1GB

# 自定義 CSS 和 JS
c.NotebookApp.extra_static_paths = ['/opt/spark101/docker/static']
c.NotebookApp.extra_template_paths = ['/opt/spark101/docker/templates']

# Spark 相關配置
c.NotebookApp.tornado_settings = {
    'headers': {
        'Content-Security-Policy': "frame-ancestors 'self' *"
    }
}

# 環境變數
import os
os.environ['PYSPARK_PYTHON'] = '/usr/local/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/local/bin/python'
os.environ['SPARK_HOME'] = '/opt/spark101/.venv/lib/python3.11/site-packages/pyspark'