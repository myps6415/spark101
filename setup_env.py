#!/usr/bin/env python3
"""
快速設置 Spark101 開發環境
"""

import os
import subprocess
import sys


def install_requirements():
    """安裝基本要求"""
    try:
        print("正在安裝 PySpark 和相關依賴...")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-r", "requirements.txt"]
        )
        print("✓ 依賴安裝完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ 安裝失敗: {e}")
        return False


def test_pyspark():
    """測試 PySpark 安裝"""
    try:
        import pyspark

        print(f"✓ PySpark {pyspark.__version__} 安裝成功")

        # 測試 SparkSession
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.appName("test").master("local[1]").getOrCreate()
        print("✓ SparkSession 創建成功")

        # 創建測試 DataFrame
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        print(f"✓ 測試 DataFrame 創建成功，記錄數: {df.count()}")

        spark.stop()
        print("✓ 環境測試完成")
        return True

    except ImportError as e:
        print(f"✗ PySpark 導入失敗: {e}")
        return False
    except Exception as e:
        print(f"✗ 測試失敗: {e}")
        return False


def setup_jupyter_kernel():
    """設置 Jupyter 內核"""
    try:
        # 安裝 ipykernel
        subprocess.check_call([sys.executable, "-m", "pip", "install", "ipykernel"])

        # 創建內核
        subprocess.check_call(
            [
                sys.executable,
                "-m",
                "ipykernel",
                "install",
                "--user",
                "--name",
                "spark101",
                "--display-name",
                "Spark101",
            ]
        )
        print("✓ Jupyter 內核設置完成")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Jupyter 內核設置失敗: {e}")
        return False


def create_spark_config():
    """創建 Spark 配置"""
    spark_defaults = """
# Spark 配置
spark.app.name=Spark101
spark.master=local[*]
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.sql.execution.arrow.pyspark.enabled=true
"""

    try:
        os.makedirs("conf", exist_ok=True)
        with open("conf/spark-defaults.conf", "w") as f:
            f.write(spark_defaults)
        print("✓ Spark 配置文件創建完成")
        return True
    except Exception as e:
        print(f"✗ 配置文件創建失敗: {e}")
        return False


def main():
    """主函數"""
    print("=== Spark101 環境設置 ===")

    # 切換到項目目錄
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)

    success = True

    # 安裝依賴
    if not install_requirements():
        success = False

    # 測試 PySpark
    if not test_pyspark():
        success = False

    # 設置 Jupyter 內核
    if not setup_jupyter_kernel():
        print("⚠️  Jupyter 內核設置失敗，但不影響基本使用")

    # 創建配置
    if not create_spark_config():
        print("⚠️  配置文件創建失敗，但不影響基本使用")

    if success:
        print("\n✅ 環境設置完成！")
        print("\n使用說明：")
        print("1. 運行練習：python exercises/chapter01/exercise_01_basic_spark.py")
        print("2. 啟動 Jupyter：jupyter notebook")
        print("3. 選擇 Spark101 內核")
    else:
        print("\n❌ 環境設置失敗，請檢查錯誤信息")


if __name__ == "__main__":
    main()
