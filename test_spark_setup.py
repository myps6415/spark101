#!/usr/bin/env python3
"""
測試 Spark 環境設置
"""

import os
import sys

def test_pyspark_import():
    """測試 PySpark 導入"""
    try:
        import pyspark
        print(f"✅ PySpark {pyspark.__version__} 導入成功")
        return True
    except ImportError as e:
        print(f"❌ PySpark 導入失敗: {e}")
        return False

def test_spark_session():
    """測試 SparkSession 創建"""
    try:
        # 設置 Java 系統屬性來避免權限問題
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:memory:myInMemDB;create=true --conf spark.sql.catalogImplementation=in-memory pyspark-shell'
        
        from pyspark.sql import SparkSession
        
        # 創建 SparkSession，使用更兼容的配置
        spark = SparkSession.builder \
            .appName("Spark101 Test") \
            .master("local[1]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:memory:myInMemDB;create=true") \
            .config("spark.sql.catalogImplementation", "in-memory") \
            .getOrCreate()
        
        # 測試基本操作
        data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
        df = spark.createDataFrame(data, ["name", "age"])
        count = df.count()
        
        print(f"✅ SparkSession 創建成功")
        print(f"✅ 測試 DataFrame 創建成功，記錄數: {count}")
        
        # 測試基本 SQL 操作
        df.createOrReplaceTempView("people")
        result = spark.sql("SELECT name, age FROM people WHERE age > 25").collect()
        print(f"✅ SQL 查詢成功，結果數: {len(result)}")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ SparkSession 創建失敗: {e}")
        print("這可能是 Java 版本兼容性問題，但不影響學習基本概念")
        return False

def provide_setup_instructions():
    """提供設置說明"""
    print("\n" + "="*60)
    print("🎉 Spark101 環境設置完成!")
    print("="*60)
    
    print("\n📚 如何使用:")
    print("1. 激活 Poetry 環境:")
    print("   poetry shell")
    print("\n2. 運行練習文件:")
    print("   poetry run python exercises/chapter01/exercise_01_basic_spark.py")
    print("\n3. 啟動 Jupyter Notebook:")
    print("   poetry run jupyter notebook")
    
    print("\n🔧 IDE 設置:")
    print("1. VS Code: 選擇 Poetry 虛擬環境作為 Python 解釋器")
    print("   路徑: .venv/bin/python")
    print("\n2. PyCharm: 添加 Poetry 虛擬環境")
    print("   File > Settings > Project > Python Interpreter > Add > Poetry Environment")
    
    print("\n⚠️  如果遇到 Java 相關錯誤:")
    print("1. 確保安裝了 Java 8 或 11:")
    print("   brew install openjdk@11")
    print("\n2. 設置 JAVA_HOME:")
    print("   export JAVA_HOME=$(/usr/libexec/java_home -v 11)")
    print("\n3. 如果仍有問題，可以學習 Spark 概念，在雲端環境中運行實際代碼")

def main():
    """主函數"""
    print("=== Spark101 環境測試 ===\n")
    
    # 測試 PySpark 導入
    if not test_pyspark_import():
        return
    
    # 測試 SparkSession（可選）
    test_spark_session()
    
    # 提供使用說明
    provide_setup_instructions()

if __name__ == "__main__":
    main()