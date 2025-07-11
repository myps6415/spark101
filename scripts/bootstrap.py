#!/usr/bin/env python3
"""
Spark 101 環境設置腳本 (Poetry 版本)
"""

import subprocess

def check_poetry():
    """檢查 Poetry 是否已安裝"""
    try:
        result = subprocess.run(['poetry', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Poetry 已安裝")
            print(f"   版本: {result.stdout.strip()}")
            return True
        else:
            print("❌ Poetry 未安裝")
            return False
    except FileNotFoundError:
        print("❌ Poetry 未找到")
        return False

def check_java():
    """檢查 Java 版本"""
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Java 已安裝")
            return True
        else:
            print("❌ Java 未安裝")
            return False
    except FileNotFoundError:
        print("❌ Java 未找到")
        return False

def install_poetry():
    """安裝 Poetry"""
    print("🔧 正在安裝 Poetry...")
    try:
        # 使用官方安裝腳本
        subprocess.run([
            'curl', '-sSL', 'https://install.python-poetry.org', '|', 'python3', '-'
        ], shell=True, check=True)
        print("✅ Poetry 安裝完成")
        print("請重新啟動終端或執行: source ~/.bashrc")
        return True
    except subprocess.CalledProcessError:
        print("❌ Poetry 安裝失敗")
        print("請手動安裝 Poetry: https://python-poetry.org/docs/#installation")
        return False

def install_dependencies():
    """使用 Poetry 安裝依賴"""
    try:
        print("📦 正在安裝專案依賴...")
        subprocess.run(['poetry', 'install'], check=True)
        print("✅ 依賴安裝完成")
        return True
    except subprocess.CalledProcessError:
        print("❌ 依賴安裝失敗")
        return False

def main():
    print("🔥 Spark 101 環境設置 (Poetry)")
    print("=" * 35)
    
    # 檢查 Poetry
    if not check_poetry():
        choice = input("是否要安裝 Poetry? (y/n): ").lower()
        if choice == 'y':
            if not install_poetry():
                return
        else:
            print("請先安裝 Poetry: https://python-poetry.org/docs/#installation")
            return
    
    # 檢查 Java
    if not check_java():
        print("請先安裝 Java 8 或更高版本")
        print("下載連結: https://www.oracle.com/java/technologies/javase-downloads.html")
        return
    
    # 安裝依賴
    if not install_dependencies():
        return
    
    print("\n🎉 環境設置完成！")
    print("現在可以開始學習 Spark 了：")
    print("1. 激活虛擬環境: poetry shell")
    print("2. 運行第一個範例: poetry run python examples/chapter01/hello_spark.py")
    print("3. 啟動 Jupyter Notebook: poetry run jupyter notebook")
    print("4. 啟動 PySpark Shell: poetry run pyspark")
    print("\n📝 常用 Poetry 命令:")
    print("- poetry install    # 安裝依賴")
    print("- poetry shell      # 激活虛擬環境")
    print("- poetry add <pkg>  # 添加新依賴")
    print("- poetry run <cmd>  # 在虛擬環境中運行命令")

if __name__ == "__main__":
    main()