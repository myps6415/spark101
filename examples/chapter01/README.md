# 第1章：Spark 基礎概念

## 📚 學習目標

- 理解 Apache Spark 的基本概念
- 掌握 SparkSession 的創建和使用
- 學會第一個 Spark 程式的編寫
- 了解 Spark 的核心架構

## 🎯 本章內容

### 核心概念
- **Apache Spark** - 分散式計算框架
- **SparkSession** - Spark 應用程式的入口點
- **DataFrame** - 結構化數據抽象
- **分散式計算** - 在多台機器上處理大數據

### 檔案說明
- `hello_spark.py` - 第一個 Spark 程式，展示基本的 DataFrame 操作

## 🚀 開始學習

### 執行範例

```bash
# 使用 Poetry（推薦）
poetry run python examples/chapter01/hello_spark.py

# 或使用 Makefile
make run-chapter01

# 直接執行
python examples/chapter01/hello_spark.py
```

### 預期輸出
程式將顯示：
- Spark 版本信息
- 簡單的 DataFrame 操作結果
- 過濾操作示例

## 🔍 深入理解

### 程式碼解析

```python
# 創建 SparkSession
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .master("local[*]") \
    .getOrCreate()
```

- `appName()` - 設定應用程式名稱
- `master("local[*]")` - 本地模式，使用所有可用核心
- `getOrCreate()` - 獲取或創建 SparkSession

### 關鍵概念
1. **本地模式 vs 集群模式**
   - 本地模式：單機執行，適合開發測試
   - 集群模式：多機執行，適合生產環境

2. **延遲計算**
   - Spark 使用延遲計算，只有在 action 操作時才真正執行

## 📝 練習建議

### 基礎練習
1. 修改應用程式名稱
2. 嘗試不同的過濾條件
3. 添加更多的 DataFrame 操作

### 進階練習
1. 創建更複雜的數據結構
2. 嘗試不同的輸出格式
3. 探索 Spark UI（訪問 http://localhost:4040）

## 🛠️ 環境需求

- Python 3.8+
- Apache Spark 3.0+
- 至少 4GB RAM

## 📖 相關文檔

- [Spark Programming Guide](https://spark.apache.org/docs/latest/programming-guide.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/)
- [DataFrame API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html)

## 🔧 疑難排解

### 常見問題

**Q: 執行時出現 "Java not found" 錯誤**
A: 確保已安裝 Java 8 或更高版本：
```bash
java -version
```

**Q: 記憶體不足錯誤**
A: 調整 Spark 配置：
```python
spark = SparkSession.builder \
    .appName("HelloSpark") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

**Q: 無法訪問 Spark UI**
A: 確保端口 4040 沒有被占用，或檢查防火牆設置

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第2章：RDD 基本操作](../chapter02/README.md)
- 深入了解 Spark 的執行模型
- 探索更多 DataFrame 操作

## 💡 學習小貼士

1. **實際操作** - 動手執行每個範例
2. **查看日誌** - 關注 Spark 的執行日誌
3. **實驗修改** - 嘗試修改程式碼參數
4. **查看 UI** - 熟悉 Spark Web UI 界面