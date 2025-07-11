# 第1章練習：Spark 基礎概念

## 練習目標
通過實際操作練習，深入理解 Spark 的基本概念和 DataFrame 操作。

## 練習1：創建你的第一個 Spark 應用

### 任務描述
創建一個 Spark 應用，處理學生成績數據。

### 數據準備
創建包含以下學生信息的數據：
- 姓名、年齡、科系、成績

### 要求
1. 創建 SparkSession
2. 創建包含至少10名學生的 DataFrame
3. 顯示 DataFrame 的基本信息
4. 計算平均成績
5. 找出成績最高的學生

### 程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("學生成績分析") \
    .master("local[*]") \
    .getOrCreate()

# 創建學生數據
students_data = [
    # 在這裡添加你的學生數據
    # 格式：("姓名", 年齡, "科系", 成績)
]

columns = ["name", "age", "department", "score"]
students_df = spark.createDataFrame(students_data, columns)

# 完成以下任務：
# 1. 顯示 DataFrame
# 2. 顯示 Schema
# 3. 計算平均成績
# 4. 找出成績最高的學生

# 清理資源
spark.stop()
```

### 預期輸出
程式應該輸出：
- 學生數據表格
- DataFrame 的 Schema
- 平均成績
- 成績最高學生的信息

## 練習2：DataFrame 基本操作

### 任務描述
對學生數據進行更複雜的分析。

### 要求
1. 篩選出成績大於80分的學生
2. 按科系分組，計算每個科系的平均成績
3. 添加一個成績等級欄位（A: 90+, B: 80-89, C: 70-79, D: <70）
4. 統計每個等級的學生人數
5. 找出每個科系成績最高的學生

### 程式碼模板

```python
from pyspark.sql.functions import col, when, avg, count, max as spark_max
from pyspark.sql.window import Window

# 使用練習1的數據
# ...

# 1. 篩選高分學生
high_score_students = students_df.filter(col("score") > 80)

# 2. 按科系分組統計
dept_stats = students_df.groupBy("department").agg(
    avg("score").alias("avg_score"),
    count("*").alias("student_count")
)

# 3. 添加成績等級
students_with_grade = students_df.withColumn("grade",
    when(col("score") >= 90, "A")
    .when(col("score") >= 80, "B")
    .when(col("score") >= 70, "C")
    .otherwise("D")
)

# 4. 統計各等級人數
grade_stats = students_with_grade.groupBy("grade").count()

# 5. 找出每個科系的最高分學生
# 提示：使用 Window 函數
window_spec = Window.partitionBy("department").orderBy(col("score").desc())
# 完成你的程式碼

# 顯示結果
```

## 練習3：數據清洗和轉換

### 任務描述
處理包含缺失值和異常值的數據。

### 數據準備
創建包含以下問題的數據：
- 部分學生姓名為空
- 部分成績為負數或超過100
- 部分年齡異常

### 要求
1. 識別和處理缺失值
2. 處理異常值
3. 數據標準化
4. 創建清洗後的數據集

### 程式碼模板

```python
from pyspark.sql.functions import col, when, isnan, isnull, coalesce, lit

# 創建包含問題的數據
dirty_data = [
    ("Alice", 20, "Computer Science", 85),
    (None, 22, "Mathematics", 90),          # 缺失姓名
    ("Bob", 19, "Physics", -10),            # 異常成績
    ("Charlie", 150, "Chemistry", 75),      # 異常年齡
    ("Diana", 21, "Biology", 95),
    ("Eve", 20, None, 80),                  # 缺失科系
    ("Frank", 23, "Computer Science", 105), # 異常成績
]

columns = ["name", "age", "department", "score"]
dirty_df = spark.createDataFrame(dirty_data, columns)

# 完成以下任務：
# 1. 檢查每列的缺失值數量
# 2. 處理缺失值
# 3. 處理異常值
# 4. 驗證清洗後的數據

# 你的程式碼在這裡
```

## 練習4：綜合分析

### 任務描述
創建一個完整的學生成績分析報告。

### 要求
1. 讀取外部數據文件
2. 進行數據清洗
3. 執行統計分析
4. 生成分析報告
5. 保存結果到文件

### 程式碼模板

```python
# 創建完整的分析程式
def analyze_student_data(spark, input_path, output_path):
    """
    分析學生成績數據
    
    Args:
        spark: SparkSession
        input_path: 輸入文件路徑
        output_path: 輸出文件路徑
    """
    
    # 1. 讀取數據
    df = spark.read.option("header", "true").csv(input_path)
    
    # 2. 數據清洗
    # 你的程式碼
    
    # 3. 統計分析
    # 你的程式碼
    
    # 4. 生成報告
    # 你的程式碼
    
    # 5. 保存結果
    # 你的程式碼
    
    return analysis_results

# 使用函數
if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("學生成績分析系統") \
        .master("local[*]") \
        .getOrCreate()
    
    try:
        results = analyze_student_data(
            spark, 
            "../../datasets/students.csv", 
            "output/analysis_results"
        )
        
        print("分析完成！")
        print(f"結果已保存到 output/analysis_results")
        
    finally:
        spark.stop()
```

## 練習答案

### 練習1解答

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("學生成績分析") \
    .master("local[*]") \
    .getOrCreate()

# 創建學生數據
students_data = [
    ("Alice", 20, "Computer Science", 85),
    ("Bob", 19, "Mathematics", 92),
    ("Charlie", 21, "Physics", 78),
    ("Diana", 20, "Chemistry", 88),
    ("Eve", 22, "Biology", 90),
    ("Frank", 19, "Computer Science", 95),
    ("Grace", 20, "Mathematics", 82),
    ("Henry", 21, "Physics", 87),
    ("Ivy", 19, "Chemistry", 91),
    ("Jack", 20, "Biology", 84)
]

columns = ["name", "age", "department", "score"]
students_df = spark.createDataFrame(students_data, columns)

# 1. 顯示 DataFrame
print("學生數據:")
students_df.show()

# 2. 顯示 Schema
print("DataFrame Schema:")
students_df.printSchema()

# 3. 計算平均成績
avg_score = students_df.agg(avg("score")).collect()[0][0]
print(f"平均成績: {avg_score:.2f}")

# 4. 找出成績最高的學生
max_score = students_df.agg(spark_max("score")).collect()[0][0]
top_student = students_df.filter(col("score") == max_score).collect()[0]
print(f"成績最高的學生: {top_student.name}, 成績: {top_student.score}")

# 清理資源
spark.stop()
```

### 練習2解答

```python
from pyspark.sql.functions import col, when, avg, count, max as spark_max, row_number
from pyspark.sql.window import Window

# ... (使用練習1的數據)

# 1. 篩選高分學生
print("成績大於80分的學生:")
high_score_students = students_df.filter(col("score") > 80)
high_score_students.show()

# 2. 按科系分組統計
print("各科系統計:")
dept_stats = students_df.groupBy("department").agg(
    avg("score").alias("avg_score"),
    count("*").alias("student_count")
)
dept_stats.show()

# 3. 添加成績等級
students_with_grade = students_df.withColumn("grade",
    when(col("score") >= 90, "A")
    .when(col("score") >= 80, "B")
    .when(col("score") >= 70, "C")
    .otherwise("D")
)
print("學生成績等級:")
students_with_grade.show()

# 4. 統計各等級人數
print("各等級人數統計:")
grade_stats = students_with_grade.groupBy("grade").count()
grade_stats.show()

# 5. 找出每個科系的最高分學生
window_spec = Window.partitionBy("department").orderBy(col("score").desc())
top_students_by_dept = students_df.withColumn("rank", row_number().over(window_spec)) \
                                  .filter(col("rank") == 1) \
                                  .drop("rank")
print("各科系最高分學生:")
top_students_by_dept.show()
```

## 練習提示

1. **記住使用 `.show()` 查看結果**
2. **使用 `.printSchema()` 了解數據結構**
3. **善用 `.describe()` 獲取統計信息**
4. **使用 `.explain()` 理解執行計劃**
5. **記得在程式結束時調用 `spark.stop()`**

## 進階挑戰

1. **性能優化**：嘗試使用 `.cache()` 優化重複使用的 DataFrame
2. **數據可視化**：將結果轉換為 Pandas DataFrame 並創建圖表
3. **錯誤處理**：添加適當的異常處理
4. **參數化**：將硬編碼的值改為參數
5. **日誌記錄**：添加適當的日誌記錄

## 學習檢核

完成練習後，你應該能夠：
- [ ] 創建和配置 SparkSession
- [ ] 創建和操作 DataFrame
- [ ] 使用各種 DataFrame 函數
- [ ] 處理缺失值和異常值
- [ ] 進行基本的統計分析
- [ ] 保存和讀取不同格式的數據