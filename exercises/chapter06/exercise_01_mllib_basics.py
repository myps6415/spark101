#!/usr/bin/env python3
"""
第6章練習1：MLlib 機器學習基礎
機器學習管道和模型訓練練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import numpy as np

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("MLlib基礎練習") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 第6章練習1：MLlib 機器學習基礎 ===")
    
    # 1. 讀取員工數據（使用 datasets 資料夾中的數據）
    print("\n1. 讀取和探索數據:")
    
    # 讀取員工數據
    employees_df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("../../datasets/employees_large.csv")
    
    print("數據概覽:")
    employees_df.show(10)
    employees_df.printSchema()
    
    print("\n數據統計:")
    employees_df.describe().show()
    
    # 2. 數據清洗和預處理
    print("\n2. 數據清洗和預處理:")
    
    # 檢查缺失值
    missing_count = employees_df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) 
                                       for c in employees_df.columns])
    print("缺失值統計:")
    missing_count.show()
    
    # 創建目標變量（高薪員工：薪資 > 平均薪資）
    avg_salary = employees_df.agg({"salary": "avg"}).collect()[0][0]
    print(f"平均薪資: {avg_salary:.2f}")
    
    # 添加目標變量
    employees_with_target = employees_df.withColumn(
        "high_salary", 
        when(col("salary") > avg_salary, 1).otherwise(0)
    )
    
    # 3. 特徵工程
    print("\n3. 特徵工程:")
    
    # 字符串索引化
    dept_indexer = StringIndexer(inputCol="department", outputCol="department_indexed")
    city_indexer = StringIndexer(inputCol="city", outputCol="city_indexed")
    
    # 特徵向量化
    feature_cols = ["age", "years_experience", "performance_rating", "department_indexed", "city_indexed"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # 特徵標準化
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    
    # 4. 分類任務 - 預測高薪員工
    print("\n4. 分類任務 - 預測高薪員工:")
    
    # 建立機器學習管道
    lr = LogisticRegression(featuresCol="scaled_features", labelCol="high_salary")
    
    # 創建管道
    pipeline = Pipeline(stages=[dept_indexer, city_indexer, assembler, scaler, lr])
    
    # 分割數據
    train_df, test_df = employees_with_target.randomSplit([0.8, 0.2], seed=42)
    
    print(f"訓練數據: {train_df.count()} 筆")
    print(f"測試數據: {test_df.count()} 筆")
    
    # 訓練模型
    print("\n訓練邏輯回歸模型...")
    model = pipeline.fit(train_df)
    
    # 預測
    predictions = model.transform(test_df)
    
    # 顯示預測結果
    print("\n預測結果:")
    predictions.select("name", "salary", "high_salary", "probability", "prediction") \
        .show(10, truncate=False)
    
    # 5. 模型評估
    print("\n5. 模型評估:")
    
    # 二元分類評估
    evaluator = BinaryClassificationEvaluator(labelCol="high_salary", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)
    print(f"AUC: {auc:.4f}")
    
    # 準確率評估
    accuracy_evaluator = MulticlassClassificationEvaluator(
        labelCol="high_salary", predictionCol="prediction", metricName="accuracy"
    )
    accuracy = accuracy_evaluator.evaluate(predictions)
    print(f"準確率: {accuracy:.4f}")
    
    # 6. 隨機森林模型比較
    print("\n6. 隨機森林模型比較:")
    
    rf = RandomForestClassifier(featuresCol="scaled_features", labelCol="high_salary", numTrees=10)
    rf_pipeline = Pipeline(stages=[dept_indexer, city_indexer, assembler, scaler, rf])
    
    rf_model = rf_pipeline.fit(train_df)
    rf_predictions = rf_model.transform(test_df)
    
    rf_auc = evaluator.evaluate(rf_predictions)
    rf_accuracy = accuracy_evaluator.evaluate(rf_predictions)
    
    print(f"隨機森林 AUC: {rf_auc:.4f}")
    print(f"隨機森林 準確率: {rf_accuracy:.4f}")
    
    # 7. 回歸任務 - 預測薪資
    print("\n7. 回歸任務 - 預測薪資:")
    
    # 線性回歸模型
    lr_reg = LinearRegression(featuresCol="scaled_features", labelCol="salary")
    regression_pipeline = Pipeline(stages=[dept_indexer, city_indexer, assembler, scaler, lr_reg])
    
    regression_model = regression_pipeline.fit(train_df)
    salary_predictions = regression_model.transform(test_df)
    
    # 顯示回歸結果
    print("\n薪資預測結果:")
    salary_predictions.select("name", "salary", "prediction").show(10)
    
    # 計算回歸指標
    from pyspark.ml.evaluation import RegressionEvaluator
    
    reg_evaluator = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="rmse")
    rmse = reg_evaluator.evaluate(salary_predictions)
    print(f"RMSE: {rmse:.2f}")
    
    r2_evaluator = RegressionEvaluator(labelCol="salary", predictionCol="prediction", metricName="r2")
    r2 = r2_evaluator.evaluate(salary_predictions)
    print(f"R²: {r2:.4f}")
    
    # 8. 特徵重要性分析
    print("\n8. 特徵重要性分析:")
    
    if hasattr(rf_model.stages[-1], 'featureImportances'):
        importances = rf_model.stages[-1].featureImportances
        feature_importance = [(feature_cols[i], float(importances[i])) 
                            for i in range(len(feature_cols))]
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        
        print("特徵重要性排序:")
        for feature, importance in feature_importance:
            print(f"{feature}: {importance:.4f}")
    
    # 清理資源
    spark.stop()
    print("\n練習完成！")

# 輔助函數：計算缺失值
def count(c):
    from pyspark.sql.functions import sum as spark_sum, when, isnan, col
    return spark_sum(when(isnan(c) | col(c).isNull(), 1).otherwise(0))

if __name__ == "__main__":
    main()