#!/usr/bin/env python3
"""
第6章：MLlib 機器學習 - 基礎機器學習
學習 Spark MLlib 的基本機器學習算法和管道
"""

import numpy as np
from pyspark.ml import Pipeline
from pyspark.ml.classification import (DecisionTreeClassifier,
                                       LogisticRegression,
                                       RandomForestClassifier)
from pyspark.ml.clustering import GaussianMixture, KMeans
from pyspark.ml.evaluation import (BinaryClassificationEvaluator,
                                   MulticlassClassificationEvaluator,
                                   RegressionEvaluator)
from pyspark.ml.feature import (IDF, PCA, HashingTF, MinMaxScaler,
                                OneHotEncoder, StandardScaler,
                                StopWordsRemover, StringIndexer, Tokenizer,
                                VectorAssembler)
from pyspark.ml.regression import (DecisionTreeRegressor, LinearRegression,
                                   RandomForestRegressor)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, isnull, rand, when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("MLlib Basics")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("🤖 MLlib 機器學習基礎示範")
    print("=" * 40)

    # 1. 準備數據
    print("\n1️⃣ 準備數據")

    # 創建分類問題的示例數據
    classification_data = [
        (1, 25, 50000, 1, 0, 1),  # age, income, experience, gender, education, target
        (2, 35, 75000, 2, 1, 1),
        (3, 45, 120000, 5, 2, 1),
        (4, 22, 35000, 0, 0, 0),
        (5, 28, 45000, 1, 1, 0),
        (6, 55, 150000, 10, 2, 1),
        (7, 19, 25000, 0, 0, 0),
        (8, 42, 95000, 8, 1, 1),
        (9, 38, 85000, 6, 2, 1),
        (10, 26, 40000, 2, 0, 0),
    ]

    # 擴展數據集
    import random

    extended_data = []
    for i in range(1000):
        age = random.randint(18, 65)
        income = random.randint(20000, 200000)
        experience = max(0, age - 22 + random.randint(-3, 3))
        gender = random.randint(0, 1)
        education = random.randint(0, 2)

        # 基於特徵生成目標值（有一定的模式）
        target = (
            1
            if (income > 60000 and experience > 3)
            or (education == 2 and income > 45000)
            else 0
        )
        # 添加一些隨機性
        if random.random() < 0.1:
            target = 1 - target

        extended_data.append((i, age, income, experience, gender, education, target))

    schema = StructType(
        [
            StructField("id", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("income", IntegerType(), True),
            StructField("experience", IntegerType(), True),
            StructField("gender", IntegerType(), True),
            StructField("education", IntegerType(), True),
            StructField("target", IntegerType(), True),
        ]
    )

    df = spark.createDataFrame(extended_data, schema)
    print("原始數據:")
    df.show(10)
    df.printSchema()

    # 數據統計
    print("數據統計:")
    df.describe().show()

    # 2. 數據預處理
    print("\n2️⃣ 數據預處理")

    # 檢查缺失值
    print("檢查缺失值:")
    df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).agg(
        *[col(c).sum().alias(f"{c}_nulls") for c in df.columns]
    ).show()

    # 特徵工程
    print("特徵工程:")

    # 創建年齡分組
    df_processed = df.withColumn(
        "age_group", when(col("age") < 30, 0).when(col("age") < 45, 1).otherwise(2)
    )

    # 創建收入分組
    df_processed = df_processed.withColumn(
        "income_group",
        when(col("income") < 50000, 0).when(col("income") < 100000, 1).otherwise(2),
    )

    print("處理後的數據:")
    df_processed.show(10)

    # 3. 特徵向量化
    print("\n3️⃣ 特徵向量化")

    # 選擇特徵列
    feature_cols = [
        "age",
        "income",
        "experience",
        "gender",
        "education",
        "age_group",
        "income_group",
    ]

    # 創建特徵向量
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    df_features = assembler.transform(df_processed)
    print("特徵向量:")
    df_features.select("features", "target").show(5, truncate=False)

    # 4. 標準化
    print("\n4️⃣ 標準化")

    # 特徵標準化
    scaler = StandardScaler(
        inputCol="features", outputCol="scaled_features", withStd=True, withMean=True
    )

    scaler_model = scaler.fit(df_features)
    df_scaled = scaler_model.transform(df_features)

    print("標準化後的特徵:")
    df_scaled.select("scaled_features", "target").show(5, truncate=False)

    # 5. 數據分割
    print("\n5️⃣ 數據分割")

    # 分割訓練集和測試集
    train_data, test_data = df_scaled.randomSplit([0.8, 0.2], seed=42)

    print(f"訓練集大小: {train_data.count()}")
    print(f"測試集大小: {test_data.count()}")

    # 6. 邏輯回歸
    print("\n6️⃣ 邏輯回歸")

    # 創建邏輯回歸模型
    lr = LogisticRegression(
        featuresCol="scaled_features", labelCol="target", predictionCol="prediction"
    )

    # 訓練模型
    lr_model = lr.fit(train_data)

    # 預測
    lr_predictions = lr_model.transform(test_data)

    print("邏輯回歸預測結果:")
    lr_predictions.select("target", "prediction", "probability").show(10)

    # 評估模型
    evaluator = BinaryClassificationEvaluator(
        labelCol="target", rawPredictionCol="rawPrediction", metricName="areaUnderROC"
    )

    lr_auc = evaluator.evaluate(lr_predictions)
    print(f"邏輯回歸 AUC: {lr_auc:.4f}")

    # 7. 決策樹
    print("\n7️⃣ 決策樹")

    # 決策樹分類器
    dt = DecisionTreeClassifier(featuresCol="scaled_features", labelCol="target")

    dt_model = dt.fit(train_data)
    dt_predictions = dt_model.transform(test_data)

    dt_auc = evaluator.evaluate(dt_predictions)
    print(f"決策樹 AUC: {dt_auc:.4f}")

    # 特徵重要性
    print("特徵重要性:")
    for i, importance in enumerate(dt_model.featureImportances.toArray()):
        print(f"  {feature_cols[i]}: {importance:.4f}")

    # 8. 隨機森林
    print("\n8️⃣ 隨機森林")

    # 隨機森林分類器
    rf = RandomForestClassifier(
        featuresCol="scaled_features", labelCol="target", numTrees=10
    )

    rf_model = rf.fit(train_data)
    rf_predictions = rf_model.transform(test_data)

    rf_auc = evaluator.evaluate(rf_predictions)
    print(f"隨機森林 AUC: {rf_auc:.4f}")

    # 9. 回歸問題
    print("\n9️⃣ 回歸問題")

    # 創建回歸問題的數據（預測收入）
    regression_data = df_scaled.select("scaled_features", col("income").alias("label"))

    # 分割數據
    reg_train, reg_test = regression_data.randomSplit([0.8, 0.2], seed=42)

    # 線性回歸
    linear_reg = LinearRegression(featuresCol="scaled_features", labelCol="label")

    lr_reg_model = linear_reg.fit(reg_train)
    lr_reg_predictions = lr_reg_model.transform(reg_test)

    print("線性回歸預測結果:")
    lr_reg_predictions.select("label", "prediction").show(10)

    # 評估回歸模型
    reg_evaluator = RegressionEvaluator(
        labelCol="label", predictionCol="prediction", metricName="rmse"
    )

    lr_rmse = reg_evaluator.evaluate(lr_reg_predictions)
    print(f"線性回歸 RMSE: {lr_rmse:.2f}")

    # 10. 聚類分析
    print("\n🔟 聚類分析")

    # K-means 聚類
    kmeans = KMeans(
        featuresCol="scaled_features", predictionCol="cluster", k=3, seed=42
    )

    kmeans_model = kmeans.fit(df_scaled)
    kmeans_predictions = kmeans_model.transform(df_scaled)

    print("K-means 聚類結果:")
    kmeans_predictions.select("cluster", "target").show(10)

    # 聚類中心
    print("聚類中心:")
    centers = kmeans_model.clusterCenters()
    for i, center in enumerate(centers):
        print(f"  Cluster {i}: {center}")

    # 11. 機器學習管道
    print("\n1️⃣1️⃣ 機器學習管道")

    # 創建完整的機器學習管道
    pipeline = Pipeline(stages=[assembler, scaler, lr])

    # 訓練管道
    pipeline_model = pipeline.fit(train_data)

    # 使用管道進行預測
    pipeline_predictions = pipeline_model.transform(test_data)

    pipeline_auc = evaluator.evaluate(pipeline_predictions)
    print(f"管道模型 AUC: {pipeline_auc:.4f}")

    # 12. 超參數調優
    print("\n1️⃣2️⃣ 超參數調優")

    # 創建參數網格
    paramGrid = (
        ParamGridBuilder()
        .addGrid(lr.regParam, [0.01, 0.1, 1.0])
        .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
        .build()
    )

    # 交叉驗證
    crossval = CrossValidator(
        estimator=lr, estimatorParamMaps=paramGrid, evaluator=evaluator, numFolds=3
    )

    # 訓練
    cv_model = crossval.fit(train_data)

    # 最佳模型預測
    cv_predictions = cv_model.transform(test_data)

    cv_auc = evaluator.evaluate(cv_predictions)
    print(f"交叉驗證最佳模型 AUC: {cv_auc:.4f}")

    # 13. 文本挖掘
    print("\n1️⃣3️⃣ 文本挖掘")

    # 創建文本數據
    text_data = [
        (1, "machine learning is awesome"),
        (2, "spark mllib is great for big data"),
        (3, "python and scala are popular languages"),
        (4, "data science requires good algorithms"),
        (5, "artificial intelligence is the future"),
    ]

    text_df = spark.createDataFrame(text_data, ["id", "text"])

    # 文本處理管道
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    stop_words_remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="tf_features")
    idf = IDF(inputCol="tf_features", outputCol="tfidf_features")

    # 建立文本處理管道
    text_pipeline = Pipeline(stages=[tokenizer, stop_words_remover, hashing_tf, idf])

    # 訓練並轉換
    text_model = text_pipeline.fit(text_df)
    text_result = text_model.transform(text_df)

    print("文本特徵提取結果:")
    text_result.select("id", "text", "tfidf_features").show(truncate=False)

    # 14. 降維
    print("\n1️⃣4️⃣ 降維")

    # PCA 降維
    pca = PCA(k=3, inputCol="scaled_features", outputCol="pca_features")

    pca_model = pca.fit(df_scaled)
    pca_result = pca_model.transform(df_scaled)

    print("PCA 降維結果:")
    pca_result.select("pca_features").show(5, truncate=False)

    # 解釋變異量
    print(f"解釋變異量: {pca_model.explainedVariance}")

    # 15. 模型評估指標
    print("\n1️⃣5️⃣ 模型評估指標")

    # 多分類評估指標
    multi_evaluator = MulticlassClassificationEvaluator(
        labelCol="target", predictionCol="prediction"
    )

    # 準確率
    accuracy = multi_evaluator.evaluate(
        lr_predictions, {multi_evaluator.metricName: "accuracy"}
    )
    print(f"準確率: {accuracy:.4f}")

    # 精確度
    precision = multi_evaluator.evaluate(
        lr_predictions, {multi_evaluator.metricName: "weightedPrecision"}
    )
    print(f"精確度: {precision:.4f}")

    # 召回率
    recall = multi_evaluator.evaluate(
        lr_predictions, {multi_evaluator.metricName: "weightedRecall"}
    )
    print(f"召回率: {recall:.4f}")

    # F1 分數
    f1 = multi_evaluator.evaluate(lr_predictions, {multi_evaluator.metricName: "f1"})
    print(f"F1 分數: {f1:.4f}")

    # 16. 保存和載入模型
    print("\n1️⃣6️⃣ 保存和載入模型")

    # 保存模型
    model_path = "/tmp/spark_lr_model"
    lr_model.write().overwrite().save(model_path)
    print(f"模型已保存到: {model_path}")

    # 載入模型
    from pyspark.ml.classification import LogisticRegressionModel

    loaded_model = LogisticRegressionModel.load(model_path)
    print("模型載入成功")

    # 使用載入的模型進行預測
    loaded_predictions = loaded_model.transform(test_data)
    loaded_auc = evaluator.evaluate(loaded_predictions)
    print(f"載入模型 AUC: {loaded_auc:.4f}")

    # 停止 SparkSession
    spark.stop()
    print("\n✅ MLlib 機器學習基礎示範完成")


if __name__ == "__main__":
    main()
