# 第6章練習：MLlib 機器學習

## 練習目標
學習使用 Spark MLlib 進行大規模機器學習，包括數據預處理、特徵工程、模型訓練和評估。

## 練習1：基本機器學習管道

### 任務描述
建立完整的機器學習管道，包括數據預處理、特徵工程、模型訓練和評估。

### 要求
1. 數據載入和探索性分析
2. 數據清洗和預處理
3. 特徵選擇和工程
4. 模型訓練和調參
5. 模型評估和驗證

### 程式碼模板

```python
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.regression import *
from pyspark.ml.evaluation import *
from pyspark.ml.tuning import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 創建 SparkSession
spark = SparkSession.builder \
    .appName("MLlib基礎練習") \
    .master("local[*]") \
    .getOrCreate()

# 房價預測數據集
housing_data = [
    (1, 3, 2, 1500, 2005, "Suburban", 350000),
    (2, 4, 3, 2200, 1995, "Urban", 450000),
    (3, 2, 1, 800, 1980, "Rural", 180000),
    (4, 5, 4, 3000, 2010, "Suburban", 650000),
    (5, 3, 2, 1800, 2000, "Urban", 420000),
    (6, 4, 3, 2500, 1990, "Suburban", 520000),
    (7, 2, 1, 900, 1975, "Rural", 200000),
    (8, 6, 5, 4000, 2015, "Urban", 800000),
    (9, 3, 2, 1600, 1985, "Suburban", 380000),
    (10, 4, 3, 2100, 2008, "Urban", 480000)
]

housing_columns = ["id", "bedrooms", "bathrooms", "sqft", "year_built", "location", "price"]

# 客戶分類數據集
customer_data = [
    (1, 25, 50000, 3, 120, "Premium"),
    (2, 35, 75000, 8, 200, "Premium"), 
    (3, 45, 60000, 12, 150, "Standard"),
    (4, 28, 45000, 2, 80, "Basic"),
    (5, 55, 90000, 15, 300, "Premium"),
    (6, 32, 55000, 5, 100, "Standard"),
    (7, 38, 65000, 7, 180, "Standard"),
    (8, 42, 70000, 10, 220, "Premium"),
    (9, 29, 40000, 1, 60, "Basic"),
    (10, 48, 80000, 18, 250, "Premium")
]

customer_columns = ["customer_id", "age", "income", "years_experience", "purchase_amount", "segment"]

# 完成以下任務：
# 1. 創建回歸模型預測房價
# 2. 創建分類模型預測客戶分群
# 3. 實現特徵工程流程
# 4. 進行模型比較和評估
# 5. 實現交叉驗證和超參數調優

# 你的程式碼在這裡

def create_regression_pipeline():
    # 房價預測流程
    housing_df = spark.createDataFrame(housing_data, housing_columns)
    
    # 特徵工程
    location_indexer = StringIndexer(inputCol="location", outputCol="location_indexed")
    assembler = VectorAssembler(
        inputCols=["bedrooms", "bathrooms", "sqft", "year_built", "location_indexed"],
        outputCol="features"
    )
    
    # 回歸模型
    lr = LinearRegression(featuresCol="features", labelCol="price")
    
    # 創建管道
    pipeline = Pipeline(stages=[location_indexer, assembler, lr])
    
    return pipeline, housing_df

def create_classification_pipeline():
    # 客戶分群流程
    customer_df = spark.createDataFrame(customer_data, customer_columns)
    
    # 特徵工程
    assembler = VectorAssembler(
        inputCols=["age", "income", "years_experience", "purchase_amount"],
        outputCol="features"
    )
    
    # 標籤編碼
    label_indexer = StringIndexer(inputCol="segment", outputCol="label")
    
    # 分類模型
    rf = RandomForestClassifier(featuresCol="features", labelCol="label")
    
    # 創建管道
    pipeline = Pipeline(stages=[assembler, label_indexer, rf])
    
    return pipeline, customer_df

spark.stop()
```

### 預期輸出
- 回歸模型的預測結果和評估指標
- 分類模型的準確率和混淆矩陣
- 特徵重要性分析

## 練習2：推薦系統

### 任務描述
使用協同過濾算法建立推薦系統。

### 要求
1. 實現基於用戶的協同過濾
2. 實現基於物品的協同過濾
3. 使用 ALS 算法進行矩陣分解
4. 評估推薦系統性能
5. 處理冷啟動問題

### 程式碼模板

```python
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

# 用戶評分數據
ratings_data = [
    (1, 101, 4.5),  # user_id, movie_id, rating
    (1, 102, 3.5),
    (1, 103, 5.0),
    (2, 101, 2.5),
    (2, 104, 4.0),
    (2, 105, 3.0),
    (3, 102, 4.5),
    (3, 103, 2.0),
    (3, 106, 4.5),
    (4, 101, 3.5),
    (4, 104, 2.5),
    (4, 107, 4.0),
    (5, 105, 5.0),
    (5, 106, 3.5),
    (5, 107, 4.5),
    (6, 101, 1.5),
    (6, 102, 2.0),
    (6, 108, 4.0)
]

ratings_columns = ["user_id", "movie_id", "rating"]

# 電影資訊
movies_data = [
    (101, "Action Movie A", "Action"),
    (102, "Comedy Movie B", "Comedy"),
    (103, "Drama Movie C", "Drama"),
    (104, "Action Movie D", "Action"),
    (105, "Comedy Movie E", "Comedy"),
    (106, "Drama Movie F", "Drama"),
    (107, "Action Movie G", "Action"),
    (108, "Comedy Movie H", "Comedy")
]

movies_columns = ["movie_id", "title", "genre"]

# 完成以下任務：
# 1. 建立 ALS 推薦模型
# 2. 訓練和評估模型
# 3. 為用戶生成推薦
# 4. 分析推薦質量
# 5. 處理新用戶推薦

# 你的程式碼在這裡

def build_recommendation_system():
    ratings_df = spark.createDataFrame(ratings_data, ratings_columns)
    movies_df = spark.createDataFrame(movies_data, movies_columns)
    
    # 數據分割
    (training, test) = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    # ALS 模型
    als = ALS(
        maxIter=10,
        regParam=0.1,
        userCol="user_id",
        itemCol="movie_id", 
        ratingCol="rating",
        coldStartStrategy="drop"
    )
    
    # 訓練模型
    model = als.fit(training)
    
    # 預測
    predictions = model.transform(test)
    
    # 評估
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    
    return model, rmse, ratings_df, movies_df
```

## 練習3：聚類分析

### 任務描述
使用聚類算法進行客戶分群和市場細分。

### 要求
1. 實現 K-Means 聚類
2. 使用 Bisecting K-Means
3. 實現高斯混合模型
4. 確定最佳聚類數量
5. 解釋和可視化聚類結果

### 程式碼模板

```python
from pyspark.ml.clustering import KMeans, BisectingKMeans, GaussianMixture
from pyspark.ml.evaluation import ClusteringEvaluator

# 客戶行為數據
customer_behavior = [
    (1, 25, 50000, 12, 8, 250),     # age, income, months_active, purchases, complaints, spend
    (2, 35, 75000, 24, 15, 480),
    (3, 45, 60000, 18, 5, 320),
    (4, 28, 45000, 6, 12, 180),
    (5, 55, 90000, 36, 25, 650),
    (6, 32, 55000, 15, 3, 290),
    (7, 38, 65000, 21, 8, 410),
    (8, 42, 70000, 30, 18, 520),
    (9, 29, 40000, 9, 15, 160),
    (10, 48, 80000, 27, 12, 580),
    (11, 26, 52000, 14, 6, 270),
    (12, 39, 68000, 22, 9, 430),
    (13, 44, 72000, 25, 11, 490),
    (14, 31, 58000, 16, 4, 310),
    (15, 52, 85000, 33, 20, 610)
]

behavior_columns = ["customer_id", "age", "income", "months_active", "purchases", "complaints", "total_spend"]

# 完成以下任務：
# 1. 數據標準化
# 2. K-Means 聚類
# 3. 確定最佳 K 值
# 4. 聚類結果分析
# 5. 業務解釋

# 你的程式碼在這裡

def perform_clustering_analysis():
    behavior_df = spark.createDataFrame(customer_behavior, behavior_columns)
    
    # 特徵工程
    feature_cols = ["age", "income", "months_active", "purchases", "complaints", "total_spend"]
    
    # 向量組裝
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # 標準化
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # K-Means
    kmeans = KMeans(featuresCol="scaledFeatures", k=3, seed=42)
    
    # 創建管道
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    
    return pipeline, behavior_df

def find_optimal_k():
    # 尋找最佳聚類數量
    behavior_df = spark.createDataFrame(customer_behavior, behavior_columns)
    
    feature_cols = ["age", "income", "months_active", "purchases", "complaints", "total_spend"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # 準備數據
    prepared_data = assembler.transform(behavior_df)
    scaled_data = scaler.fit(prepared_data).transform(prepared_data)
    
    # 嘗試不同的 K 值
    k_values = range(2, 8)
    silhouette_scores = []
    
    for k in k_values:
        kmeans = KMeans(featuresCol="scaledFeatures", k=k, seed=42)
        model = kmeans.fit(scaled_data)
        predictions = model.transform(scaled_data)
        
        evaluator = ClusteringEvaluator(featuresCol="scaledFeatures")
        score = evaluator.evaluate(predictions)
        silhouette_scores.append((k, score))
    
    return silhouette_scores
```

## 練習4：異常檢測

### 任務描述
實現異常檢測算法，識別數據中的異常點。

### 要求
1. 實現基於統計的異常檢測
2. 使用 Isolation Forest
3. 實現 One-Class SVM
4. 評估異常檢測性能
5. 處理不平衡數據

### 程式碼模板

```python
from pyspark.ml.feature import Normalizer
from pyspark.sql.functions import sqrt, pow, mean, stddev

# 網路流量數據（包含異常）
network_traffic = [
    (1, 1024, 50, 0.1, 0),      # packet_size, frequency, duration, is_anomaly
    (2, 1500, 75, 0.15, 0),
    (3, 800, 45, 0.08, 0),
    (4, 2000, 100, 0.2, 0),
    (5, 1200, 60, 0.12, 0),
    (6, 10000, 500, 2.0, 1),    # 異常：大包
    (7, 1100, 55, 0.11, 0),
    (8, 900, 40, 0.09, 0),
    (9, 50, 1000, 5.0, 1),      # 異常：小包高頻
    (10, 1300, 65, 0.13, 0),
    (11, 1400, 70, 0.14, 0),
    (12, 20000, 800, 8.0, 1),   # 異常：超大包
    (13, 1000, 50, 0.1, 0),
    (14, 1600, 80, 0.16, 0),
    (15, 100, 2000, 10.0, 1)    # 異常：超高頻
]

traffic_columns = ["id", "packet_size", "frequency", "duration", "is_anomaly"]

# 完成以下任務：
# 1. 統計異常檢測
# 2. 基於聚類的異常檢測  
# 3. 評估檢測性能
# 4. 實時異常檢測
# 5. 異常原因分析

# 你的程式碼在這裡

def statistical_anomaly_detection():
    traffic_df = spark.createDataFrame(network_traffic, traffic_columns)
    
    # 計算統計量
    stats = traffic_df.select(
        mean("packet_size").alias("mean_size"),
        stddev("packet_size").alias("std_size"),
        mean("frequency").alias("mean_freq"),
        stddev("frequency").alias("std_freq"),
        mean("duration").alias("mean_duration"),
        stddev("duration").alias("std_duration")
    ).collect()[0]
    
    # Z-score 異常檢測
    z_threshold = 2.0
    
    anomaly_detection = traffic_df.withColumn(
        "size_zscore", 
        abs(col("packet_size") - stats["mean_size"]) / stats["std_size"]
    ).withColumn(
        "freq_zscore",
        abs(col("frequency") - stats["mean_freq"]) / stats["std_freq"] 
    ).withColumn(
        "duration_zscore",
        abs(col("duration") - stats["mean_duration"]) / stats["std_duration"]
    ).withColumn(
        "is_statistical_anomaly",
        (col("size_zscore") > z_threshold) | 
        (col("freq_zscore") > z_threshold) |
        (col("duration_zscore") > z_threshold)
    )
    
    return anomaly_detection

def clustering_based_anomaly_detection():
    traffic_df = spark.createDataFrame(network_traffic, traffic_columns)
    
    # 特徵工程
    assembler = VectorAssembler(
        inputCols=["packet_size", "frequency", "duration"],
        outputCol="features"
    )
    
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # K-means 聚類
    kmeans = KMeans(featuresCol="scaledFeatures", k=2, seed=42)
    
    # 管道
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(traffic_df)
    
    # 預測聚類
    clustered = model.transform(traffic_df)
    
    # 計算到聚類中心的距離
    # 這裡簡化處理，實際應該計算到最近聚類中心的距離
    
    return clustered
```

## 練習答案

### 練習1解答

```python
# 回歸模型實現
def train_regression_model():
    housing_df = spark.createDataFrame(housing_data, housing_columns)
    
    print("房價數據探索:")
    housing_df.describe().show()
    
    # 特徵工程
    location_indexer = StringIndexer(inputCol="location", outputCol="location_indexed")
    assembler = VectorAssembler(
        inputCols=["bedrooms", "bathrooms", "sqft", "year_built", "location_indexed"],
        outputCol="features"
    )
    
    # 特徵標準化
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # 回歸模型
    lr = LinearRegression(featuresCol="scaledFeatures", labelCol="price")
    
    # 創建管道
    pipeline = Pipeline(stages=[location_indexer, assembler, scaler, lr])
    
    # 數據分割
    (training, test) = housing_df.randomSplit([0.8, 0.2], seed=42)
    
    # 訓練模型
    model = pipeline.fit(training)
    
    # 預測
    predictions = model.transform(test)
    
    print("回歸預測結果:")
    predictions.select("price", "prediction").show()
    
    # 評估
    evaluator = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="rmse")
    rmse = evaluator.evaluate(predictions)
    
    evaluator_r2 = RegressionEvaluator(labelCol="price", predictionCol="prediction", metricName="r2")
    r2 = evaluator_r2.evaluate(predictions)
    
    print(f"RMSE: {rmse:.2f}")
    print(f"R²: {r2:.3f}")
    
    # 特徵重要性（線性回歸的係數）
    lr_model = model.stages[-1]
    print("模型係數:", lr_model.coefficients)
    print("截距:", lr_model.intercept)
    
    return model

# 分類模型實現
def train_classification_model():
    customer_df = spark.createDataFrame(customer_data, customer_columns)
    
    print("客戶數據探索:")
    customer_df.groupBy("segment").count().show()
    
    # 特徵工程
    assembler = VectorAssembler(
        inputCols=["age", "income", "years_experience", "purchase_amount"],
        outputCol="features"
    )
    
    # 標籤編碼
    label_indexer = StringIndexer(inputCol="segment", outputCol="label")
    label_converter = IndexToString(inputCol="prediction", outputCol="predicted_segment", 
                                  labels=label_indexer.fit(customer_df).labels)
    
    # 隨機森林分類器
    rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=10)
    
    # 創建管道
    pipeline = Pipeline(stages=[assembler, label_indexer, rf, label_converter])
    
    # 數據分割
    (training, test) = customer_df.randomSplit([0.7, 0.3], seed=42)
    
    # 訓練模型
    model = pipeline.fit(training)
    
    # 預測
    predictions = model.transform(test)
    
    print("分類預測結果:")
    predictions.select("segment", "predicted_segment", "probability").show()
    
    # 評估
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    
    print(f"準確率: {accuracy:.3f}")
    
    # 特徵重要性
    rf_model = model.stages[2]
    print("特徵重要性:", rf_model.featureImportances)
    
    return model

# 交叉驗證和超參數調優
def hyperparameter_tuning():
    customer_df = spark.createDataFrame(customer_data, customer_columns)
    
    # 準備管道
    assembler = VectorAssembler(
        inputCols=["age", "income", "years_experience", "purchase_amount"],
        outputCol="features"
    )
    label_indexer = StringIndexer(inputCol="segment", outputCol="label")
    rf = RandomForestClassifier(featuresCol="features", labelCol="label")
    
    pipeline = Pipeline(stages=[assembler, label_indexer, rf])
    
    # 超參數網格
    paramGrid = ParamGridBuilder() \
        .addGrid(rf.numTrees, [5, 10, 20]) \
        .addGrid(rf.maxDepth, [3, 5, 7]) \
        .build()
    
    # 交叉驗證
    crossval = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=paramGrid,
        evaluator=MulticlassClassificationEvaluator(labelCol="label"),
        numFolds=3
    )
    
    # 訓練最佳模型
    cv_model = crossval.fit(customer_df)
    
    print("最佳模型參數:")
    best_model = cv_model.bestModel
    rf_stage = best_model.stages[2]
    print(f"numTrees: {rf_stage.getNumTrees}")
    print(f"maxDepth: {rf_stage.getMaxDepth}")
    
    return cv_model

# 執行所有訓練
regression_model = train_regression_model()
classification_model = train_classification_model()
best_model = hyperparameter_tuning()
```

### 練習2解答

```python
def build_als_recommendation_system():
    ratings_df = spark.createDataFrame(ratings_data, ratings_columns)
    movies_df = spark.createDataFrame(movies_data, movies_columns)
    
    print("評分數據統計:")
    ratings_df.describe().show()
    
    print("用戶評分分佈:")
    ratings_df.groupBy("rating").count().orderBy("rating").show()
    
    # 數據分割
    (training, test) = ratings_df.randomSplit([0.8, 0.2], seed=42)
    
    # ALS 模型
    als = ALS(
        maxIter=10,
        regParam=0.1,
        rank=10,
        userCol="user_id",
        itemCol="movie_id", 
        ratingCol="rating",
        coldStartStrategy="drop"
    )
    
    # 訓練模型
    model = als.fit(training)
    
    # 預測測試集
    predictions = model.transform(test)
    
    print("預測結果:")
    predictions.select("user_id", "movie_id", "rating", "prediction").show()
    
    # 評估
    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"RMSE: {rmse:.3f}")
    
    # 為所有用戶生成推薦
    user_recs = model.recommendForAllUsers(3)
    print("用戶推薦:")
    user_recs.show(truncate=False)
    
    # 為所有電影推薦用戶
    movie_recs = model.recommendForAllItems(3)
    print("電影推薦用戶:")
    movie_recs.show(truncate=False)
    
    # 為特定用戶生成推薦
    specific_users = spark.createDataFrame([(1,), (2,)], ["user_id"])
    user_subset_recs = model.recommendForUserSubset(specific_users, 5)
    
    print("特定用戶推薦:")
    user_subset_recs.show(truncate=False)
    
    # 加入電影信息
    def get_movie_recommendations_with_details(user_id):
        user_df = spark.createDataFrame([(user_id,)], ["user_id"])
        recs = model.recommendForUserSubset(user_df, 5)
        
        # 展開推薦列表
        recs_expanded = recs.select("user_id", explode("recommendations").alias("rec"))
        recs_detailed = recs_expanded.select(
            "user_id",
            col("rec.movie_id").alias("movie_id"),
            col("rec.rating").alias("predicted_rating")
        )
        
        # 加入電影詳情
        detailed_recs = recs_detailed.join(movies_df, "movie_id")
        return detailed_recs
    
    print("用戶1的詳細推薦:")
    detailed_recs = get_movie_recommendations_with_details(1)
    detailed_recs.show()
    
    return model, rmse

# 推薦系統評估
def evaluate_recommendation_quality():
    ratings_df = spark.createDataFrame(ratings_data, ratings_columns)
    
    # 計算推薦多樣性
    movies_df = spark.createDataFrame(movies_data, movies_columns)
    
    # 按類型統計
    genre_stats = movies_df.groupBy("genre").count()
    print("電影類型分佈:")
    genre_stats.show()
    
    # 計算用戶評分習慣
    user_stats = ratings_df.groupBy("user_id").agg(
        avg("rating").alias("avg_rating"),
        count("rating").alias("rating_count"),
        stddev("rating").alias("rating_std")
    )
    
    print("用戶評分統計:")
    user_stats.show()
    
    return user_stats

# 執行推薦系統
als_model, rmse = build_als_recommendation_system()
user_analysis = evaluate_recommendation_quality()
```

### 練習3解答

```python
def comprehensive_clustering_analysis():
    behavior_df = spark.createDataFrame(customer_behavior, behavior_columns)
    
    print("客戶行為數據探索:")
    behavior_df.describe().show()
    
    # 特徵工程
    feature_cols = ["age", "income", "months_active", "purchases", "complaints", "total_spend"]
    
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # 準備數據
    assembled_data = assembler.transform(behavior_df)
    scaled_data = scaler.fit(assembled_data).transform(assembled_data)
    
    # K-Means 聚類
    kmeans = KMeans(featuresCol="scaledFeatures", k=3, seed=42)
    kmeans_model = kmeans.fit(scaled_data)
    kmeans_predictions = kmeans_model.transform(scaled_data)
    
    print("K-Means 聚類結果:")
    kmeans_predictions.select("customer_id", "prediction").show()
    
    # 聚類中心
    centers = kmeans_model.clusterCenters()
    print("聚類中心:")
    for i, center in enumerate(centers):
        print(f"聚類 {i}: {center}")
    
    # 聚類評估
    evaluator = ClusteringEvaluator(featuresCol="scaledFeatures")
    silhouette = evaluator.evaluate(kmeans_predictions)
    print(f"Silhouette 係數: {silhouette:.3f}")
    
    # 聚類分析
    cluster_analysis = kmeans_predictions.groupBy("prediction").agg(
        avg("age").alias("avg_age"),
        avg("income").alias("avg_income"),
        avg("months_active").alias("avg_months_active"),
        avg("purchases").alias("avg_purchases"),
        avg("complaints").alias("avg_complaints"),
        avg("total_spend").alias("avg_spend"),
        count("*").alias("cluster_size")
    )
    
    print("聚類特徵分析:")
    cluster_analysis.show()
    
    # Bisecting K-Means
    bisecting_kmeans = BisectingKMeans(featuresCol="scaledFeatures", k=3, seed=42)
    bisecting_model = bisecting_kmeans.fit(scaled_data)
    bisecting_predictions = bisecting_model.transform(scaled_data)
    
    bisecting_silhouette = evaluator.evaluate(bisecting_predictions)
    print(f"Bisecting K-Means Silhouette: {bisecting_silhouette:.3f}")
    
    # 高斯混合模型
    gmm = GaussianMixture(featuresCol="scaledFeatures", k=3, seed=42)
    gmm_model = gmm.fit(scaled_data)
    gmm_predictions = gmm_model.transform(scaled_data)
    
    print("高斯混合模型結果:")
    gmm_predictions.select("customer_id", "prediction", "probability").show()
    
    return kmeans_model, bisecting_model, gmm_model

def find_optimal_clusters():
    behavior_df = spark.createDataFrame(customer_behavior, behavior_columns)
    
    feature_cols = ["age", "income", "months_active", "purchases", "complaints", "total_spend"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # 準備數據
    assembled_data = assembler.transform(behavior_df)
    scaled_data = scaler.fit(assembled_data).transform(assembled_data)
    
    # 測試不同的 K 值
    k_values = range(2, 8)
    results = []
    
    for k in k_values:
        kmeans = KMeans(featuresCol="scaledFeatures", k=k, seed=42)
        model = kmeans.fit(scaled_data)
        predictions = model.transform(scaled_data)
        
        evaluator = ClusteringEvaluator(featuresCol="scaledFeatures")
        silhouette = evaluator.evaluate(predictions)
        
        # 計算 WSSSE (Within Set Sum of Squared Errors)
        wssse = model.summary.trainingCost
        
        results.append((k, silhouette, wssse))
        print(f"K={k}: Silhouette={silhouette:.3f}, WSSSE={wssse:.2f}")
    
    return results

# 執行聚類分析
kmeans_model, bisecting_model, gmm_model = comprehensive_clustering_analysis()
optimal_k_results = find_optimal_clusters()
```

### 練習4解答

```python
def comprehensive_anomaly_detection():
    traffic_df = spark.createDataFrame(network_traffic, traffic_columns)
    
    print("網路流量數據分析:")
    traffic_df.describe().show()
    
    print("異常標籤分佈:")
    traffic_df.groupBy("is_anomaly").count().show()
    
    # 1. 統計方法異常檢測
    stats = traffic_df.select(
        mean("packet_size").alias("mean_size"),
        stddev("packet_size").alias("std_size"),
        mean("frequency").alias("mean_freq"),
        stddev("frequency").alias("std_freq"),
        mean("duration").alias("mean_duration"),
        stddev("duration").alias("std_duration")
    ).collect()[0]
    
    # Z-score 異常檢測
    z_threshold = 2.0
    
    statistical_anomalies = traffic_df.withColumn(
        "size_zscore", 
        abs(col("packet_size") - stats["mean_size"]) / stats["std_size"]
    ).withColumn(
        "freq_zscore",
        abs(col("frequency") - stats["mean_freq"]) / stats["std_freq"] 
    ).withColumn(
        "duration_zscore",
        abs(col("duration") - stats["mean_duration"]) / stats["std_duration"]
    ).withColumn(
        "max_zscore",
        greatest(col("size_zscore"), col("freq_zscore"), col("duration_zscore"))
    ).withColumn(
        "is_statistical_anomaly",
        col("max_zscore") > z_threshold
    )
    
    print("統計異常檢測結果:")
    statistical_anomalies.select("id", "is_anomaly", "is_statistical_anomaly", "max_zscore").show()
    
    # 2. 基於聚類的異常檢測
    assembler = VectorAssembler(
        inputCols=["packet_size", "frequency", "duration"],
        outputCol="features"
    )
    
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
    
    # 使用較多的聚類數，正常數據應該聚集，異常數據會形成小聚類或單獨聚類
    kmeans = KMeans(featuresCol="scaledFeatures", k=5, seed=42)
    
    # 管道
    pipeline = Pipeline(stages=[assembler, scaler, kmeans])
    model = pipeline.fit(traffic_df)
    
    clustered = model.transform(traffic_df)
    
    # 分析聚類大小
    cluster_sizes = clustered.groupBy("prediction").count().orderBy("count")
    print("聚類大小分析:")
    cluster_sizes.show()
    
    # 將小聚類標記為異常
    small_clusters = cluster_sizes.filter(col("count") <= 2).select("prediction").rdd.flatMap(lambda x: x).collect()
    
    clustering_anomalies = clustered.withColumn(
        "is_clustering_anomaly",
        col("prediction").isin(small_clusters)
    )
    
    print("聚類異常檢測結果:")
    clustering_anomalies.select("id", "is_anomaly", "is_clustering_anomaly", "prediction").show()
    
    # 3. 組合異常檢測
    combined_results = statistical_anomalies.join(
        clustering_anomalies.select("id", "is_clustering_anomaly", "prediction"), 
        "id"
    ).withColumn(
        "is_combined_anomaly",
        col("is_statistical_anomaly") | col("is_clustering_anomaly")
    )
    
    print("組合異常檢測結果:")
    combined_results.select("id", "is_anomaly", "is_statistical_anomaly", 
                          "is_clustering_anomaly", "is_combined_anomaly").show()
    
    # 4. 評估檢測性能
    def evaluate_detection(df, prediction_col, actual_col="is_anomaly"):
        evaluator = BinaryClassificationEvaluator(labelCol=actual_col, rawPredictionCol=prediction_col)
        
        # 計算混淆矩陣
        tp = df.filter((col(actual_col) == 1) & (col(prediction_col) == 1)).count()
        tn = df.filter((col(actual_col) == 0) & (col(prediction_col) == 0)).count()
        fp = df.filter((col(actual_col) == 0) & (col(prediction_col) == 1)).count()
        fn = df.filter((col(actual_col) == 1) & (col(prediction_col) == 0)).count()
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
        
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "tp": tp, "tn": tn, "fp": fp, "fn": fn
        }
    
    # 轉換布林值為數值
    eval_df = combined_results.withColumn("stat_pred", col("is_statistical_anomaly").cast("int")) \
                             .withColumn("cluster_pred", col("is_clustering_anomaly").cast("int")) \
                             .withColumn("combined_pred", col("is_combined_anomaly").cast("int"))
    
    print("統計方法性能:")
    stat_performance = evaluate_detection(eval_df, "stat_pred")
    print(f"Precision: {stat_performance['precision']:.3f}")
    print(f"Recall: {stat_performance['recall']:.3f}")
    print(f"F1: {stat_performance['f1']:.3f}")
    
    print("聚類方法性能:")
    cluster_performance = evaluate_detection(eval_df, "cluster_pred")
    print(f"Precision: {cluster_performance['precision']:.3f}")
    print(f"Recall: {cluster_performance['recall']:.3f}")
    print(f"F1: {cluster_performance['f1']:.3f}")
    
    print("組合方法性能:")
    combined_performance = evaluate_detection(eval_df, "combined_pred")
    print(f"Precision: {combined_performance['precision']:.3f}")
    print(f"Recall: {combined_performance['recall']:.3f}")
    print(f"F1: {combined_performance['f1']:.3f}")
    
    return model, combined_results

# 執行異常檢測
anomaly_model, anomaly_results = comprehensive_anomaly_detection()
```

## 練習提示

1. **特徵工程**：
   - 正確的數據預處理是關鍵
   - 使用適當的特徵縮放和標準化
   - 處理類別變數和缺失值

2. **模型選擇**：
   - 根據問題類型選擇合適的算法
   - 考慮數據大小和計算資源
   - 使用交叉驗證避免過擬合

3. **評估指標**：
   - 選擇符合業務目標的評估指標
   - 注意類別不平衡問題
   - 使用多種指標全面評估

4. **超參數調優**：
   - 使用網格搜索或隨機搜索
   - 考慮計算成本和時間
   - 避免過度調優導致的過擬合

## 進階挑戰

1. **大規模機器學習**：
   - 處理 TB 級別的數據集
   - 實現分散式特徵工程
   - 優化模型訓練和推理性能

2. **深度學習整合**：
   - 使用 Spark 結合 TensorFlow/PyTorch
   - 實現分散式深度學習訓練
   - 處理非結構化數據

3. **實時機器學習**：
   - 結合 Streaming 實現在線學習
   - 實現模型增量更新
   - 建立 A/B 測試框架

## 學習檢核

完成練習後，你應該能夠：
- [ ] 建立完整的機器學習管道
- [ ] 實現各種監督和無監督學習算法
- [ ] 進行特徵工程和模型調優
- [ ] 評估和比較不同模型性能
- [ ] 處理大規模機器學習問題
- [ ] 應用機器學習解決實際業務問題