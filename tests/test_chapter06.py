"""
第6章：Spark MLlib 機器學習的測試
"""

import pytest
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# MLlib imports
from pyspark.ml.feature import (
    VectorAssembler, StandardScaler, MinMaxScaler, StringIndexer, 
    OneHotEncoder, Tokenizer, HashingTF, IDF, PCA, Bucketizer
)
from pyspark.ml.classification import (
    LogisticRegression, DecisionTreeClassifier, RandomForestClassifier,
    NaiveBayes, LinearSVC
)
from pyspark.ml.regression import (
    LinearRegression, DecisionTreeRegressor, RandomForestRegressor,
    GBTRegressor
)
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator,
    RegressionEvaluator
)
from pyspark.ml import Pipeline
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.mllib.stat import Statistics
from pyspark.mllib.linalg import Vectors as MLLibVectors
from pyspark.ml.linalg import Vectors as MLVectors


class TestFeatureEngineering:
    """測試特徵工程"""
    
    def test_vector_assembler(self, spark_session):
        """測試向量組裝器"""
        # 創建測試數據
        data = [
            (1, 2.0, 3.0, 4.0),
            (2, 5.0, 6.0, 7.0),
            (3, 8.0, 9.0, 10.0)
        ]
        df = spark_session.createDataFrame(data, ["id", "feature1", "feature2", "feature3"])
        
        # 使用 VectorAssembler
        assembler = VectorAssembler(
            inputCols=["feature1", "feature2", "feature3"],
            outputCol="features"
        )
        
        result_df = assembler.transform(df)
        
        # 檢查結果
        assert "features" in result_df.columns
        features = result_df.select("features").collect()
        assert len(features) == 3
        assert len(features[0].features) == 3
    
    def test_standard_scaler(self, spark_session):
        """測試標準化缩放器"""
        # 創建測試數據
        data = [
            (1, MLVectors.dense([1.0, 2.0, 3.0])),
            (2, MLVectors.dense([4.0, 5.0, 6.0])),
            (3, MLVectors.dense([7.0, 8.0, 9.0]))
        ]
        df = spark_session.createDataFrame(data, ["id", "features"])
        
        # 標準化
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(df)
        scaled_df = scaler_model.transform(df)
        
        # 檢查結果
        assert "scaled_features" in scaled_df.columns
        scaled_features = scaled_df.select("scaled_features").collect()
        assert len(scaled_features) == 3
    
    def test_string_indexer(self, spark_session):
        """測試字符串索引器"""
        # 創建測試數據
        data = [
            (1, "category_a"),
            (2, "category_b"),
            (3, "category_a"),
            (4, "category_c"),
            (5, "category_b")
        ]
        df = spark_session.createDataFrame(data, ["id", "category"])
        
        # 字符串索引
        indexer = StringIndexer(
            inputCol="category",
            outputCol="category_index"
        )
        
        indexer_model = indexer.fit(df)
        indexed_df = indexer_model.transform(df)
        
        # 檢查結果
        assert "category_index" in indexed_df.columns
        indices = [row.category_index for row in indexed_df.collect()]
        assert set(indices) == {0.0, 1.0, 2.0}  # 三個不同的類別
    
    def test_one_hot_encoder(self, spark_session):
        """測試獨熱編碼"""
        # 創建測試數據
        data = [
            (1, 0.0),
            (2, 1.0),
            (3, 0.0),
            (4, 2.0),
            (5, 1.0)
        ]
        df = spark_session.createDataFrame(data, ["id", "category_index"])
        
        # 獨熱編碼
        encoder = OneHotEncoder(
            inputCol="category_index",
            outputCol="category_vector"
        )
        
        encoded_df = encoder.transform(df)
        
        # 檢查結果
        assert "category_vector" in encoded_df.columns
        vectors = [row.category_vector for row in encoded_df.collect()]
        assert all(len(v) == 3 for v in vectors)  # 應該有3維向量
    
    def test_text_processing(self, spark_session):
        """測試文本處理"""
        # 創建測試數據
        data = [
            (1, "hello world spark"),
            (2, "machine learning is great"),
            (3, "spark mllib is powerful")
        ]
        df = spark_session.createDataFrame(data, ["id", "text"])
        
        # 分詞
        tokenizer = Tokenizer(inputCol="text", outputCol="words")
        words_df = tokenizer.transform(df)
        
        # 哈希TF
        hashing_tf = HashingTF(inputCol="words", outputCol="raw_features", numFeatures=100)
        tf_df = hashing_tf.transform(words_df)
        
        # IDF
        idf = IDF(inputCol="raw_features", outputCol="features")
        idf_model = idf.fit(tf_df)
        tfidf_df = idf_model.transform(tf_df)
        
        # 檢查結果
        assert "words" in words_df.columns
        assert "raw_features" in tf_df.columns
        assert "features" in tfidf_df.columns
        
        # 檢查分詞結果
        words = words_df.select("words").collect()
        assert len(words[0].words) == 3  # "hello world spark"
    
    def test_pca(self, spark_session):
        """測試主成分分析"""
        # 創建測試數據
        data = [
            (1, MLVectors.dense([1.0, 2.0, 3.0, 4.0])),
            (2, MLVectors.dense([2.0, 3.0, 4.0, 5.0])),
            (3, MLVectors.dense([3.0, 4.0, 5.0, 6.0])),
            (4, MLVectors.dense([4.0, 5.0, 6.0, 7.0]))
        ]
        df = spark_session.createDataFrame(data, ["id", "features"])
        
        # PCA
        pca = PCA(k=2, inputCol="features", outputCol="pca_features")
        pca_model = pca.fit(df)
        pca_df = pca_model.transform(df)
        
        # 檢查結果
        assert "pca_features" in pca_df.columns
        pca_features = pca_df.select("pca_features").collect()
        assert all(len(f.pca_features) == 2 for f in pca_features)


class TestClassification:
    """測試分類算法"""
    
    @pytest.fixture(scope="function")
    def classification_data(self, spark_session):
        """創建分類測試數據"""
        data = [
            (1.0, MLVectors.dense([1.0, 2.0])),
            (0.0, MLVectors.dense([2.0, 1.0])),
            (1.0, MLVectors.dense([3.0, 4.0])),
            (0.0, MLVectors.dense([4.0, 3.0])),
            (1.0, MLVectors.dense([5.0, 6.0])),
            (0.0, MLVectors.dense([6.0, 5.0]))
        ]
        return spark_session.createDataFrame(data, ["label", "features"])
    
    def test_logistic_regression(self, classification_data):
        """測試邏輯回歸"""
        # 分割數據
        train_data = classification_data
        
        # 邏輯回歸
        lr = LogisticRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(train_data)
        
        # 預測
        predictions = lr_model.transform(train_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        assert "probability" in predictions.columns
        
        # 評估
        evaluator = BinaryClassificationEvaluator()
        auc = evaluator.evaluate(predictions)
        assert 0 <= auc <= 1
    
    def test_decision_tree_classifier(self, classification_data):
        """測試決策樹分類器"""
        dt = DecisionTreeClassifier(featuresCol="features", labelCol="label")
        dt_model = dt.fit(classification_data)
        
        predictions = dt_model.transform(classification_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        
        # 檢查模型屬性
        assert dt_model.numNodes > 0
        assert dt_model.depth >= 0
    
    def test_random_forest_classifier(self, classification_data):
        """測試隨機森林分類器"""
        rf = RandomForestClassifier(
            featuresCol="features", 
            labelCol="label",
            numTrees=5,
            maxDepth=3
        )
        rf_model = rf.fit(classification_data)
        
        predictions = rf_model.transform(classification_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        assert len(rf_model.trees) == 5
    
    def test_naive_bayes(self, classification_data):
        """測試樸素貝葉斯"""
        nb = NaiveBayes(featuresCol="features", labelCol="label")
        nb_model = nb.fit(classification_data)
        
        predictions = nb_model.transform(classification_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        
        # 評估
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator.evaluate(predictions)
        assert 0 <= accuracy <= 1


class TestRegression:
    """測試回歸算法"""
    
    @pytest.fixture(scope="function")
    def regression_data(self, spark_session):
        """創建回歸測試數據"""
        data = [
            (1.0, MLVectors.dense([1.0, 2.0])),
            (2.0, MLVectors.dense([2.0, 3.0])),
            (3.0, MLVectors.dense([3.0, 4.0])),
            (4.0, MLVectors.dense([4.0, 5.0])),
            (5.0, MLVectors.dense([5.0, 6.0])),
            (6.0, MLVectors.dense([6.0, 7.0]))
        ]
        return spark_session.createDataFrame(data, ["label", "features"])
    
    def test_linear_regression(self, regression_data):
        """測試線性回歸"""
        lr = LinearRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(regression_data)
        
        predictions = lr_model.transform(regression_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        
        # 檢查模型係數
        assert lr_model.coefficients is not None
        assert lr_model.intercept is not None
        
        # 評估
        evaluator = RegressionEvaluator(
            labelCol="label",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        assert rmse >= 0
    
    def test_decision_tree_regressor(self, regression_data):
        """測試決策樹回歸器"""
        dt = DecisionTreeRegressor(featuresCol="features", labelCol="label")
        dt_model = dt.fit(regression_data)
        
        predictions = dt_model.transform(regression_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        assert dt_model.numNodes > 0
    
    def test_random_forest_regressor(self, regression_data):
        """測試隨機森林回歸器"""
        rf = RandomForestRegressor(
            featuresCol="features",
            labelCol="label",
            numTrees=3,
            maxDepth=3
        )
        rf_model = rf.fit(regression_data)
        
        predictions = rf_model.transform(regression_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        assert len(rf_model.trees) == 3
    
    def test_gradient_boosted_trees(self, regression_data):
        """測試梯度提升樹"""
        gbt = GBTRegressor(
            featuresCol="features",
            labelCol="label",
            maxIter=5
        )
        gbt_model = gbt.fit(regression_data)
        
        predictions = gbt_model.transform(regression_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        assert gbt_model.numTrees <= 5


class TestClustering:
    """測試聚類算法"""
    
    @pytest.fixture(scope="function")
    def clustering_data(self, spark_session):
        """創建聚類測試數據"""
        data = [
            (1, MLVectors.dense([1.0, 1.0])),
            (2, MLVectors.dense([1.5, 1.5])),
            (3, MLVectors.dense([2.0, 2.0])),
            (4, MLVectors.dense([8.0, 8.0])),
            (5, MLVectors.dense([8.5, 8.5])),
            (6, MLVectors.dense([9.0, 9.0]))
        ]
        return spark_session.createDataFrame(data, ["id", "features"])
    
    def test_kmeans(self, clustering_data):
        """測試 K-means 聚類"""
        kmeans = KMeans(k=2, featuresCol="features")
        kmeans_model = kmeans.fit(clustering_data)
        
        predictions = kmeans_model.transform(clustering_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        
        # 檢查聚類中心
        centers = kmeans_model.clusterCenters()
        assert len(centers) == 2
        
        # 檢查預測結果
        cluster_assignments = [row.prediction for row in predictions.collect()]
        assert set(cluster_assignments) == {0, 1}  # 應該有兩個聚類
    
    def test_bisecting_kmeans(self, clustering_data):
        """測試二分 K-means"""
        bkmeans = BisectingKMeans(k=2, featuresCol="features")
        bkmeans_model = bkmeans.fit(clustering_data)
        
        predictions = bkmeans_model.transform(clustering_data)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        
        # 檢查聚類中心
        centers = bkmeans_model.clusterCenters()
        assert len(centers) == 2
    
    def test_clustering_evaluation(self, clustering_data):
        """測試聚類評估"""
        kmeans = KMeans(k=2, featuresCol="features")
        kmeans_model = kmeans.fit(clustering_data)
        
        # 計算 WSSSE (Within Set Sum of Squared Errors)
        wssse = kmeans_model.summary.trainingCost
        assert wssse >= 0
        
        # 計算輪廓係數（如果可用）
        predictions = kmeans_model.transform(clustering_data)
        assert predictions.count() == clustering_data.count()


class TestRecommendation:
    """測試推薦算法"""
    
    @pytest.fixture(scope="function")
    def rating_data(self, spark_session):
        """創建評分數據"""
        data = [
            (1, 1, 5.0),
            (1, 2, 3.0),
            (1, 3, 4.0),
            (2, 1, 4.0),
            (2, 2, 2.0),
            (2, 4, 5.0),
            (3, 1, 3.0),
            (3, 3, 5.0),
            (3, 4, 4.0)
        ]
        return spark_session.createDataFrame(data, ["userId", "movieId", "rating"])
    
    def test_als_recommendation(self, rating_data):
        """測試 ALS 協同過濾"""
        als = ALS(
            userCol="userId",
            itemCol="movieId",
            ratingCol="rating",
            rank=2,
            maxIter=5,
            regParam=0.1,
            coldStartStrategy="drop"
        )
        
        als_model = als.fit(rating_data)
        
        # 為用戶推薦物品
        user_recs = als_model.recommendForAllUsers(2)
        assert "userId" in user_recs.columns
        assert "recommendations" in user_recs.columns
        
        # 為物品推薦用戶
        item_recs = als_model.recommendForAllItems(2)
        assert "movieId" in item_recs.columns
        assert "recommendations" in item_recs.columns
        
        # 預測評分
        predictions = als_model.transform(rating_data)
        assert "prediction" in predictions.columns


class TestPipeline:
    """測試機器學習管道"""
    
    def test_simple_pipeline(self, spark_session):
        """測試簡單管道"""
        # 創建測試數據
        data = [
            (1, "category_a", 1.0, 2.0, 1.0),
            (2, "category_b", 2.0, 3.0, 0.0),
            (3, "category_a", 3.0, 4.0, 1.0),
            (4, "category_c", 4.0, 5.0, 0.0)
        ]
        df = spark_session.createDataFrame(data, ["id", "category", "feature1", "feature2", "label"])
        
        # 建立管道
        indexer = StringIndexer(inputCol="category", outputCol="category_index")
        assembler = VectorAssembler(
            inputCols=["category_index", "feature1", "feature2"],
            outputCol="features"
        )
        lr = LogisticRegression(featuresCol="features", labelCol="label")
        
        pipeline = Pipeline(stages=[indexer, assembler, lr])
        
        # 訓練管道
        pipeline_model = pipeline.fit(df)
        
        # 預測
        predictions = pipeline_model.transform(df)
        
        # 檢查結果
        assert "prediction" in predictions.columns
        assert "probability" in predictions.columns
        assert "features" in predictions.columns
    
    def test_cross_validation(self, spark_session):
        """測試交叉驗證"""
        # 創建較大的測試數據集
        data = []
        for i in range(50):
            label = 1.0 if i % 2 == 0 else 0.0
            feature1 = float(i)
            feature2 = float(i * 2)
            data.append((i, feature1, feature2, label))
        
        df = spark_session.createDataFrame(data, ["id", "feature1", "feature2", "label"])
        
        # 建立管道
        assembler = VectorAssembler(
            inputCols=["feature1", "feature2"],
            outputCol="features"
        )
        lr = LogisticRegression(featuresCol="features", labelCol="label")
        
        pipeline = Pipeline(stages=[assembler, lr])
        
        # 參數網格
        param_grid = ParamGridBuilder() \
            .addGrid(lr.regParam, [0.01, 0.1]) \
            .addGrid(lr.maxIter, [5, 10]) \
            .build()
        
        # 交叉驗證
        evaluator = BinaryClassificationEvaluator()
        cv = CrossValidator(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            numFolds=3
        )
        
        # 訓練
        cv_model = cv.fit(df)
        
        # 檢查結果
        assert cv_model.bestModel is not None
        assert len(cv_model.avgMetrics) == len(param_grid)


class TestStatistics:
    """測試統計功能"""
    
    def test_correlation(self, spark_session):
        """測試相關性分析"""
        # 創建測試數據
        data = [
            (1.0, 2.0, 3.0),
            (2.0, 4.0, 6.0),
            (3.0, 6.0, 9.0),
            (4.0, 8.0, 12.0),
            (5.0, 10.0, 15.0)
        ]
        df = spark_session.createDataFrame(data, ["x", "y", "z"])
        
        # 組裝特徵向量
        assembler = VectorAssembler(inputCols=["x", "y", "z"], outputCol="features")
        features_df = assembler.transform(df)
        
        # 轉換為 RDD
        features_rdd = features_df.select("features").rdd.map(lambda row: row.features)
        
        # 計算相關性矩陣
        corr_matrix = Statistics.corr(features_rdd, method="pearson")
        
        # 檢查結果
        assert corr_matrix.shape == (3, 3)
        # 對角線應該都是1（自相關）
        for i in range(3):
            assert abs(corr_matrix[i, i] - 1.0) < 1e-10
    
    def test_summary_statistics(self, spark_session):
        """測試匯總統計"""
        # 創建測試數據
        data = [
            (1.0, 2.0),
            (2.0, 4.0),
            (3.0, 6.0),
            (4.0, 8.0),
            (5.0, 10.0)
        ]
        df = spark_session.createDataFrame(data, ["x", "y"])
        
        # 組裝特徵向量
        assembler = VectorAssembler(inputCols=["x", "y"], outputCol="features")
        features_df = assembler.transform(df)
        
        # 轉換為 RDD
        features_rdd = features_df.select("features").rdd.map(lambda row: row.features)
        
        # 計算匯總統計
        summary = Statistics.colStats(features_rdd)
        
        # 檢查結果
        means = summary.mean()
        assert len(means) == 2
        assert abs(means[0] - 3.0) < 1e-10  # x的平均值
        assert abs(means[1] - 6.0) < 1e-10  # y的平均值
        
        variances = summary.variance()
        assert len(variances) == 2


class TestModelPersistence:
    """測試模型持久化"""
    
    def test_save_load_model(self, spark_session, temp_dir):
        """測試模型保存和加載"""
        # 創建訓練數據
        data = [
            (1.0, MLVectors.dense([1.0, 2.0])),
            (0.0, MLVectors.dense([2.0, 1.0])),
            (1.0, MLVectors.dense([3.0, 4.0])),
            (0.0, MLVectors.dense([4.0, 3.0]))
        ]
        df = spark_session.createDataFrame(data, ["label", "features"])
        
        # 訓練模型
        lr = LogisticRegression(featuresCol="features", labelCol="label")
        lr_model = lr.fit(df)
        
        # 保存模型
        model_path = os.path.join(temp_dir, "lr_model")
        lr_model.write().overwrite().save(model_path)
        
        # 加載模型
        from pyspark.ml.classification import LogisticRegressionModel
        loaded_model = LogisticRegressionModel.load(model_path)
        
        # 檢查模型
        assert loaded_model is not None
        
        # 比較預測結果
        original_predictions = lr_model.transform(df)
        loaded_predictions = loaded_model.transform(df)
        
        original_pred = [row.prediction for row in original_predictions.collect()]
        loaded_pred = [row.prediction for row in loaded_predictions.collect()]
        
        assert original_pred == loaded_pred