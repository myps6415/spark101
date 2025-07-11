# 第6章：MLlib 機器學習

## 📚 學習目標

- 理解 Spark MLlib 的核心概念
- 掌握特徵工程和數據預處理
- 學會使用各種機器學習演算法
- 了解模型評估和調優技巧

## 🎯 本章內容

### 核心概念
- **MLlib** - Spark 機器學習庫
- **Pipeline** - 機器學習管道
- **Feature Engineering** - 特徵工程
- **Model Evaluation** - 模型評估

### 檔案說明
- `mllib_basics.py` - 機器學習基礎操作

## 🚀 開始學習

### 執行範例

```bash
# 執行機器學習基礎範例
poetry run python examples/chapter06/mllib_basics.py

# 或使用 Makefile
make run-chapter06
```

## 🔍 深入理解

### MLlib 架構

```
原始數據 → 特徵工程 → 模型訓練 → 模型評估 → 模型部署
    ↓         ↓         ↓         ↓         ↓
  DataFrame  Vector   Algorithm  Metrics  Pipeline
```

### 主要組件

#### 1. 特徵工程
```python
from pyspark.ml.feature import VectorAssembler, StringIndexer, StandardScaler

# 向量化
assembler = VectorAssembler(
    inputCols=["age", "salary", "experience"],
    outputCol="features"
)

# 字串索引
indexer = StringIndexer(
    inputCol="category",
    outputCol="category_index"
)

# 標準化
scaler = StandardScaler(
    inputCol="features",
    outputCol="scaled_features"
)
```

#### 2. 機器學習演算法
```python
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.regression import LinearRegression, RandomForestRegressor

# 邏輯回歸
lr = LogisticRegression(
    featuresCol="features",
    labelCol="label"
)

# 隨機森林
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100
)
```

#### 3. 模型評估
```python
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator

# 二元分類評估
evaluator = BinaryClassificationEvaluator(
    rawPredictionCol="prediction",
    labelCol="label",
    metricName="areaUnderROC"
)

# 回歸評估
evaluator = RegressionEvaluator(
    predictionCol="prediction",
    labelCol="label",
    metricName="rmse"
)
```

## 🛠️ 機器學習管道

### 1. 創建管道
```python
from pyspark.ml import Pipeline

# 定義管道階段
pipeline = Pipeline(stages=[
    assembler,      # 特徵向量化
    scaler,         # 標準化
    lr             # 模型訓練
])

# 訓練管道
model = pipeline.fit(training_data)

# 預測
predictions = model.transform(test_data)
```

### 2. 管道的優勢
- **重複使用** - 可以應用到新數據
- **一致性** - 確保相同的處理步驟
- **模組化** - 易於維護和修改

## 📊 常用演算法

### 1. 分類算法

#### 邏輯回歸
```python
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=10,
    regParam=0.01
)
```

#### 決策樹
```python
from pyspark.ml.classification import DecisionTreeClassifier

dt = DecisionTreeClassifier(
    featuresCol="features",
    labelCol="label",
    maxDepth=5
)
```

#### 隨機森林
```python
from pyspark.ml.classification import RandomForestClassifier

rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=5
)
```

### 2. 回歸算法

#### 線性回歸
```python
from pyspark.ml.regression import LinearRegression

lr = LinearRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=10,
    regParam=0.3
)
```

#### 決策樹回歸
```python
from pyspark.ml.regression import DecisionTreeRegressor

dt = DecisionTreeRegressor(
    featuresCol="features",
    labelCol="label",
    maxDepth=5
)
```

### 3. 聚類算法

#### K-means
```python
from pyspark.ml.clustering import KMeans

kmeans = KMeans(
    featuresCol="features",
    predictionCol="cluster",
    k=3,
    seed=42
)
```

#### 高斯混合模型
```python
from pyspark.ml.clustering import GaussianMixture

gmm = GaussianMixture(
    featuresCol="features",
    predictionCol="cluster",
    k=3
)
```

## 🔧 特徵工程

### 1. 數值特徵處理
```python
from pyspark.ml.feature import StandardScaler, MinMaxScaler, Normalizer

# 標準化
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# 最大最小縮放
minmax_scaler = MinMaxScaler(inputCol="features", outputCol="scaled_features")

# 正規化
normalizer = Normalizer(inputCol="features", outputCol="normalized_features")
```

### 2. 類別特徵處理
```python
from pyspark.ml.feature import StringIndexer, OneHotEncoder

# 字串索引
indexer = StringIndexer(inputCol="category", outputCol="category_index")

# 獨熱編碼
encoder = OneHotEncoder(inputCol="category_index", outputCol="category_vec")
```

### 3. 文本特徵處理
```python
from pyspark.ml.feature import Tokenizer, HashingTF, IDF

# 分詞
tokenizer = Tokenizer(inputCol="text", outputCol="words")

# 哈希詞頻
hashingTF = HashingTF(inputCol="words", outputCol="tf_features")

# 逆文件頻率
idf = IDF(inputCol="tf_features", outputCol="tfidf_features")
```

## 📈 模型評估

### 1. 分類指標
```python
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

evaluator = MulticlassClassificationEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = evaluator.evaluate(predictions)
```

### 2. 回歸指標
```python
from pyspark.ml.evaluation import RegressionEvaluator

evaluator = RegressionEvaluator(
    labelCol="label",
    predictionCol="prediction",
    metricName="rmse"
)

rmse = evaluator.evaluate(predictions)
```

## 🎯 超參數調優

### 1. 參數網格搜索
```python
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

# 創建參數網格
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.1, 0.01]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0]) \
    .build()

# 交叉驗證
crossval = CrossValidator(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3
)

# 訓練
cv_model = crossval.fit(training_data)
```

### 2. 訓練驗證分割
```python
from pyspark.ml.tuning import TrainValidationSplit

tvs = TrainValidationSplit(
    estimator=lr,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    trainRatio=0.8
)

tvs_model = tvs.fit(training_data)
```

## 📝 練習建議

### 基礎練習
1. 實現基本的分類和回歸任務
2. 嘗試不同的特徵工程技術
3. 比較不同演算法的性能

### 進階練習
1. 建立完整的機器學習管道
2. 實施交叉驗證和超參數調優
3. 處理不平衡數據集

## 🛠️ 實用技巧

### 1. 數據準備
```python
# 處理缺失值
df = df.fillna({"feature1": 0, "feature2": "unknown"})

# 處理異常值
df = df.filter(col("feature") > 0)

# 數據採樣
sample_df = df.sample(0.1, seed=42)
```

### 2. 特徵選擇
```python
from pyspark.ml.feature import ChiSqSelector

selector = ChiSqSelector(
    featuresCol="features",
    outputCol="selected_features",
    labelCol="label",
    numTopFeatures=10
)
```

### 3. 模型持久化
```python
# 保存模型
model.write().overwrite().save("path/to/model")

# 載入模型
from pyspark.ml.classification import LogisticRegressionModel
loaded_model = LogisticRegressionModel.load("path/to/model")
```

## 🔧 疑難排解

### 常見問題

**Q: 特徵向量化失敗？**
A: 確保所有輸入列都是數值型，或先進行適當的類型轉換。

**Q: 模型性能差？**
A: 檢查特徵工程、嘗試不同的演算法、調整超參數。

**Q: 記憶體不足？**
A: 減少特徵維度、使用採樣、增加執行器記憶體。

## 💡 最佳實踐

1. **數據探索** - 充分理解數據特性
2. **特徵工程** - 投入時間創建有意義的特徵
3. **模型驗證** - 使用交叉驗證評估模型
4. **超參數調優** - 系統性地搜索最佳參數
5. **模型解釋** - 理解模型的決策邏輯

## 📖 相關文檔

- [MLlib Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [ML Pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html)
- [Feature Extraction](https://spark.apache.org/docs/latest/ml-features.html)

## ➡️ 下一步

完成本章後，建議繼續學習：
- [第7章：性能調優](../chapter07/README.md)
- 了解 Spark 性能優化技巧
- 學習集群部署和監控

## 🎯 學習檢核

完成本章學習後，你應該能夠：
- [ ] 理解 MLlib 的核心概念
- [ ] 進行特徵工程和數據預處理
- [ ] 使用各種機器學習演算法
- [ ] 建立完整的機器學習管道
- [ ] 評估和調優模型性能

## 🗂️ 章節文件總覽

### mllib_basics.py
- 機器學習基礎操作
- 特徵工程示例
- 多種演算法實現
- 模型評估和調優
- 文本挖掘和降維
- 模型持久化