# 推薦系統

一個基於 Apache Spark MLlib 的推薦系統，使用協同過濾演算法 (Collaborative Filtering) 和 ALS (Alternating Least Squares) 矩陣分解技術為用戶推薦商品。

## 功能特色

### 🎯 推薦演算法
- **協同過濾**: 基於用戶行為的推薦
- **矩陣分解 (ALS)**: 高效的大規模推薦演算法
- **隱式反饋**: 支援點擊、瀏覽等隱式反饋
- **冷啟動處理**: 針對新用戶和新商品的推薦策略

### 📊 推薦品質
- **覆蓋率分析**: 推薦系統覆蓋的商品比例
- **多樣性評估**: 推薦結果的多樣性分析
- **新穎性計算**: 推薦商品的新穎性評估
- **準確性指標**: RMSE、MAE、R² 等評估指標

### 🔧 系統功能
- **批次推薦**: 為大量用戶批次生成推薦
- **即時推薦**: 為單一用戶即時生成推薦
- **相似商品**: 基於商品特徵的相似推薦
- **超參數調優**: 自動化超參數最佳化

### 📈 分析功能
- **資料品質分析**: 評分資料的品質評估
- **用戶畫像生成**: 詳細的用戶偏好分析
- **商品分析**: 商品受歡迎程度和類別分析
- **推薦效果分析**: 推薦結果的詳細分析

## 安裝需求

```bash
# Python 依賴
pip install pyspark pandas matplotlib seaborn numpy scikit-learn

# 或使用 poetry（推薦）
poetry install
```

## 快速開始

### 1. 準備資料

推薦系統需要以下資料：

**評分資料 (ratings.csv)**
```csv
user_id,item_id,rating,timestamp
1,101,4.5,1609459200
1,102,3.0,1609459300
2,101,5.0,1609459400
```

**商品資料 (products.csv，可選)**
```csv
item_id,title,category,price,brand,description
101,iPhone 13,Electronics,999.99,Apple,Latest iPhone model
102,MacBook Pro,Electronics,1999.99,Apple,Professional laptop
```

**用戶資料 (users.csv，可選)**
```csv
user_id,age,gender,occupation,location
1,25,M,Engineer,New York
2,32,F,Teacher,Los Angeles
```

### 2. 基本使用

```bash
# 訓練推薦模型
python recommendation_system.py --ratings-path ratings.csv --products-path products.csv --mode train

# 為特定用戶生成推薦
python recommendation_system.py --ratings-path ratings.csv --products-path products.csv --mode recommend --user-id 1

# 使用範例資料訓練
python recommendation_system.py --ratings-path ./sample_ratings.csv --mode train
```

### 3. 查看結果

```bash
# 查看訓練報告
cat recommendation_output/training_report.json

# 查看可視化圖表
open recommendation_output/rating_distribution.png
open recommendation_output/model_metrics.png
```

## 詳細使用指南

### 資料格式要求

#### 評分資料
- **user_id**: 用戶唯一標識符 (整數)
- **item_id**: 商品唯一標識符 (整數)
- **rating**: 評分值 (浮點數，通常 1-5)
- **timestamp**: 時間戳 (整數，可選)

#### 商品資料
- **item_id**: 商品唯一標識符 (整數)
- **title**: 商品標題 (字串)
- **category**: 商品類別 (字串)
- **price**: 商品價格 (浮點數)
- **brand**: 品牌 (字串)
- **description**: 商品描述 (字串)

#### 用戶資料
- **user_id**: 用戶唯一標識符 (整數)
- **age**: 年齡 (整數)
- **gender**: 性別 (字串)
- **occupation**: 職業 (字串)
- **location**: 地區 (字串)

### 參數配置

```python
# 建立自訂配置
config = RecommendationConfig(
    ratings_path="./ratings.csv",
    products_path="./products.csv",
    users_path="./users.csv",
    output_path="./output",
    model_path="./model",
    num_recommendations=10,
    min_ratings_per_user=5,
    min_ratings_per_item=5,
    test_ratio=0.2,
    als_config={
        "rank": 50,          # 潛在因子數量
        "maxIter": 10,       # 最大迭代次數
        "regParam": 0.01,    # 正則化參數
        "alpha": 1.0,        # 隱式反饋的信心參數
        "coldStartStrategy": "drop",  # 冷啟動策略
        "nonnegative": True  # 非負約束
    }
)
```

### ALS 超參數說明

- **rank**: 潛在因子的數量，影響模型的複雜度
- **maxIter**: 最大迭代次數，影響收斂速度
- **regParam**: 正則化參數，防止過擬合
- **alpha**: 隱式反饋的信心參數
- **coldStartStrategy**: 處理新用戶/商品的策略

## 使用範例

### 1. 生成範例資料

```bash
# 生成範例評分資料
python -c "
import pandas as pd
import numpy as np
import random

# 生成 1000 用戶，500 商品，50000 評分
users = range(1, 1001)
items = range(1, 501)
ratings = []

for _ in range(50000):
    user_id = random.choice(users)
    item_id = random.choice(items)
    rating = random.uniform(1, 5)
    timestamp = random.randint(1577836800, 1609459200)  # 2020年
    ratings.append([user_id, item_id, rating, timestamp])

df = pd.DataFrame(ratings, columns=['user_id', 'item_id', 'rating', 'timestamp'])
df.to_csv('sample_ratings.csv', index=False)
print('範例評分資料已生成: sample_ratings.csv')
"

# 生成範例商品資料
python -c "
import pandas as pd
import random

categories = ['Electronics', 'Books', 'Clothing', 'Sports', 'Home']
brands = ['Apple', 'Samsung', 'Nike', 'Adidas', 'Sony']

products = []
for i in range(1, 501):
    products.append([
        i,
        f'Product {i}',
        random.choice(categories),
        round(random.uniform(10, 1000), 2),
        random.choice(brands),
        f'Description for product {i}'
    ])

df = pd.DataFrame(products, columns=['item_id', 'title', 'category', 'price', 'brand', 'description'])
df.to_csv('sample_products.csv', index=False)
print('範例商品資料已生成: sample_products.csv')
"
```

### 2. 訓練模型

```bash
# 使用範例資料訓練
python recommendation_system.py \
    --ratings-path sample_ratings.csv \
    --products-path sample_products.csv \
    --mode train \
    --output-path ./results \
    --model-path ./model

# 查看訓練結果
cat results/training_report.json | jq '.model_metrics'
```

### 3. 生成推薦

```bash
# 為用戶 1 生成 10 個推薦
python recommendation_system.py \
    --ratings-path sample_ratings.csv \
    --products-path sample_products.csv \
    --mode recommend \
    --user-id 1 \
    --num-recommendations 10

# 為用戶 100 生成 5 個推薦
python recommendation_system.py \
    --ratings-path sample_ratings.csv \
    --products-path sample_products.csv \
    --mode recommend \
    --user-id 100 \
    --num-recommendations 5
```

### 4. 批次推薦

```python
# 使用 Python API 進行批次推薦
from pyspark.sql import SparkSession
from recommendation_system import RecommendationSystem, RecommendationConfig

# 建立 Spark 會話
spark = SparkSession.builder.appName("BatchRecommendation").getOrCreate()

# 建立配置
config = RecommendationConfig(
    ratings_path="sample_ratings.csv",
    products_path="sample_products.csv",
    model_path="./model"
)

# 建立推薦系統
rec_system = RecommendationSystem(config, spark)

# 為多個用戶生成推薦
user_ids = [1, 2, 3, 4, 5]
for user_id in user_ids:
    recommendations = rec_system.generate_recommendations_for_user(user_id, 10)
    print(f"用戶 {user_id} 的推薦:")
    for rec in recommendations["recommendations"]:
        print(f"  {rec['title']} - 預測評分: {rec['predicted_rating']:.2f}")
```

## 進階功能

### 1. 超參數調優

```python
# 自動超參數調優
def tune_hyperparameters():
    # 載入資料
    ratings_df = spark.read.csv("ratings.csv", header=True, inferSchema=True)
    
    # 分割資料
    train_df, validation_df = ratings_df.randomSplit([0.8, 0.2])
    
    # 執行調優
    best_params = rec_engine.tune_hyperparameters(train_df, validation_df)
    
    return best_params
```

### 2. 即時推薦服務

```python
# 建立即時推薦服務
class RecommendationService:
    def __init__(self, model_path):
        self.spark = SparkSession.builder.getOrCreate()
        self.model = ALSModel.load(model_path)
    
    def get_recommendations(self, user_id, num_recommendations=10):
        user_df = self.spark.createDataFrame([(user_id,)], ["user_id"])
        recommendations = self.model.recommendForUserSubset(user_df, num_recommendations)
        return recommendations.collect()
```

### 3. 混合推薦

```python
# 結合協同過濾和基於內容的推薦
class HybridRecommendation:
    def __init__(self, cf_model, content_model):
        self.cf_model = cf_model
        self.content_model = content_model
    
    def recommend(self, user_id, num_recommendations=10):
        # 協同過濾推薦
        cf_recs = self.cf_model.recommend(user_id, num_recommendations)
        
        # 基於內容的推薦
        content_recs = self.content_model.recommend(user_id, num_recommendations)
        
        # 混合推薦結果
        hybrid_recs = self.combine_recommendations(cf_recs, content_recs)
        
        return hybrid_recs
```

## 評估指標

### 1. 準確性指標

```python
# RMSE (Root Mean Square Error)
rmse = evaluator.setMetricName("rmse").evaluate(predictions)

# MAE (Mean Absolute Error)
mae = evaluator.setMetricName("mae").evaluate(predictions)

# R² (決定係數)
r2 = evaluator.setMetricName("r2").evaluate(predictions)
```

### 2. 排名指標

```python
# 計算 Precision@K
def precision_at_k(predictions, k=10):
    # 實現 Precision@K 計算
    pass

# 計算 Recall@K
def recall_at_k(predictions, k=10):
    # 實現 Recall@K 計算
    pass

# 計算 NDCG@K
def ndcg_at_k(predictions, k=10):
    # 實現 NDCG@K 計算
    pass
```

### 3. 多樣性指標

```python
# 計算推薦多樣性
def calculate_diversity(recommendations, products_df):
    # 計算類別多樣性
    categories = recommendations.join(products_df, "item_id").select("category").distinct().count()
    total_categories = products_df.select("category").distinct().count()
    
    return categories / total_categories
```

## 效能優化

### 1. Spark 配置優化

```python
# 優化 Spark 配置
spark = SparkSession.builder \
    .appName("RecommendationSystem") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.cores", "4") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()
```

### 2. 資料分區策略

```python
# 按用戶分區
ratings_df = ratings_df.repartition("user_id")

# 按商品分區
ratings_df = ratings_df.repartition("item_id")

# 使用 Bucketing
ratings_df.write \
    .bucketBy(10, "user_id") \
    .sortBy("timestamp") \
    .saveAsTable("ratings_bucketed")
```

### 3. 快取策略

```python
# 快取經常使用的資料
ratings_df.cache()

# 使用不同的儲存級別
from pyspark.storagelevel import StorageLevel
ratings_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

## 部署指南

### 1. 本地部署

```bash
# 啟動推薦服務
python recommendation_service.py --model-path ./model --port 8080

# 測試推薦服務
curl -X POST http://localhost:8080/recommend \
  -H "Content-Type: application/json" \
  -d '{"user_id": 1, "num_recommendations": 10}'
```

### 2. Docker 部署

```dockerfile
FROM python:3.8-slim

WORKDIR /app

# 安裝 Java (Spark 需要)
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk && \
    rm -rf /var/lib/apt/lists/*

# 設定 Java 環境變數
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# 複製依賴文件
COPY requirements.txt .
RUN pip install -r requirements.txt

# 複製應用程式
COPY . .

# 暴露端口
EXPOSE 8080

# 啟動服務
CMD ["python", "recommendation_service.py", "--model-path", "./model", "--port", "8080"]
```

### 3. Kubernetes 部署

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: recommendation-system
spec:
  replicas: 3
  selector:
    matchLabels:
      app: recommendation-system
  template:
    metadata:
      labels:
        app: recommendation-system
    spec:
      containers:
      - name: recommendation-system
        image: recommendation-system:latest
        ports:
        - containerPort: 8080
        resources:
          requests:
            memory: "4Gi"
            cpu: "2"
          limits:
            memory: "8Gi"
            cpu: "4"
        env:
        - name: SPARK_EXECUTOR_MEMORY
          value: "4g"
        - name: SPARK_EXECUTOR_CORES
          value: "2"
---
apiVersion: v1
kind: Service
metadata:
  name: recommendation-service
spec:
  selector:
    app: recommendation-system
  ports:
  - port: 80
    targetPort: 8080
  type: LoadBalancer
```

## 監控和維護

### 1. 模型性能監控

```python
# 監控推薦品質
def monitor_recommendation_quality():
    # 計算即時評估指標
    current_rmse = calculate_current_rmse()
    
    # 監控推薦覆蓋率
    coverage = calculate_coverage()
    
    # 監控推薦多樣性
    diversity = calculate_diversity()
    
    # 發送警報
    if current_rmse > threshold:
        send_alert("Model performance degraded")
```

### 2. A/B 測試

```python
# 實施 A/B 測試
def ab_test_recommendations():
    # 隨機分配用戶到不同的推薦策略
    if user_id % 2 == 0:
        return strategy_a.recommend(user_id)
    else:
        return strategy_b.recommend(user_id)
```

### 3. 模型更新

```python
# 定期更新模型
def update_model():
    # 載入新的評分資料
    new_ratings = load_new_ratings()
    
    # 重新訓練模型
    new_model = train_model(new_ratings)
    
    # 評估新模型
    if evaluate_model(new_model) > current_model_performance:
        deploy_model(new_model)
```

## 常見問題和解決方案

### 1. 記憶體不足

```bash
# 增加 Spark 記憶體
export SPARK_EXECUTOR_MEMORY=8g
export SPARK_DRIVER_MEMORY=4g

# 或在程式碼中設定
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.driver.memory", "4g")
```

### 2. 冷啟動問題

```python
# 處理新用戶
def handle_new_user(user_id):
    # 使用熱門商品推薦
    popular_items = get_popular_items(limit=10)
    
    # 或使用基於內容的推薦
    content_based_recs = get_content_based_recommendations(user_id)
    
    return popular_items + content_based_recs
```

### 3. 資料稀疏性

```python
# 處理稀疏資料
def handle_sparse_data():
    # 使用隱式反饋
    implicit_feedback = generate_implicit_feedback()
    
    # 結合顯式和隱式反饋
    combined_data = combine_feedback(explicit_ratings, implicit_feedback)
    
    return combined_data
```

## 擴展功能

### 1. 深度學習推薦

```python
# 使用深度學習模型
from pyspark.ml.recommendation import DeepFM

# 建立 DeepFM 模型
deep_fm = DeepFM(
    userCol="user_id",
    itemCol="item_id",
    ratingCol="rating",
    featuresCol="features"
)

# 訓練模型
deep_model = deep_fm.fit(training_data)
```

### 2. 圖神經網絡

```python
# 使用圖神經網絡
from pyspark.ml.recommendation import GraphSAGE

# 建立用戶-商品圖
user_item_graph = build_user_item_graph(ratings_df)

# 訓練 GraphSAGE 模型
graph_model = GraphSAGE().fit(user_item_graph)
```

### 3. 多目標推薦

```python
# 多目標推薦（點擊率 + 轉換率）
class MultiObjectiveRecommendation:
    def __init__(self):
        self.ctr_model = build_ctr_model()
        self.cvr_model = build_cvr_model()
    
    def recommend(self, user_id):
        # 結合多個目標
        ctr_scores = self.ctr_model.predict(user_id)
        cvr_scores = self.cvr_model.predict(user_id)
        
        # 加權組合
        final_scores = 0.7 * ctr_scores + 0.3 * cvr_scores
        
        return final_scores
```

## 授權

本專案使用 MIT 授權條款。

## 貢獻

歡迎提交 Issues 和 Pull Requests！

## 聯絡方式

如有問題，請聯絡：[your-email@example.com](mailto:your-email@example.com)