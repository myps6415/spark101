#!/usr/bin/env python3
"""
第7章練習4：SQL 和執行計劃優化
學習如何優化 Spark SQL 查詢和分析執行計劃
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, avg, sum as spark_sum, max as spark_max, \
    min as spark_min, stddev, rand, round as spark_round, desc, asc, \
    expr, broadcast, collect_list, explode, array, struct, \
    window, to_timestamp, date_format, year, month, dayofweek, \
    row_number, dense_rank, lag, lead, first, last
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, \
    TimestampType, BooleanType, ArrayType, DateType

import time
import random
from datetime import datetime, timedelta
import re

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("SQL執行計劃優化練習") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.cbo.enabled", "true") \
        .config("spark.sql.cbo.joinReorder.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    print("=== 第7章練習4：SQL 和執行計劃優化 ===")
    
    # 1. 執行計劃分析工具
    print("\n1. 執行計劃分析工具:")
    
    # 1.1 執行計劃分析函數
    print("\n1.1 執行計劃分析工具:")
    
    def analyze_execution_plan(df, query_name="Query", detailed=True):
        """詳細分析執行計劃"""
        print(f"\n{'='*20} {query_name} 執行計劃分析 {'='*20}")
        
        if detailed:
            print("詳細執行計劃:")
            df.explain(True)
        else:
            print("簡單執行計劃:")
            df.explain()
        
        # 格式化輸出
        print("\n格式化物理計劃:")
        try:
            df.explain("formatted")
        except:
            print("格式化計劃不可用，使用標準計劃")
            df.explain()
        
        return df
    
    def extract_plan_metrics(plan_string):
        """從執行計劃中提取關鍵指標"""
        metrics = {
            "joins": len(re.findall(r'Join', plan_string)),
            "exchanges": len(re.findall(r'Exchange', plan_string)),
            "sorts": len(re.findall(r'Sort', plan_string)),
            "aggregates": len(re.findall(r'Aggregate|HashAggregate', plan_string)),
            "broadcasts": len(re.findall(r'BroadcastHashJoin', plan_string)),
            "shuffles": len(re.findall(r'ShuffledHashJoin|SortMergeJoin', plan_string))
        }
        
        print("執行計劃指標:")
        for metric, count in metrics.items():
            print(f"- {metric}: {count}")
        
        return metrics
    
    def time_query_execution(df, query_name="Query"):
        """測量查詢執行時間"""
        start_time = time.time()
        result = df.collect()
        execution_time = time.time() - start_time
        
        print(f"{query_name} 執行時間: {execution_time:.2f} 秒")
        print(f"結果記錄數: {len(result)}")
        
        return execution_time, len(result)
    
    # 1.2 創建測試數據集
    print("\n1.2 創建複雜測試數據集:")
    
    def create_test_tables():
        """創建複雜的測試數據表"""
        
        # 訂單表 - 大表
        orders_data = []
        for i in range(100000):
            order_date = datetime.now() - timedelta(days=random.randint(1, 365))
            orders_data.append((
                i + 1,
                f"customer_{random.randint(1, 10000)}",
                f"product_{random.randint(1, 1000)}",
                random.randint(1, 10),
                random.uniform(10, 1000),
                order_date,
                random.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
                random.choice(["online", "store", "phone"]),
                random.choice(["US", "CN", "JP", "UK", "DE", "FR"])
            ))
        
        orders_df = spark.createDataFrame(orders_data, [
            "order_id", "customer_id", "product_id", "quantity", "price", 
            "order_date", "status", "channel", "country"
        ])
        
        # 客戶表 - 中等大小
        customers_data = []
        for i in range(10000):
            reg_date = datetime.now() - timedelta(days=random.randint(30, 1000))
            customers_data.append((
                f"customer_{i + 1}",
                f"Customer {i + 1}",
                random.randint(18, 80),
                random.choice(["M", "F"]),
                random.choice(["Premium", "Standard", "Basic"]),
                reg_date,
                random.choice(["US", "CN", "JP", "UK", "DE", "FR"]),
                random.uniform(20000, 200000),
                random.choice([True, False])
            ))
        
        customers_df = spark.createDataFrame(customers_data, [
            "customer_id", "customer_name", "age", "gender", "tier", 
            "registration_date", "country", "annual_income", "is_active"
        ])
        
        # 產品表 - 小表
        products_data = []
        categories = ["Electronics", "Books", "Clothing", "Home", "Sports", "Beauty"]
        for i in range(1000):
            products_data.append((
                f"product_{i + 1}",
                f"Product {i + 1}",
                random.choice(categories),
                random.uniform(5, 500),
                random.uniform(10, 1000),
                random.randint(0, 1000),
                random.choice([True, False])
            ))
        
        products_df = spark.createDataFrame(products_data, [
            "product_id", "product_name", "category", "cost", "price", 
            "stock_quantity", "is_available"
        ])
        
        # 促銷表 - 小表
        promotions_data = []
        for i in range(100):
            start_date = datetime.now() - timedelta(days=random.randint(1, 100))
            end_date = start_date + timedelta(days=random.randint(7, 30))
            promotions_data.append((
                i + 1,
                f"PROMO_{i + 1}",
                random.uniform(0.05, 0.5),
                start_date,
                end_date,
                random.choice(categories),
                random.choice([True, False])
            ))
        
        promotions_df = spark.createDataFrame(promotions_data, [
            "promo_id", "promo_code", "discount_rate", "start_date", 
            "end_date", "category", "is_active"
        ])
        
        return orders_df, customers_df, products_df, promotions_df
    
    orders_df, customers_df, products_df, promotions_df = create_test_tables()
    
    print(f"訂單表: {orders_df.count()} 記錄")
    print(f"客戶表: {customers_df.count()} 記錄") 
    print(f"產品表: {products_df.count()} 記錄")
    print(f"促銷表: {promotions_df.count()} 記錄")
    
    # 創建臨時視圖
    orders_df.createOrReplaceTempView("orders")
    customers_df.createOrReplaceTempView("customers")
    products_df.createOrReplaceTempView("products")
    promotions_df.createOrReplaceTempView("promotions")
    
    # 2. JOIN 策略優化
    print("\n2. JOIN 策略優化:")
    
    # 2.1 不同 JOIN 策略比較
    print("\n2.1 JOIN 策略性能比較:")
    
    def test_join_strategies():
        """測試不同 JOIN 策略的性能"""
        
        # 基本JOIN查詢
        base_query = """
            SELECT 
                c.customer_name,
                c.tier,
                p.product_name,
                p.category,
                o.quantity,
                o.price,
                o.order_date
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
            WHERE o.status = 'delivered'
                AND c.is_active = true
                AND p.is_available = true
        """
        
        join_strategies = [
            ("默認策略", base_query),
            ("廣播客戶表", base_query.replace("JOIN customers c", "JOIN /*+ BROADCAST(c) */ customers c")),
            ("廣播產品表", base_query.replace("JOIN products p", "JOIN /*+ BROADCAST(p) */ products p")),
            ("廣播兩表", base_query.replace("JOIN customers c", "JOIN /*+ BROADCAST(c) */ customers c")
                                    .replace("JOIN products p", "JOIN /*+ BROADCAST(p) */ products p")),
            ("Shuffle Hash Join", base_query.replace("JOIN customers c", "JOIN /*+ SHUFFLEHASH(c) */ customers c")),
            ("Sort Merge Join", base_query.replace("JOIN customers c", "JOIN /*+ MERGE(c) */ customers c"))
        ]
        
        join_results = []
        
        for strategy_name, query in join_strategies:
            print(f"\n測試 JOIN 策略: {strategy_name}")
            
            # 執行查詢
            result_df = spark.sql(query)
            
            # 分析執行計劃
            analyze_execution_plan(result_df, strategy_name, detailed=False)
            
            # 測量執行時間
            exec_time, result_count = time_query_execution(result_df, strategy_name)
            
            join_results.append({
                "strategy": strategy_name,
                "execution_time": exec_time,
                "result_count": result_count
            })
        
        # 性能比較總結
        print("\nJOIN 策略性能比較:")
        print("策略\t\t\t執行時間(秒)\t結果數")
        print("-" * 50)
        for result in join_results:
            print(f"{result['strategy']:<25}\t{result['execution_time']:.2f}\t\t{result['result_count']}")
        
        return join_results
    
    join_results = test_join_strategies()
    
    # 2.2 JOIN 重排序優化
    print("\n2.2 JOIN 重排序優化:")
    
    def test_join_reordering():
        """測試 JOIN 重排序對性能的影響"""
        
        join_orders = [
            ("訂單->客戶->產品", """
                SELECT COUNT(*), AVG(o.price)
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id  
                JOIN products p ON o.product_id = p.product_id
                WHERE c.tier = 'Premium' AND p.category = 'Electronics'
            """),
            ("訂單->產品->客戶", """
                SELECT COUNT(*), AVG(o.price)
                FROM orders o
                JOIN products p ON o.product_id = p.product_id
                JOIN customers c ON o.customer_id = c.customer_id
                WHERE c.tier = 'Premium' AND p.category = 'Electronics'
            """),
            ("客戶->訂單->產品", """
                SELECT COUNT(*), AVG(o.price)
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN products p ON o.product_id = p.product_id
                WHERE c.tier = 'Premium' AND p.category = 'Electronics'
            """)
        ]
        
        reorder_results = []
        
        for order_name, query in join_orders:
            print(f"\nJOIN 順序: {order_name}")
            
            result_df = spark.sql(query)
            exec_time, result_count = time_query_execution(result_df, order_name)
            
            reorder_results.append({
                "join_order": order_name,
                "execution_time": exec_time,
                "result_count": result_count
            })
        
        return reorder_results
    
    reorder_results = test_join_reordering()
    
    # 3. 聚合操作優化
    print("\n3. 聚合操作優化:")
    
    # 3.1 聚合策略比較
    print("\n3.1 聚合策略優化:")
    
    def test_aggregation_optimization():
        """測試不同聚合策略的性能"""
        
        aggregation_queries = [
            ("基本聚合", """
                SELECT 
                    country,
                    status,
                    COUNT(*) as order_count,
                    AVG(price) as avg_price,
                    SUM(price * quantity) as total_revenue
                FROM orders
                GROUP BY country, status
                ORDER BY total_revenue DESC
            """),
            ("窗口函數聚合", """
                SELECT 
                    country,
                    status,
                    order_count,
                    avg_price,
                    total_revenue,
                    ROW_NUMBER() OVER (PARTITION BY country ORDER BY total_revenue DESC) as rank
                FROM (
                    SELECT 
                        country,
                        status,
                        COUNT(*) as order_count,
                        AVG(price) as avg_price,
                        SUM(price * quantity) as total_revenue
                    FROM orders
                    GROUP BY country, status
                )
            """),
            ("多級聚合", """
                WITH country_stats AS (
                    SELECT 
                        country,
                        COUNT(*) as total_orders,
                        SUM(price * quantity) as country_revenue
                    FROM orders
                    GROUP BY country
                ),
                status_stats AS (
                    SELECT 
                        country,
                        status,
                        COUNT(*) as status_orders,
                        AVG(price) as avg_price
                    FROM orders
                    GROUP BY country, status
                )
                SELECT 
                    cs.country,
                    ss.status,
                    ss.status_orders,
                    ss.avg_price,
                    cs.total_orders,
                    cs.country_revenue,
                    (ss.status_orders * 100.0 / cs.total_orders) as status_percentage
                FROM country_stats cs
                JOIN status_stats ss ON cs.country = ss.country
                ORDER BY cs.country_revenue DESC, ss.status_orders DESC
            """)
        ]
        
        aggregation_results = []
        
        for agg_name, query in aggregation_queries:
            print(f"\n聚合策略: {agg_name}")
            
            result_df = spark.sql(query)
            analyze_execution_plan(result_df, agg_name, detailed=False)
            exec_time, result_count = time_query_execution(result_df, agg_name)
            
            aggregation_results.append({
                "strategy": agg_name,
                "execution_time": exec_time,
                "result_count": result_count
            })
        
        return aggregation_results
    
    aggregation_results = test_aggregation_optimization()
    
    # 4. 子查詢優化
    print("\n4. 子查詢優化:")
    
    # 4.1 子查詢重寫測試
    print("\n4.1 子查詢重寫優化:")
    
    def test_subquery_optimization():
        """測試子查詢優化技術"""
        
        subquery_tests = [
            ("相關子查詢", """
                SELECT 
                    c.customer_name,
                    c.tier,
                    (SELECT AVG(o.price) 
                     FROM orders o 
                     WHERE o.customer_id = c.customer_id) as avg_order_value,
                    (SELECT COUNT(*) 
                     FROM orders o 
                     WHERE o.customer_id = c.customer_id) as order_count
                FROM customers c
                WHERE c.is_active = true
                LIMIT 1000
            """),
            ("窗口函數重寫", """
                SELECT 
                    c.customer_name,
                    c.tier,
                    AVG(o.price) OVER (PARTITION BY c.customer_id) as avg_order_value,
                    COUNT(*) OVER (PARTITION BY c.customer_id) as order_count
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                WHERE c.is_active = true
                QUALIFY ROW_NUMBER() OVER (PARTITION BY c.customer_id ORDER BY o.order_id) = 1
                LIMIT 1000
            """),
            ("JOIN重寫", """
                SELECT 
                    c.customer_name,
                    c.tier,
                    COALESCE(o_stats.avg_order_value, 0) as avg_order_value,
                    COALESCE(o_stats.order_count, 0) as order_count
                FROM customers c
                LEFT JOIN (
                    SELECT 
                        customer_id,
                        AVG(price) as avg_order_value,
                        COUNT(*) as order_count
                    FROM orders
                    GROUP BY customer_id
                ) o_stats ON c.customer_id = o_stats.customer_id
                WHERE c.is_active = true
                LIMIT 1000
            """)
        ]
        
        subquery_results = []
        
        for test_name, query in subquery_tests:
            print(f"\n子查詢測試: {test_name}")
            
            result_df = spark.sql(query)
            analyze_execution_plan(result_df, test_name, detailed=False)
            exec_time, result_count = time_query_execution(result_df, test_name)
            
            subquery_results.append({
                "method": test_name,
                "execution_time": exec_time,
                "result_count": result_count
            })
        
        return subquery_results
    
    subquery_results = test_subquery_optimization()
    
    # 5. 複雜查詢優化
    print("\n5. 複雜查詢優化:")
    
    # 5.1 複雜業務查詢優化
    print("\n5.1 複雜業務查詢優化:")
    
    def test_complex_query_optimization():
        """測試複雜查詢的優化技術"""
        
        # 原始複雜查詢
        original_query = """
            WITH customer_metrics AS (
                SELECT 
                    c.customer_id,
                    c.customer_name,
                    c.tier,
                    c.annual_income,
                    COUNT(o.order_id) as total_orders,
                    SUM(o.price * o.quantity) as total_spent,
                    AVG(o.price) as avg_order_value,
                    MAX(o.order_date) as last_order_date,
                    COUNT(DISTINCT p.category) as categories_purchased
                FROM customers c
                LEFT JOIN orders o ON c.customer_id = o.customer_id
                LEFT JOIN products p ON o.product_id = p.product_id
                WHERE c.is_active = true
                    AND (o.status IN ('delivered', 'shipped') OR o.status IS NULL)
                GROUP BY c.customer_id, c.customer_name, c.tier, c.annual_income
            ),
            tier_benchmarks AS (
                SELECT 
                    tier,
                    AVG(total_spent) as avg_spent_by_tier,
                    PERCENTILE_APPROX(total_spent, 0.5) as median_spent_by_tier,
                    AVG(total_orders) as avg_orders_by_tier
                FROM customer_metrics
                WHERE total_orders > 0
                GROUP BY tier
            )
            SELECT 
                cm.customer_id,
                cm.customer_name,
                cm.tier,
                cm.total_orders,
                cm.total_spent,
                cm.avg_order_value,
                cm.categories_purchased,
                tb.avg_spent_by_tier,
                tb.avg_orders_by_tier,
                CASE 
                    WHEN cm.total_spent > tb.avg_spent_by_tier * 1.5 THEN 'High Value'
                    WHEN cm.total_spent > tb.avg_spent_by_tier THEN 'Above Average'
                    WHEN cm.total_spent > tb.avg_spent_by_tier * 0.5 THEN 'Average'
                    ELSE 'Below Average'
                END as customer_segment,
                DATEDIFF(CURRENT_DATE(), cm.last_order_date) as days_since_last_order
            FROM customer_metrics cm
            LEFT JOIN tier_benchmarks tb ON cm.tier = tb.tier
            WHERE cm.total_orders > 0
            ORDER BY cm.total_spent DESC
            LIMIT 1000
        """
        
        # 優化版本1：減少JOIN操作
        optimized_query_v1 = """
            WITH enriched_orders AS (
                SELECT 
                    o.customer_id,
                    o.price * o.quantity as order_value,
                    o.price,
                    o.order_date,
                    p.category,
                    c.customer_name,
                    c.tier,
                    c.annual_income
                FROM orders o
                JOIN customers c ON o.customer_id = c.customer_id
                JOIN products p ON o.product_id = p.product_id
                WHERE c.is_active = true
                    AND o.status IN ('delivered', 'shipped')
            ),
            customer_aggregates AS (
                SELECT 
                    customer_id,
                    customer_name,
                    tier,
                    annual_income,
                    COUNT(*) as total_orders,
                    SUM(order_value) as total_spent,
                    AVG(price) as avg_order_value,
                    MAX(order_date) as last_order_date,
                    COUNT(DISTINCT category) as categories_purchased
                FROM enriched_orders
                GROUP BY customer_id, customer_name, tier, annual_income
            ),
            tier_stats AS (
                SELECT 
                    tier,
                    AVG(total_spent) as avg_spent_by_tier,
                    AVG(total_orders) as avg_orders_by_tier
                FROM customer_aggregates
                GROUP BY tier
            )
            SELECT 
                ca.*,
                ts.avg_spent_by_tier,
                ts.avg_orders_by_tier,
                CASE 
                    WHEN ca.total_spent > ts.avg_spent_by_tier * 1.5 THEN 'High Value'
                    WHEN ca.total_spent > ts.avg_spent_by_tier THEN 'Above Average'
                    WHEN ca.total_spent > ts.avg_spent_by_tier * 0.5 THEN 'Average'
                    ELSE 'Below Average'
                END as customer_segment,
                DATEDIFF(CURRENT_DATE(), ca.last_order_date) as days_since_last_order
            FROM customer_aggregates ca
            JOIN tier_stats ts ON ca.tier = ts.tier
            ORDER BY ca.total_spent DESC
            LIMIT 1000
        """
        
        # 優化版本2：使用廣播變量
        optimized_query_v2 = optimized_query_v1.replace(
            "JOIN customers c", "JOIN /*+ BROADCAST(c) */ customers c"
        ).replace(
            "JOIN products p", "JOIN /*+ BROADCAST(p) */ products p"
        )
        
        complex_queries = [
            ("原始複雜查詢", original_query),
            ("優化版本1", optimized_query_v1),
            ("優化版本2 (廣播)", optimized_query_v2)
        ]
        
        complex_results = []
        
        for query_name, query in complex_queries:
            print(f"\n複雜查詢測試: {query_name}")
            
            try:
                result_df = spark.sql(query)
                analyze_execution_plan(result_df, query_name, detailed=False)
                exec_time, result_count = time_query_execution(result_df, query_name)
                
                complex_results.append({
                    "query_version": query_name,
                    "execution_time": exec_time,
                    "result_count": result_count,
                    "status": "success"
                })
            except Exception as e:
                print(f"查詢執行失敗: {str(e)}")
                complex_results.append({
                    "query_version": query_name,
                    "execution_time": 0,
                    "result_count": 0,
                    "status": f"failed: {str(e)}"
                })
        
        return complex_results
    
    complex_results = test_complex_query_optimization()
    
    # 6. Catalyst 優化器配置
    print("\n6. Catalyst 優化器配置:")
    
    # 6.1 優化器參數調優
    print("\n6.1 優化器參數調優:")
    
    def test_catalyst_optimization():
        """測試 Catalyst 優化器的不同配置"""
        
        # 保存原始配置
        original_configs = {}
        catalyst_configs = [
            "spark.sql.cbo.enabled",
            "spark.sql.cbo.joinReorder.enabled",
            "spark.sql.adaptive.enabled",
            "spark.sql.adaptive.coalescePartitions.enabled",
            "spark.sql.adaptive.skewJoin.enabled"
        ]
        
        for config in catalyst_configs:
            original_configs[config] = spark.conf.get(config, "false")
        
        # 測試不同配置組合
        config_tests = [
            ("默認配置", {}),
            ("禁用CBO", {"spark.sql.cbo.enabled": "false"}),
            ("禁用自適應", {"spark.sql.adaptive.enabled": "false"}),
            ("禁用JOIN重排序", {"spark.sql.cbo.joinReorder.enabled": "false"}),
            ("全部優化", {
                "spark.sql.cbo.enabled": "true",
                "spark.sql.cbo.joinReorder.enabled": "true",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true",
                "spark.sql.adaptive.skewJoin.enabled": "true"
            })
        ]
        
        # 測試查詢
        test_query = """
            SELECT 
                p.category,
                c.tier,
                COUNT(*) as order_count,
                AVG(o.price * o.quantity) as avg_order_value,
                SUM(o.price * o.quantity) as total_revenue
            FROM orders o
            JOIN customers c ON o.customer_id = c.customer_id
            JOIN products p ON o.product_id = p.product_id
            WHERE o.status = 'delivered'
                AND c.is_active = true
            GROUP BY p.category, c.tier
            HAVING COUNT(*) > 100
            ORDER BY total_revenue DESC
        """
        
        catalyst_results = []
        
        for config_name, configs in config_tests:
            print(f"\n測試配置: {config_name}")
            
            # 應用配置
            for key, value in configs.items():
                spark.conf.set(key, value)
                print(f"設置 {key} = {value}")
            
            # 執行查詢
            result_df = spark.sql(test_query)
            exec_time, result_count = time_query_execution(result_df, config_name)
            
            catalyst_results.append({
                "config": config_name,
                "execution_time": exec_time,
                "result_count": result_count
            })
        
        # 恢復原始配置
        for config, value in original_configs.items():
            spark.conf.set(config, value)
        
        return catalyst_results
    
    catalyst_results = test_catalyst_optimization()
    
    # 7. 統計信息和成本優化
    print("\n7. 統計信息和成本優化:")
    
    # 7.1 收集統計信息
    print("\n7.1 收集和使用統計信息:")
    
    def collect_table_statistics():
        """收集表統計信息"""
        
        tables = ["orders", "customers", "products", "promotions"]
        
        for table in tables:
            print(f"\n收集 {table} 表的統計信息:")
            
            # 收集基本統計信息
            try:
                spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS")
                print(f"✓ {table} 基本統計信息收集完成")
            except Exception as e:
                print(f"✗ {table} 基本統計信息收集失敗: {e}")
            
            # 收集列統計信息
            try:
                spark.sql(f"ANALYZE TABLE {table} COMPUTE STATISTICS FOR ALL COLUMNS")
                print(f"✓ {table} 列統計信息收集完成")
            except Exception as e:
                print(f"✗ {table} 列統計信息收集失敗: {e}")
            
            # 顯示統計信息
            try:
                stats_df = spark.sql(f"DESCRIBE EXTENDED {table}")
                print(f"{table} 表信息:")
                stats_df.filter(col("col_name").isin(["Statistics", "Table Size Bytes", "Num Files"])).show(truncate=False)
            except Exception as e:
                print(f"無法顯示 {table} 統計信息: {e}")
    
    collect_table_statistics()
    
    # 8. 查詢計劃可視化和分析
    print("\n8. 查詢計劃可視化和分析:")
    
    # 8.1 執行計劃比較
    print("\n8.1 執行計劃深度比較:")
    
    def compare_execution_plans():
        """比較不同查詢的執行計劃"""
        
        comparison_queries = [
            ("簡單JOIN", """
                SELECT c.customer_name, COUNT(o.order_id)
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                GROUP BY c.customer_name
                LIMIT 100
            """),
            ("複雜JOIN", """
                SELECT 
                    c.customer_name,
                    p.category,
                    COUNT(o.order_id) as order_count,
                    SUM(o.price * o.quantity) as total_value
                FROM customers c
                JOIN orders o ON c.customer_id = o.customer_id
                JOIN products p ON o.product_id = p.product_id
                LEFT JOIN promotions pr ON p.category = pr.category 
                    AND o.order_date BETWEEN pr.start_date AND pr.end_date
                WHERE c.tier = 'Premium'
                GROUP BY c.customer_name, p.category
                HAVING COUNT(o.order_id) > 5
                ORDER BY total_value DESC
                LIMIT 100
            """)
        ]
        
        plan_comparisons = []
        
        for query_name, query in comparison_queries:
            print(f"\n分析查詢: {query_name}")
            
            result_df = spark.sql(query)
            
            # 獲取執行計劃字符串
            plan_str = result_df._jdf.queryExecution().executedPlan().toString()
            
            # 提取計劃指標
            metrics = extract_plan_metrics(plan_str)
            
            # 執行查詢
            exec_time, result_count = time_query_execution(result_df, query_name)
            
            plan_comparisons.append({
                "query": query_name,
                "execution_time": exec_time,
                "result_count": result_count,
                "plan_metrics": metrics
            })
        
        return plan_comparisons
    
    plan_comparisons = compare_execution_plans()
    
    # 9. 性能調優建議
    print("\n9. 性能調優建議:")
    
    # 9.1 生成調優建議
    print("\n9.1 基於測試結果的調優建議:")
    
    def generate_tuning_recommendations():
        """基於測試結果生成調優建議"""
        
        recommendations = []
        
        # 基於JOIN測試結果
        best_join = min(join_results, key=lambda x: x['execution_time'])
        recommendations.append(f"推薦JOIN策略: {best_join['strategy']} (執行時間: {best_join['execution_time']:.2f}s)")
        
        # 基於聚合測試結果
        best_agg = min(aggregation_results, key=lambda x: x['execution_time'])
        recommendations.append(f"推薦聚合策略: {best_agg['strategy']} (執行時間: {best_agg['execution_time']:.2f}s)")
        
        # 基於子查詢測試結果
        best_subquery = min(subquery_results, key=lambda x: x['execution_time'])
        recommendations.append(f"推薦子查詢重寫: {best_subquery['method']} (執行時間: {best_subquery['execution_time']:.2f}s)")
        
        # 基於Catalyst配置結果
        best_catalyst = min(catalyst_results, key=lambda x: x['execution_time'])
        recommendations.append(f"推薦優化器配置: {best_catalyst['config']} (執行時間: {best_catalyst['execution_time']:.2f}s)")
        
        # 通用建議
        general_recommendations = [
            "使用廣播JOIN處理小表(<200MB)",
            "為大表收集統計信息以改善查詢計劃",
            "使用列式存儲格式(Parquet)提高I/O性能",
            "啟用自適應查詢執行(AQE)以動態優化",
            "合理設置分區數避免小文件問題",
            "使用緩存重複訪問的中間結果",
            "優化JOIN順序將小表放在前面"
        ]
        
        print("基於測試的調優建議:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec}")
        
        print("\n通用SQL優化建議:")
        for i, rec in enumerate(general_recommendations, 1):
            print(f"{i}. {rec}")
        
        return recommendations, general_recommendations
    
    specific_recs, general_recs = generate_tuning_recommendations()
    
    # 10. 性能測試總結
    print("\n10. 性能測試總結:")
    
    def create_performance_summary():
        """創建性能測試總結報告"""
        
        summary_data = []
        
        # JOIN測試總結
        for result in join_results:
            summary_data.append(("JOIN策略", result['strategy'], result['execution_time'], result['result_count']))
        
        # 聚合測試總結
        for result in aggregation_results:
            summary_data.append(("聚合策略", result['strategy'], result['execution_time'], result['result_count']))
        
        # 子查詢測試總結
        for result in subquery_results:
            summary_data.append(("子查詢策略", result['method'], result['execution_time'], result['result_count']))
        
        # 創建總結DataFrame
        summary_df = spark.createDataFrame(summary_data, ["測試類型", "策略", "執行時間", "結果數"])
        
        print("性能測試總結:")
        summary_df.show(50, truncate=False)
        
        # 最佳性能統計
        best_performers = {}
        for test_type in ["JOIN策略", "聚合策略", "子查詢策略"]:
            type_results = summary_df.filter(col("測試類型") == test_type)
            best = type_results.orderBy(col("執行時間")).first()
            if best:
                best_performers[test_type] = {
                    "strategy": best["策略"],
                    "time": best["執行時間"]
                }
        
        print("\n各類別最佳性能:")
        for test_type, info in best_performers.items():
            print(f"- {test_type}: {info['strategy']} ({info['time']:.2f}s)")
        
        return summary_df, best_performers
    
    summary_df, best_performers = create_performance_summary()
    
    # 清理資源
    spark.stop()
    print("\nSQL 和執行計劃優化練習完成！")

if __name__ == "__main__":
    main()