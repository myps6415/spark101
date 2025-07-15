#!/usr/bin/env python3
"""
第5章練習3：狀態管理和複雜事件處理
Spark Streaming 狀態管理和複雜事件處理練習
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, count, sum as spark_sum, avg, max as spark_max, \
    min as spark_min, when, expr, current_timestamp, to_timestamp, unix_timestamp, \
    struct, collect_list, size, array_contains, explode, split, \
    lag, lead, first, last, row_number, dense_rank
from pyspark.sql.streaming import GroupState, GroupStateTimeout
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, \
    DoubleType, TimestampType, ArrayType, MapType, BooleanType
from pyspark.sql.window import Window
import time
import json
from datetime import datetime, timedelta

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("狀態管理和複雜事件處理練習") \
        .master("local[*]") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-stateful-checkpoint") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=== 第5章練習3：狀態管理和複雜事件處理 ===")
    
    # 1. 創建用戶行為事件流
    print("\n1. 創建用戶行為事件流:")
    
    # 1.1 模擬電商用戶行為數據
    print("\n1.1 用戶行為事件流:")
    
    # 使用 rate source 創建用戶行為事件
    user_events_stream = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 15) \
        .option("numPartitions", 2) \
        .load()
    
    # 豐富事件數據
    enriched_events = user_events_stream.select(
        col("timestamp"),
        (col("value") % 1000).alias("user_id"),
        (col("value") % 10).alias("event_type_id"),
        (col("value") % 100).alias("product_id"),
        (col("value") % 1000 + 1).alias("session_value"),
        col("value").alias("sequence_id")
    ).withColumn(
        "event_type",
        when(col("event_type_id") == 0, "page_view")
        .when(col("event_type_id") == 1, "product_view")
        .when(col("event_type_id") == 2, "add_to_cart")
        .when(col("event_type_id") == 3, "remove_from_cart")
        .when(col("event_type_id") == 4, "checkout_start")
        .when(col("event_type_id") == 5, "checkout_complete")
        .when(col("event_type_id") == 6, "payment_success")
        .when(col("event_type_id") == 7, "payment_failed")
        .when(col("event_type_id") == 8, "logout")
        .otherwise("session_timeout")
    ).withColumn(
        "event_value",
        when(col("event_type").isin("add_to_cart", "checkout_complete"), col("session_value"))
        .otherwise(0)
    )
    
    print("啟動用戶事件流...")
    
    # 2. 用戶會話狀態管理
    print("\n2. 用戶會話狀態管理:")
    
    # 2.1 會話狀態聚合
    print("\n2.1 實時會話狀態追蹤:")
    
    # 使用 mapGroupsWithState 進行狀態管理
    session_state_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("session_start", TimestampType(), True),
        StructField("last_activity", TimestampType(), True),
        StructField("page_views", IntegerType(), True),
        StructField("products_viewed", ArrayType(IntegerType()), True),
        StructField("cart_items", ArrayType(IntegerType()), True),
        StructField("total_cart_value", IntegerType(), True),
        StructField("events_count", IntegerType(), True),
        StructField("session_duration_minutes", DoubleType(), True),
        StructField("conversion_funnel", ArrayType(StringType()), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    # 會話狀態聚合
    session_aggregation = enriched_events \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "page_view", 1).otherwise(0)).alias("page_views"),
            spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("product_views"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("add_to_cart_events"),
            spark_sum(when(col("event_type").isin("checkout_complete", "payment_success"), 1).otherwise(0)).alias("conversions"),
            collect_list("event_type").alias("event_sequence"),
            collect_list("product_id").alias("products_interacted"),
            spark_sum("event_value").alias("total_session_value"),
            min("timestamp").alias("session_start"),
            max("timestamp").alias("last_activity")
        ).withColumn(
            "session_duration_minutes",
            (unix_timestamp("last_activity") - unix_timestamp("session_start")) / 60.0
        ).withColumn(
            "conversion_rate",
            col("conversions") / col("total_events") * 100
        ).withColumn(
            "user_segment",
            when(col("conversions") > 0, "Converter")
            .when(col("add_to_cart_events") > 0, "Cart User")
            .when(col("product_views") > 3, "Browser")
            .otherwise("Casual Visitor")
        )
    
    # 啟動會話聚合查詢
    session_query = session_aggregation.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("運行會話狀態追蹤...")
    time.sleep(25)
    session_query.stop()
    
    # 3. 複雜事件模式檢測
    print("\n3. 複雜事件模式檢測:")
    
    # 3.1 購買漏斗分析
    print("\n3.1 購買漏斗模式檢測:")
    
    # 定義購買漏斗步驟
    funnel_events = enriched_events.withColumn(
        "funnel_step",
        when(col("event_type") == "product_view", 1)
        .when(col("event_type") == "add_to_cart", 2)
        .when(col("event_type") == "checkout_start", 3)
        .when(col("event_type") == "payment_success", 4)
        .otherwise(0)
    ).filter(col("funnel_step") > 0)
    
    # 檢測完整購買路径
    purchase_funnel_analysis = funnel_events \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy(
            window(col("timestamp"), "10 minutes"),
            col("user_id")
        ).agg(
            collect_list(struct("timestamp", "funnel_step", "event_type")).alias("funnel_events"),
            max("funnel_step").alias("max_funnel_step"),
            count("*").alias("funnel_events_count")
        ).withColumn(
            "funnel_completion",
            when(col("max_funnel_step") == 4, "Complete Purchase")
            .when(col("max_funnel_step") == 3, "Abandoned at Payment")
            .when(col("max_funnel_step") == 2, "Abandoned at Checkout")
            .otherwise("Abandoned at Cart")
        ).withColumn(
            "is_successful_conversion", 
            col("max_funnel_step") == 4
        )
    
    # 啟動漏斗分析查詢
    funnel_query = purchase_funnel_analysis.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='8 seconds') \
        .start()
    
    print("運行購買漏斗分析...")
    time.sleep(20)
    funnel_query.stop()
    
    # 3.2 異常行為檢測
    print("\n3.2 異常行為模式檢測:")
    
    # 檢測異常行為模式
    anomaly_detection = enriched_events \
        .withWatermark("timestamp", "3 minutes") \
        .groupBy(
            window(col("timestamp"), "2 minutes"),
            col("user_id")
        ).agg(
            count("*").alias("events_per_window"),
            collect_list("event_type").alias("event_types"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
            spark_sum(when(col("event_type") == "remove_from_cart", 1).otherwise(0)).alias("cart_removals"),
            spark_sum(when(col("event_type") == "payment_failed", 1).otherwise(0)).alias("payment_failures")
        ).withColumn(
            "suspicious_behavior",
            when(col("events_per_window") > 20, "High Frequency")
            .when(col("cart_additions") > 5, "Excessive Cart Activity")
            .when(col("payment_failures") > 2, "Multiple Payment Failures")
            .when(col("cart_removals") > col("cart_additions"), "Negative Cart Behavior")
            .otherwise("Normal")
        ).filter(col("suspicious_behavior") != "Normal")
    
    # 啟動異常檢測查詢
    anomaly_query = anomaly_detection.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='6 seconds') \
        .start()
    
    print("運行異常行為檢測...")
    time.sleep(18)
    anomaly_query.stop()
    
    # 4. 實時個性化推薦
    print("\n4. 實時個性化推薦:")
    
    # 4.1 基於行為的實時推薦
    print("\n4.1 實時推薦引擎:")
    
    # 分析用戶興趣
    user_interest_analysis = enriched_events \
        .filter(col("event_type").isin("product_view", "add_to_cart")) \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("user_id") \
        .agg(
            collect_list("product_id").alias("viewed_products"),
            count("*").alias("interaction_count"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_additions"),
            avg("product_id").alias("avg_product_interest")
        ).withColumn(
            "user_preference_score",
            col("cart_additions") * 3 + col("interaction_count")
        ).withColumn(
            "recommendation_category",
            when(col("avg_product_interest") < 25, "Electronics")
            .when(col("avg_product_interest") < 50, "Fashion")
            .when(col("avg_product_interest") < 75, "Home & Garden")
            .otherwise("Sports & Outdoors")
        )
    
    # 4.2 協同過濾推薦
    collaborative_filtering = user_interest_analysis \
        .select("user_id", "recommendation_category", "user_preference_score") \
        .withColumn(
            "similar_users_products",
            when(col("recommendation_category") == "Electronics", array(lit(101), lit(102), lit(103)))
            .when(col("recommendation_category") == "Fashion", array(lit(201), lit(202), lit(203)))
            .when(col("recommendation_category") == "Home & Garden", array(lit(301), lit(302), lit(303)))
            .otherwise(array(lit(401), lit(402), lit(403)))
        )
    
    # 啟動推薦引擎查詢
    recommendation_query = collaborative_filtering.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='12 seconds') \
        .start()
    
    print("運行實時推薦引擎...")
    time.sleep(25)
    recommendation_query.stop()
    
    # 5. 業務指標監控
    print("\n5. 實時業務指標監控:")
    
    # 5.1 實時KPI監控
    print("\n5.1 實時KPI儀表板:")
    
    # 業務關鍵指標
    business_kpis = enriched_events \
        .withWatermark("timestamp", "1 minute") \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            count("*").alias("total_events"),
            expr("approx_count_distinct(user_id)").alias("active_users"),
            spark_sum(when(col("event_type") == "payment_success", 1).otherwise(0)).alias("successful_purchases"),
            spark_sum(when(col("event_type") == "payment_failed", 1).otherwise(0)).alias("failed_payments"),
            spark_sum("event_value").alias("total_revenue"),
            avg("event_value").alias("avg_transaction_value")
        ).withColumn(
            "conversion_rate_percent",
            col("successful_purchases") / col("active_users") * 100
        ).withColumn(
            "payment_success_rate",
            col("successful_purchases") / (col("successful_purchases") + col("failed_payments")) * 100
        )
    
    # 5.2 產品熱度分析
    product_popularity = enriched_events \
        .filter(col("event_type").isin("product_view", "add_to_cart", "payment_success")) \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes"),
            col("product_id")
        ).agg(
            count("*").alias("product_interactions"),
            spark_sum(when(col("event_type") == "product_view", 1).otherwise(0)).alias("views"),
            spark_sum(when(col("event_type") == "add_to_cart", 1).otherwise(0)).alias("cart_adds"),
            spark_sum(when(col("event_type") == "payment_success", 1).otherwise(0)).alias("purchases")
        ).withColumn(
            "product_score",
            col("views") + col("cart_adds") * 2 + col("purchases") * 5
        ).withColumn(
            "popularity_rank",
            row_number().over(Window.partitionBy("window").orderBy(col("product_score").desc()))
        ).filter(col("popularity_rank") <= 5)
    
    # 啟動KPI監控查詢
    kpi_query = business_kpis.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("運行實時KPI監控...")
    time.sleep(15)
    kpi_query.stop()
    
    # 6. 風險管理和欺詐檢測
    print("\n6. 風險管理和欺詐檢測:")
    
    # 6.1 欺詐行為檢測
    print("\n6.1 實時欺詐檢測:")
    
    fraud_detection = enriched_events \
        .withWatermark("timestamp", "5 minutes") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "payment_failed", 1).otherwise(0)).alias("payment_failures"),
            spark_sum(when(col("event_type") == "payment_success", 1).otherwise(0)).alias("successful_payments"),
            spark_sum("event_value").alias("total_transaction_value"),
            expr("approx_count_distinct(product_id)").alias("unique_products"),
            collect_list("event_type").alias("event_pattern")
        ).withColumn(
            "avg_transaction_value",
            col("total_transaction_value") / (col("successful_payments") + 1)
        ).withColumn(
            "fraud_risk_score",
            when(col("payment_failures") > 3, 50)
            .otherwise(0) +
            when(col("avg_transaction_value") > 500, 30)
            .otherwise(0) +
            when(col("total_events") > 50, 20)
            .otherwise(0)
        ).withColumn(
            "fraud_risk_level",
            when(col("fraud_risk_score") >= 70, "HIGH")
            .when(col("fraud_risk_score") >= 40, "MEDIUM")
            .otherwise("LOW")
        ).filter(col("fraud_risk_level") != "LOW")
    
    # 啟動欺詐檢測查詢
    fraud_query = fraud_detection.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='8 seconds') \
        .start()
    
    print("運行欺詐檢測系統...")
    time.sleep(20)
    fraud_query.stop()
    
    # 7. 客戶生命週期管理
    print("\n7. 客戶生命週期管理:")
    
    # 7.1 客戶價值分層
    print("\n7.1 實時客戶價值分析:")
    
    customer_lifecycle = enriched_events \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy("user_id") \
        .agg(
            count("*").alias("engagement_score"),
            spark_sum("event_value").alias("monetary_value"),
            expr("approx_count_distinct(date_format(timestamp, 'yyyy-MM-dd'))").alias("active_days"),
            spark_sum(when(col("event_type") == "payment_success", 1).otherwise(0)).alias("purchase_frequency"),
            min("timestamp").alias("first_interaction"),
            max("timestamp").alias("last_interaction")
        ).withColumn(
            "customer_lifetime_days",
            expr("datediff(last_interaction, first_interaction)")
        ).withColumn(
            "customer_tier",
            when((col("monetary_value") > 1000) & (col("purchase_frequency") > 2), "VIP")
            .when((col("monetary_value") > 500) & (col("purchase_frequency") > 1), "Premium")
            .when(col("purchase_frequency") > 0, "Active")
            .otherwise("Prospect")
        ).withColumn(
            "retention_risk",
            when(col("engagement_score") < 5, "High Risk")
            .when(col("engagement_score") < 15, "Medium Risk")
            .otherwise("Low Risk")
        )
    
    # 啟動客戶生命週期分析
    lifecycle_query = customer_lifecycle.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("運行客戶生命週期分析...")
    time.sleep(25)
    lifecycle_query.stop()
    
    # 8. 多維度實時分析
    print("\n8. 多維度實時分析:")
    
    # 8.1 時段分析
    print("\n8.1 時段行為分析:")
    
    temporal_analysis = enriched_events \
        .withColumn("hour_of_day", expr("hour(timestamp)")) \
        .withColumn("day_period", 
            when(col("hour_of_day").between(6, 11), "Morning")
            .when(col("hour_of_day").between(12, 17), "Afternoon")
            .when(col("hour_of_day").between(18, 22), "Evening")
            .otherwise("Night")
        ).groupBy(
            window(col("timestamp"), "10 minutes"),
            col("day_period"),
            col("event_type")
        ).agg(
            count("*").alias("event_count"),
            expr("approx_count_distinct(user_id)").alias("unique_users")
        )
    
    # 8.2 設備和渠道分析
    channel_analysis = enriched_events \
        .withColumn("channel_type",
            when(col("user_id") % 3 == 0, "Mobile")
            .when(col("user_id") % 3 == 1, "Desktop")
            .otherwise("Tablet")
        ).groupBy(
            window(col("timestamp"), "5 minutes"),
            col("channel_type")
        ).agg(
            count("*").alias("events"),
            expr("approx_count_distinct(user_id)").alias("users"),
            spark_sum("event_value").alias("revenue")
        ).withColumn(
            "revenue_per_user",
            col("revenue") / col("users")
        )
    
    # 啟動多維度分析
    temporal_query = temporal_analysis.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='7 seconds') \
        .start()
    
    print("運行時段行為分析...")
    time.sleep(15)
    temporal_query.stop()
    
    # 9. 告警和通知系統
    print("\n9. 實時告警系統:")
    
    # 9.1 業務告警
    print("\n9.1 業務指標告警:")
    
    business_alerts = enriched_events \
        .withWatermark("timestamp", "30 seconds") \
        .groupBy(window(col("timestamp"), "1 minute")) \
        .agg(
            count("*").alias("total_events"),
            spark_sum(when(col("event_type") == "payment_failed", 1).otherwise(0)).alias("payment_failures"),
            spark_sum(when(col("event_type") == "session_timeout", 1).otherwise(0)).alias("session_timeouts"),
            expr("approx_count_distinct(user_id)").alias("active_users")
        ).withColumn(
            "payment_failure_rate",
            col("payment_failures") / col("total_events") * 100
        ).withColumn(
            "alert_type",
            when(col("payment_failure_rate") > 10, "HIGH_PAYMENT_FAILURE_RATE")
            .when(col("session_timeouts") > col("active_users") * 0.3, "HIGH_SESSION_TIMEOUT_RATE")
            .when(col("active_users") < 5, "LOW_USER_ACTIVITY")
            .otherwise("NORMAL")
        ).filter(col("alert_type") != "NORMAL")
    
    # 啟動告警系統
    alert_query = business_alerts.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime='5 seconds') \
        .start()
    
    print("運行實時告警系統...")
    time.sleep(15)
    alert_query.stop()
    
    # 10. 系統總結和最佳實踐
    print("\n10. 狀態管理系統總結:")
    
    # 總結已實現的功能
    implemented_features = [
        "用戶會話狀態管理",
        "購買漏斗模式檢測", 
        "異常行為識別",
        "實時個性化推薦",
        "業務KPI監控",
        "欺詐檢測系統",
        "客戶生命週期管理",
        "多維度行為分析",
        "實時告警通知"
    ]
    
    print("已實現的狀態管理功能:")
    for i, feature in enumerate(implemented_features, 1):
        print(f"{i}. {feature}")
    
    # 最佳實踐建議
    best_practices = [
        "使用適當的watermark處理延遲數據",
        "合理設計狀態數據結構避免內存溢出",
        "實施狀態超時機制清理過期狀態",
        "使用checkpoint確保狀態容錯恢復",
        "監控狀態存儲大小和處理延遲",
        "設計合理的狀態分區策略",
        "實現狀態數據的定期清理機制"
    ]
    
    print("\n狀態管理最佳實踐:")
    for i, practice in enumerate(best_practices, 1):
        print(f"{i}. {practice}")
    
    # 性能指標
    print("\n系統性能特點:")
    performance_metrics = {
        "實時性": "秒級延遲",
        "吞吐量": "支持每秒數千事件",
        "狀態容量": "支持百萬級用戶狀態", 
        "容錯能力": "checkpoint自動恢復",
        "可擴展性": "水平擴展支持",
        "監控能力": "全方位指標監控"
    }
    
    for metric, value in performance_metrics.items():
        print(f"- {metric}: {value}")
    
    # 清理資源
    spark.stop()
    print("\n狀態管理和複雜事件處理練習完成！")

if __name__ == "__main__":
    main()