#!/usr/bin/env python3
"""
第5章練習2：窗口操作和時間處理
Spark Streaming 高級窗口操作和時間語義練習
"""

import random
import time
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (array_contains, avg, col, collect_list,
                                   count, current_timestamp, date_format,
                                   explode, expr, hour)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min
from pyspark.sql.functions import minute, regexp_extract, second, size, split
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, when, window
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("窗口操作和時間處理練習")
        .master("local[*]")
        .config(
            "spark.sql.streaming.checkpointLocation", "/tmp/spark-streaming-checkpoint"
        )
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=== 第5章練習2：窗口操作和時間處理 ===")

    # 1. 創建帶時間戳的數據源
    print("\n1. 創建帶時間戳的數據源:")

    # 1.1 創建模擬傳感器數據
    print("\n1.1 創建模擬IoT傳感器數據:")

    # 定義數據模式
    sensor_schema = StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("event_time", StringType(), True),
        ]
    )

    # 創建模擬數據流
    def generate_sensor_data():
        base_time = datetime.now()
        sensor_data = []

        for i in range(100):
            timestamp = base_time + timedelta(seconds=i * 5)  # 每5秒一個數據點

            for sensor_id in ["sensor_001", "sensor_002", "sensor_003"]:
                location = {
                    "sensor_001": "warehouse_A",
                    "sensor_002": "warehouse_B",
                    "sensor_003": "warehouse_C",
                }[sensor_id]

                # 模擬溫度和濕度波動
                base_temp = 20 + random.gauss(0, 2)
                base_humidity = 60 + random.gauss(0, 5)

                sensor_data.append(
                    (
                        sensor_id,
                        location,
                        round(base_temp, 2),
                        round(base_humidity, 2),
                        timestamp,
                        timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                    )
                )

        return sensor_data

    # 創建DataFrame
    sensor_data = generate_sensor_data()
    sensor_df = spark.createDataFrame(sensor_data, sensor_schema)

    print(f"生成了 {sensor_df.count()} 條傳感器數據")
    print("傳感器數據示例:")
    sensor_df.show(10, truncate=False)

    # 1.2 創建流式數據源
    print("\n1.2 創建流式數據源:")

    # 使用 rate source 創建時間戳數據
    rate_stream = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 10)
        .option("numPartitions", 3)
        .load()
    )

    # 豐富 rate 數據
    enriched_stream = (
        rate_stream.select(
            col("timestamp"),
            (col("value") % 5).alias("sensor_id"),
            (col("value") % 100 + 20).alias("temperature"),
            (col("value") % 50 + 40).alias("humidity"),
            col("value").alias("sequence_number"),
        )
        .withColumn(
            "sensor_name",
            when(col("sensor_id") == 0, "sensor_001")
            .when(col("sensor_id") == 1, "sensor_002")
            .when(col("sensor_id") == 2, "sensor_003")
            .when(col("sensor_id") == 3, "sensor_004")
            .otherwise("sensor_005"),
        )
        .withColumn(
            "location",
            when(col("sensor_id") == 0, "warehouse_A")
            .when(col("sensor_id") == 1, "warehouse_B")
            .when(col("sensor_id") == 2, "warehouse_C")
            .when(col("sensor_id") == 3, "warehouse_D")
            .otherwise("warehouse_E"),
        )
    )

    # 2. 基本時間窗口操作
    print("\n2. 基本時間窗口操作:")

    # 2.1 固定時間窗口
    print("\n2.1 固定時間窗口聚合:")

    # 10秒固定窗口
    fixed_window_aggregation = enriched_stream.groupBy(
        window(col("timestamp"), "10 seconds"), col("location")
    ).agg(
        count("*").alias("reading_count"),
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity"),
        spark_max("temperature").alias("max_temperature"),
        spark_min("temperature").alias("min_temperature"),
    )

    # 啟動固定窗口查詢
    fixed_window_query = (
        fixed_window_aggregation.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("運行固定窗口聚合...")
    time.sleep(20)
    fixed_window_query.stop()

    # 2.2 滑動時間窗口
    print("\n2.2 滑動時間窗口:")

    # 20秒窗口，每5秒滑動
    sliding_window_aggregation = (
        enriched_stream.groupBy(
            window(col("timestamp"), "20 seconds", "5 seconds"), col("sensor_name")
        )
        .agg(
            count("*").alias("reading_count"),
            avg("temperature").alias("avg_temperature"),
            spark_max("temperature").alias("max_temperature"),
            collect_list("temperature").alias("temperature_readings"),
        )
        .withColumn(
            "temperature_variance",
            expr(
                "aggregate(temperature_readings, 0.0, (acc, x) -> acc + (x - avg_temperature) * (x - avg_temperature)) / size(temperature_readings)"
            ),
        )
    )

    # 啟動滑動窗口查詢
    sliding_window_query = (
        sliding_window_aggregation.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="3 seconds")
        .start()
    )

    print("運行滑動窗口聚合...")
    time.sleep(15)
    sliding_window_query.stop()

    # 3. 複雜時間窗口模式
    print("\n3. 複雜時間窗口模式:")

    # 3.1 會話窗口模擬
    print("\n3.1 會話窗口模擬:")

    # 使用watermark處理延遲數據
    session_like_aggregation = (
        enriched_stream.withWatermark("timestamp", "30 seconds")
        .groupBy(
            window(col("timestamp"), "30 seconds", "10 seconds"),
            col("location"),
            col("sensor_name"),
        )
        .agg(
            count("*").alias("session_events"),
            avg("temperature").alias("session_avg_temp"),
            spark_max("timestamp").alias("session_end"),
            spark_min("timestamp").alias("session_start"),
        )
        .withColumn(
            "session_duration_seconds",
            expr(
                "cast((session_end.getTime() - session_start.getTime()) / 1000 as int)"
            ),
        )
    )

    # 3.2 自定義窗口邏輯
    print("\n3.2 自定義窗口邏輯:")

    # 根據溫度閾值創建事件窗口
    temperature_events = enriched_stream.withColumn(
        "temperature_alert",
        when(col("temperature") > 25, "HIGH")
        .when(col("temperature") < 18, "LOW")
        .otherwise("NORMAL"),
    ).filter(col("temperature_alert") != "NORMAL")

    # 溫度異常事件聚合
    alert_window_aggregation = temperature_events.groupBy(
        window(col("timestamp"), "1 minute"), col("temperature_alert"), col("location")
    ).agg(
        count("*").alias("alert_count"),
        avg("temperature").alias("avg_alert_temperature"),
        collect_list("sensor_name").alias("affected_sensors"),
    )

    # 啟動溫度告警窗口查詢
    alert_query = (
        alert_window_aggregation.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("運行溫度告警窗口...")
    time.sleep(25)
    alert_query.stop()

    # 4. 事件時間 vs 處理時間
    print("\n4. 事件時間 vs 處理時間:")

    # 4.1 延遲數據處理
    print("\n4.1 延遲數據處理:")

    # 模擬延遲數據
    delayed_data_stream = enriched_stream.withColumn(
        "event_timestamp",
        # 模擬一些數據延遲5-15秒到達
        when(
            col("sequence_number") % 10 == 0, expr("timestamp - interval 10 seconds")
        ).otherwise(col("timestamp")),
    )

    # 使用watermark處理延遲數據
    late_data_aggregation = (
        delayed_data_stream.withWatermark("event_timestamp", "20 seconds")
        .groupBy(window(col("event_timestamp"), "15 seconds"), col("location"))
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("avg_temperature"),
            expr("count(case when timestamp != event_timestamp then 1 end)").alias(
                "late_arrivals"
            ),
        )
    )

    # 啟動延遲數據處理查詢
    late_data_query = (
        late_data_aggregation.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("運行延遲數據處理...")
    time.sleep(20)
    late_data_query.stop()

    # 5. 多級窗口聚合
    print("\n5. 多級窗口聚合:")

    # 5.1 分層時間聚合
    print("\n5.1 分層時間聚合:")

    # 第一級：秒級聚合
    second_level_agg = (
        enriched_stream.groupBy(
            window(col("timestamp"), "10 seconds"), col("sensor_name"), col("location")
        )
        .agg(
            count("*").alias("readings_per_10sec"),
            avg("temperature").alias("avg_temp_10sec"),
            avg("humidity").alias("avg_humidity_10sec"),
        )
        .select(
            col("window").alias("time_window"),
            col("sensor_name"),
            col("location"),
            col("readings_per_10sec"),
            col("avg_temp_10sec"),
            col("avg_humidity_10sec"),
        )
    )

    # 第二級：分鐘級聚合（在實際應用中會從第一級結果再聚合）
    minute_level_agg = enriched_stream.groupBy(
        window(col("timestamp"), "1 minute"), col("location")
    ).agg(
        count("*").alias("readings_per_minute"),
        avg("temperature").alias("avg_temp_minute"),
        spark_max("temperature").alias("max_temp_minute"),
        spark_min("temperature").alias("min_temp_minute"),
        expr("stddev(temperature)").alias("temp_stddev_minute"),
    )

    # 啟動分層聚合查詢
    minute_agg_query = (
        minute_level_agg.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("運行分鐘級聚合...")
    time.sleep(25)
    minute_agg_query.stop()

    # 6. 復雜事件處理 (CEP)
    print("\n6. 復雜事件處理:")

    # 6.1 模式檢測
    print("\n6.1 溫度趨勢模式檢測:")

    # 檢測連續上升的溫度模式
    temperature_trends = (
        enriched_stream.select(
            col("timestamp"),
            col("sensor_name"),
            col("location"),
            col("temperature"),
            expr(
                "lag(temperature) over (partition by sensor_name order by timestamp)"
            ).alias("prev_temperature"),
            expr(
                "lag(temperature, 2) over (partition by sensor_name order by timestamp)"
            ).alias("prev2_temperature"),
        )
        .filter(
            col("prev_temperature").isNotNull() & col("prev2_temperature").isNotNull()
        )
        .withColumn(
            "trend_pattern",
            when(
                (col("temperature") > col("prev_temperature"))
                & (col("prev_temperature") > col("prev2_temperature")),
                "RISING",
            )
            .when(
                (col("temperature") < col("prev_temperature"))
                & (col("prev_temperature") < col("prev2_temperature")),
                "FALLING",
            )
            .otherwise("STABLE"),
        )
    )

    # 6.2 異常模式聚合
    pattern_aggregation = (
        temperature_trends.filter(col("trend_pattern") != "STABLE")
        .groupBy(
            window(col("timestamp"), "30 seconds"),
            col("location"),
            col("trend_pattern"),
        )
        .agg(
            count("*").alias("pattern_count"),
            collect_list("sensor_name").alias("affected_sensors"),
            avg("temperature").alias("avg_temperature_in_pattern"),
        )
    )

    # 啟動模式檢測查詢
    pattern_query = (
        pattern_aggregation.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="8 seconds")
        .start()
    )

    print("運行模式檢測...")
    time.sleep(20)
    pattern_query.stop()

    # 7. 狀態管理和累積計算
    print("\n7. 狀態管理和累積計算:")

    # 7.1 累積溫度統計
    print("\n7.1 累積溫度統計:")

    cumulative_stats = enriched_stream.groupBy("sensor_name", "location").agg(
        count("*").alias("total_readings"),
        avg("temperature").alias("overall_avg_temperature"),
        spark_max("temperature").alias("max_temperature_ever"),
        spark_min("temperature").alias("min_temperature_ever"),
        expr("approx_percentile(temperature, 0.5)").alias("median_temperature"),
        expr("stddev(temperature)").alias("temperature_stddev"),
    )

    # 7.2 運行平均計算
    running_averages = (
        enriched_stream.groupBy(
            window(col("timestamp"), "20 seconds", "5 seconds"), col("sensor_name")
        )
        .agg(
            avg("temperature").alias("window_avg_temperature"),
            count("*").alias("window_reading_count"),
        )
        .withColumn(
            "temperature_category",
            when(col("window_avg_temperature") > 24, "HOT")
            .when(col("window_avg_temperature") < 19, "COLD")
            .otherwise("NORMAL"),
        )
    )

    # 啟動運行平均查詢
    running_avg_query = (
        running_averages.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="6 seconds")
        .start()
    )

    print("運行動態平均計算...")
    time.sleep(18)
    running_avg_query.stop()

    # 8. 時間域連接 (Temporal Joins)
    print("\n8. 時間域連接:")

    # 8.1 創建傳感器配置流
    print("\n8.1 傳感器配置流連接:")

    # 模擬傳感器配置更新
    sensor_config_data = [
        ("sensor_001", "warehouse_A", "25.0", "critical", "2024-01-01 00:00:00"),
        ("sensor_002", "warehouse_B", "23.0", "normal", "2024-01-01 00:00:00"),
        ("sensor_003", "warehouse_C", "24.0", "critical", "2024-01-01 00:00:00"),
        ("sensor_004", "warehouse_D", "22.0", "normal", "2024-01-01 00:00:00"),
        ("sensor_005", "warehouse_E", "26.0", "critical", "2024-01-01 00:00:00"),
    ]

    config_schema = StructType(
        [
            StructField("sensor_id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("threshold_temp", StringType(), True),
            StructField("priority", StringType(), True),
            StructField("config_timestamp", StringType(), True),
        ]
    )

    sensor_config_df = (
        spark.createDataFrame(sensor_config_data, config_schema)
        .withColumn("threshold_temp", col("threshold_temp").cast(DoubleType()))
        .withColumn("config_timestamp", to_timestamp(col("config_timestamp")))
    )

    # 廣播配置數據進行連接
    enriched_with_config = enriched_stream.join(
        broadcast(sensor_config_df.select("sensor_id", "threshold_temp", "priority")),
        enriched_stream.sensor_name == sensor_config_df.sensor_id,
        "left",
    ).withColumn(
        "temperature_status",
        when(col("temperature") > col("threshold_temp"), "ABOVE_THRESHOLD").otherwise(
            "NORMAL"
        ),
    )

    # 基於配置的告警聚合
    config_based_alerts = (
        enriched_with_config.filter(col("temperature_status") == "ABOVE_THRESHOLD")
        .groupBy(
            window(col("timestamp"), "45 seconds"), col("location"), col("priority")
        )
        .agg(
            count("*").alias("alert_count"),
            avg("temperature").alias("avg_alert_temperature"),
            collect_list("sensor_name").alias("alerting_sensors"),
        )
    )

    # 啟動配置基礎告警查詢
    config_alert_query = (
        config_based_alerts.writeStream.outputMode("update")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("運行配置基礎告警...")
    time.sleep(25)
    config_alert_query.stop()

    # 9. 高級時間處理技術
    print("\n9. 高級時間處理技術:")

    # 9.1 自適應窗口大小
    print("\n9.1 自適應窗口處理:")

    # 根據數據頻率調整窗口大小
    adaptive_windows = (
        enriched_stream.groupBy(window(col("timestamp"), "15 seconds"), col("location"))
        .agg(
            count("*").alias("event_count"), avg("temperature").alias("avg_temperature")
        )
        .withColumn(
            "data_density",
            when(col("event_count") > 20, "HIGH")
            .when(col("event_count") > 10, "MEDIUM")
            .otherwise("LOW"),
        )
        .withColumn(
            "recommended_window_size",
            when(col("data_density") == "HIGH", "5 seconds")
            .when(col("data_density") == "MEDIUM", "10 seconds")
            .otherwise("30 seconds"),
        )
    )

    # 9.2 多時間尺度分析
    multi_scale_analysis = (
        enriched_stream.select(
            col("timestamp"),
            col("sensor_name"),
            col("location"),
            col("temperature"),
            # 短期窗口（10秒）
            avg("temperature")
            .over(
                Window.partitionBy("sensor_name")
                .orderBy(col("timestamp").cast("long"))
                .rangeBetween(-10, 0)
            )
            .alias("temp_10sec_avg"),
            # 中期窗口（60秒）
            avg("temperature")
            .over(
                Window.partitionBy("sensor_name")
                .orderBy(col("timestamp").cast("long"))
                .rangeBetween(-60, 0)
            )
            .alias("temp_60sec_avg"),
            # 長期窗口（300秒）
            avg("temperature")
            .over(
                Window.partitionBy("sensor_name")
                .orderBy(col("timestamp").cast("long"))
                .rangeBetween(-300, 0)
            )
            .alias("temp_300sec_avg"),
        )
        .withColumn("short_term_anomaly", expr("abs(temperature - temp_10sec_avg) > 2"))
        .withColumn(
            "long_term_trend",
            when(col("temp_60sec_avg") > col("temp_300sec_avg"), "WARMING")
            .when(col("temp_60sec_avg") < col("temp_300sec_avg"), "COOLING")
            .otherwise("STABLE"),
        )
    )

    # 啟動多尺度分析查詢
    multi_scale_query = (
        multi_scale_analysis.writeStream.outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="8 seconds")
        .start()
    )

    print("運行多時間尺度分析...")
    time.sleep(20)
    multi_scale_query.stop()

    # 10. 性能優化和監控
    print("\n10. 流處理性能優化:")

    # 10.1 批處理間隔優化
    optimized_aggregation = (
        enriched_stream.coalesce(2)
        .groupBy(window(col("timestamp"), "30 seconds"), col("location"))
        .agg(
            count("*").alias("event_count"),
            avg("temperature").alias("avg_temperature"),
            expr("approx_count_distinct(sensor_name)").alias("active_sensors"),
        )
    )

    # 10.2 檢查點和容錯
    checkpoint_query = (
        optimized_aggregation.writeStream.outputMode("update")
        .format("console")
        .option("checkpointLocation", "/tmp/spark-streaming-checkpoint-optimized")
        .trigger(processingTime="5 seconds")
        .start()
    )

    print("運行優化聚合...")
    time.sleep(15)
    checkpoint_query.stop()

    # 總結報告
    print("\n=== 窗口操作和時間處理總結 ===")

    techniques_covered = [
        "固定時間窗口聚合",
        "滑動時間窗口處理",
        "Watermark延遲數據處理",
        "複雜事件處理(CEP)",
        "多級時間聚合",
        "狀態管理和累積計算",
        "時間域連接操作",
        "自適應窗口技術",
        "多時間尺度分析",
        "性能優化策略",
    ]

    print("涵蓋的時間處理技術:")
    for i, technique in enumerate(techniques_covered, 1):
        print(f"{i}. {technique}")

    best_practices = [
        "合理設置Watermark處理延遲數據",
        "根據業務需求選擇適當的窗口大小",
        "使用廣播變量優化流表連接",
        "設置合適的觸發間隔避免小檔案",
        "監控流處理延遲和吞吐量",
        "使用checkpoint確保容錯恢復",
    ]

    print("\n最佳實踐建議:")
    for i, practice in enumerate(best_practices, 1):
        print(f"{i}. {practice}")

    # 清理資源
    spark.stop()
    print("\n窗口操作和時間處理練習完成！")


if __name__ == "__main__":
    main()
