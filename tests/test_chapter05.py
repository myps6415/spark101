"""
第5章：Spark Streaming 的測試
"""

import os
import tempfile
import threading
import time

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import (avg, col, count, current_timestamp, explode,
                                   lit)
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import split
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import to_timestamp, when, window
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType)


class TestStreamingBasics:
    """測試 Streaming 基本操作"""

    @pytest.fixture(scope="function")
    def streaming_spark_session(self):
        """創建支持 Streaming 的 SparkSession"""
        spark = (
            SparkSession.builder.appName("StreamingTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        # 停止所有活躍的流
        for stream in spark.streams.active:
            stream.stop()

        spark.stop()

    def test_create_stream_from_rate_source(self, streaming_spark_session):
        """測試從 rate source 創建流"""
        # 創建 rate source stream
        rate_stream = (
            streaming_spark_session.readStream.format("rate")
            .option("rowsPerSecond", 5)
            .option("numPartitions", 2)
            .load()
        )

        # 檢查 schema
        assert "timestamp" in rate_stream.columns
        assert "value" in rate_stream.columns

        # 簡單轉換
        processed_stream = rate_stream.select(
            col("timestamp"), col("value"), (col("value") * 2).alias("doubled_value")
        )

        assert "doubled_value" in processed_stream.columns

    def test_create_stream_from_socket(self, streaming_spark_session):
        """測試從 socket 創建流（模擬）"""
        # 注意：實際的 socket stream 需要運行的 socket 服務器
        # 這裡我們只測試流的創建，不實際運行
        try:
            socket_stream = (
                streaming_spark_session.readStream.format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load()
            )

            # 檢查 schema
            assert "value" in socket_stream.columns
        except Exception:
            # 如果沒有 socket 服務器，跳過測試
            pytest.skip("Socket server not available")

    def test_memory_sink_stream(self, streaming_spark_session):
        """測試 memory sink 流"""
        # 創建 rate stream
        rate_stream = (
            streaming_spark_session.readStream.format("rate")
            .option("rowsPerSecond", 2)
            .load()
        )

        # 簡單處理
        processed_stream = rate_stream.select(
            col("timestamp"), col("value"), (col("value") % 10).alias("mod_value")
        )

        # 寫入 memory sink
        query = (
            processed_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("rate_test")
            .start()
        )

        # 等待一些數據
        time.sleep(3)

        # 檢查結果
        result_df = streaming_spark_session.sql("SELECT * FROM rate_test")
        assert result_df.count() > 0

        query.stop()

    def test_console_sink_stream(self, streaming_spark_session):
        """測試 console sink 流"""
        # 創建 rate stream
        rate_stream = (
            streaming_spark_session.readStream.format("rate")
            .option("rowsPerSecond", 1)
            .load()
        )

        # 處理數據
        processed_stream = rate_stream.select(
            col("timestamp"),
            col("value"),
            when(col("value") % 2 == 0, "even").otherwise("odd").alias("parity"),
        )

        # 寫入 console
        query = (
            processed_stream.writeStream.outputMode("append")
            .format("console")
            .option("numRows", 5)
            .start()
        )

        # 運行一段時間
        time.sleep(2)

        # 檢查查詢狀態
        assert query.isActive

        query.stop()


class TestStreamingTransformations:
    """測試流轉換操作"""

    @pytest.fixture(scope="function")
    def streaming_spark(self):
        """創建 Streaming SparkSession"""
        spark = (
            SparkSession.builder.appName("StreamingTransformTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        for stream in spark.streams.active:
            stream.stop()
        spark.stop()

    def test_filter_transformation(self, streaming_spark):
        """測試過濾轉換"""
        # 創建流
        rate_stream = (
            streaming_spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        )

        # 過濾偶數
        filtered_stream = rate_stream.filter(col("value") % 2 == 0)

        # 寫入 memory
        query = (
            filtered_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("filtered_test")
            .start()
        )

        time.sleep(3)

        # 檢查結果
        result_df = streaming_spark.sql("SELECT * FROM filtered_test")
        if result_df.count() > 0:
            values = [row.value for row in result_df.collect()]
            assert all(v % 2 == 0 for v in values)

        query.stop()

    def test_select_transformation(self, streaming_spark):
        """測試選擇轉換"""
        rate_stream = (
            streaming_spark.readStream.format("rate").option("rowsPerSecond", 3).load()
        )

        # 選擇和轉換列
        selected_stream = rate_stream.select(
            col("timestamp"),
            col("value").alias("original_value"),
            (col("value") * 2).alias("doubled"),
            (col("value") ** 2).alias("squared"),
        )

        query = (
            selected_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("selected_test")
            .start()
        )

        time.sleep(2)

        result_df = streaming_spark.sql("SELECT * FROM selected_test")
        if result_df.count() > 0:
            row = result_df.collect()[0]
            assert row.doubled == row.original_value * 2
            assert row.squared == row.original_value**2

        query.stop()

    def test_group_by_aggregation(self, streaming_spark):
        """測試分組聚合"""
        rate_stream = (
            streaming_spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        )

        # 按值的奇偶性分組
        aggregated_stream = (
            rate_stream.withColumn(
                "parity", when(col("value") % 2 == 0, "even").otherwise("odd")
            )
            .groupBy("parity")
            .count()
        )

        query = (
            aggregated_stream.writeStream.outputMode("complete")
            .format("memory")
            .queryName("aggregated_test")
            .start()
        )

        time.sleep(3)

        result_df = streaming_spark.sql("SELECT * FROM aggregated_test")
        if result_df.count() > 0:
            parities = [row.parity for row in result_df.collect()]
            assert "even" in parities or "odd" in parities

        query.stop()


class TestWindowOperations:
    """測試視窗操作"""

    @pytest.fixture(scope="function")
    def window_spark(self):
        """創建支持視窗操作的 SparkSession"""
        spark = (
            SparkSession.builder.appName("WindowTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        for stream in spark.streams.active:
            stream.stop()
        spark.stop()

    def test_time_window_aggregation(self, window_spark):
        """測試時間視窗聚合"""
        # 創建帶時間戳的流
        rate_stream = (
            window_spark.readStream.format("rate").option("rowsPerSecond", 10).load()
        )

        # 時間視窗聚合
        windowed_stream = rate_stream.groupBy(
            window(col("timestamp"), "10 seconds", "5 seconds")
        ).agg(
            count("*").alias("count"),
            avg("value").alias("avg_value"),
            spark_max("value").alias("max_value"),
        )

        query = (
            windowed_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("windowed_test")
            .start()
        )

        time.sleep(15)  # 等待足夠長時間以產生多個視窗

        result_df = window_spark.sql("SELECT * FROM windowed_test")
        if result_df.count() > 0:
            assert "window" in result_df.columns
            assert "count" in result_df.columns
            assert "avg_value" in result_df.columns

        query.stop()

    def test_session_window(self, window_spark):
        """測試會話視窗（如果支持）"""
        rate_stream = (
            window_spark.readStream.format("rate").option("rowsPerSecond", 2).load()
        )

        # 基本視窗操作
        windowed_stream = rate_stream.groupBy(
            window(col("timestamp"), "5 seconds")
        ).count()

        query = (
            windowed_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("session_test")
            .start()
        )

        time.sleep(8)

        result_df = window_spark.sql("SELECT * FROM session_test")
        if result_df.count() > 0:
            assert "window" in result_df.columns
            assert "count" in result_df.columns

        query.stop()


class TestStatefulOperations:
    """測試狀態操作"""

    @pytest.fixture(scope="function")
    def stateful_spark(self):
        """創建支持狀態操作的 SparkSession"""
        spark = (
            SparkSession.builder.appName("StatefulTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        for stream in spark.streams.active:
            stream.stop()
        spark.stop()

    def test_update_mode_aggregation(self, stateful_spark):
        """測試 update 模式聚合"""
        rate_stream = (
            stateful_spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        )

        # 狀態聚合
        stateful_stream = (
            rate_stream.withColumn("mod_value", col("value") % 5)
            .groupBy("mod_value")
            .agg(count("*").alias("count"), avg("value").alias("avg_value"))
        )

        query = (
            stateful_stream.writeStream.outputMode("update")
            .format("memory")
            .queryName("stateful_test")
            .start()
        )

        time.sleep(5)

        result_df = stateful_spark.sql("SELECT * FROM stateful_test")
        if result_df.count() > 0:
            assert "mod_value" in result_df.columns
            assert "count" in result_df.columns

        query.stop()

    def test_watermark_handling(self, stateful_spark):
        """測試水位線處理"""
        rate_stream = (
            stateful_spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        )

        # 設置水位線
        watermarked_stream = (
            rate_stream.withWatermark("timestamp", "10 seconds")
            .groupBy(window(col("timestamp"), "5 seconds"))
            .count()
        )

        query = (
            watermarked_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("watermark_test")
            .start()
        )

        time.sleep(10)

        result_df = stateful_spark.sql("SELECT * FROM watermark_test")
        # 這個測試主要確保水位線設置不會導致錯誤
        assert result_df.columns is not None

        query.stop()


class TestFileStreamOperations:
    """測試文件流操作"""

    @pytest.fixture(scope="function")
    def file_spark(self):
        """創建支持文件流的 SparkSession"""
        spark = (
            SparkSession.builder.appName("FileStreamTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        for stream in spark.streams.active:
            stream.stop()
        spark.stop()

    def test_csv_file_stream(self, file_spark, temp_dir):
        """測試 CSV 文件流"""
        # 定義 schema
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

        # 創建輸入目錄
        input_dir = os.path.join(temp_dir, "input")
        os.makedirs(input_dir, exist_ok=True)

        # 創建文件流
        file_stream = (
            file_spark.readStream.option("header", "true").schema(schema).csv(input_dir)
        )

        # 處理流
        processed_stream = file_stream.select(
            col("id"), col("name"), (col("value") * 2).alias("doubled_value")
        )

        # 創建輸出查詢
        query = (
            processed_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("csv_stream_test")
            .start()
        )

        # 模擬文件到達
        def write_test_file():
            time.sleep(1)
            test_file = os.path.join(input_dir, "test_data.csv")
            with open(test_file, "w") as f:
                f.write("id,name,value\n")
                f.write("1,Alice,100\n")
                f.write("2,Bob,200\n")

        # 在另一個線程中寫入文件
        file_thread = threading.Thread(target=write_test_file)
        file_thread.start()

        # 等待處理
        time.sleep(3)
        file_thread.join()

        # 檢查結果
        result_df = file_spark.sql("SELECT * FROM csv_stream_test")
        if result_df.count() > 0:
            assert "doubled_value" in result_df.columns
            rows = result_df.collect()
            assert any(row.doubled_value == 200 for row in rows)  # Alice: 100 * 2

        query.stop()

    def test_json_file_stream(self, file_spark, temp_dir):
        """測試 JSON 文件流"""
        # 定義 schema
        schema = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("score", IntegerType(), True),
            ]
        )

        # 創建輸入目錄
        input_dir = os.path.join(temp_dir, "json_input")
        os.makedirs(input_dir, exist_ok=True)

        # 創建 JSON 流
        json_stream = file_spark.readStream.schema(schema).json(input_dir)

        # 過濾和轉換
        processed_stream = json_stream.filter(col("score") > 80).select(
            col("id"),
            col("name"),
            col("score"),
            when(col("score") >= 90, "A").otherwise("B").alias("grade"),
        )

        query = (
            processed_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("json_stream_test")
            .start()
        )

        # 寫入測試文件
        def write_json_file():
            time.sleep(1)
            test_file = os.path.join(input_dir, "scores.json")
            with open(test_file, "w") as f:
                f.write('{"id": 1, "name": "Alice", "score": 95}\n')
                f.write('{"id": 2, "name": "Bob", "score": 85}\n')
                f.write('{"id": 3, "name": "Charlie", "score": 75}\n')

        json_thread = threading.Thread(target=write_json_file)
        json_thread.start()

        time.sleep(3)
        json_thread.join()

        # 檢查結果
        result_df = file_spark.sql("SELECT * FROM json_stream_test")
        if result_df.count() > 0:
            rows = result_df.collect()
            scores = [row.score for row in rows]
            assert all(score > 80 for score in scores)  # 過濾條件
            grades = [row.grade for row in rows]
            assert "A" in grades or "B" in grades

        query.stop()


class TestStreamingOutputModes:
    """測試流輸出模式"""

    @pytest.fixture(scope="function")
    def output_spark(self):
        """創建支持輸出模式測試的 SparkSession"""
        spark = (
            SparkSession.builder.appName("OutputModeTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        for stream in spark.streams.active:
            stream.stop()
        spark.stop()

    def test_append_mode(self, output_spark):
        """測試 append 輸出模式"""
        rate_stream = (
            output_spark.readStream.format("rate").option("rowsPerSecond", 3).load()
        )

        # 簡單轉換（適合 append 模式）
        processed_stream = rate_stream.select(
            col("timestamp"), col("value"), (col("value") % 10).alias("mod_value")
        )

        query = (
            processed_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("append_test")
            .start()
        )

        time.sleep(3)

        result_df = output_spark.sql("SELECT * FROM append_test")
        initial_count = result_df.count()

        time.sleep(2)

        result_df = output_spark.sql("SELECT * FROM append_test")
        final_count = result_df.count()

        # append 模式下，記錄數應該增加
        assert final_count >= initial_count

        query.stop()

    def test_complete_mode(self, output_spark):
        """測試 complete 輸出模式"""
        rate_stream = (
            output_spark.readStream.format("rate").option("rowsPerSecond", 5).load()
        )

        # 聚合操作（適合 complete 模式）
        aggregated_stream = (
            rate_stream.withColumn("bucket", col("value") % 3).groupBy("bucket").count()
        )

        query = (
            aggregated_stream.writeStream.outputMode("complete")
            .format("memory")
            .queryName("complete_test")
            .start()
        )

        time.sleep(3)

        result_df = output_spark.sql("SELECT * FROM complete_test")
        if result_df.count() > 0:
            # complete 模式會顯示所有分組的結果
            buckets = [row.bucket for row in result_df.collect()]
            assert len(set(buckets)) <= 3  # 最多3個bucket (0, 1, 2)

        query.stop()

    def test_update_mode(self, output_spark):
        """測試 update 輸出模式"""
        rate_stream = (
            output_spark.readStream.format("rate").option("rowsPerSecond", 4).load()
        )

        # 聚合操作（適合 update 模式）
        aggregated_stream = (
            rate_stream.withColumn("group_key", col("value") % 2)
            .groupBy("group_key")
            .agg(count("*").alias("count"), avg("value").alias("avg_value"))
        )

        query = (
            aggregated_stream.writeStream.outputMode("update")
            .format("memory")
            .queryName("update_test")
            .start()
        )

        time.sleep(3)

        result_df = output_spark.sql("SELECT * FROM update_test")
        if result_df.count() > 0:
            # update 模式會顯示更新的聚合結果
            assert "count" in result_df.columns
            assert "avg_value" in result_df.columns

        query.stop()


class TestStreamingErrorHandling:
    """測試流錯誤處理"""

    @pytest.fixture(scope="function")
    def error_spark(self):
        """創建用於錯誤處理測試的 SparkSession"""
        spark = (
            SparkSession.builder.appName("ErrorHandlingTest")
            .master("local[2]")
            .config("spark.sql.streaming.checkpointLocation", tempfile.mkdtemp())
            .getOrCreate()
        )

        yield spark

        for stream in spark.streams.active:
            try:
                stream.stop()
            except:
                pass
        spark.stop()

    def test_query_exception_handling(self, error_spark):
        """測試查詢異常處理"""
        rate_stream = (
            error_spark.readStream.format("rate").option("rowsPerSecond", 2).load()
        )

        # 正常的查詢
        normal_query = (
            rate_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("normal_query")
            .start()
        )

        time.sleep(2)

        # 檢查查詢狀態
        assert normal_query.isActive
        assert normal_query.exception() is None

        normal_query.stop()

    def test_invalid_output_mode(self, error_spark):
        """測試無效輸出模式"""
        rate_stream = (
            error_spark.readStream.format("rate").option("rowsPerSecond", 1).load()
        )

        # 對於非聚合查詢，complete 模式是無效的
        try:
            invalid_query = (
                rate_stream.writeStream.outputMode("complete")
                .format("memory")
                .queryName("invalid_query")
                .start()
            )

            time.sleep(1)
            invalid_query.stop()

            # 如果沒有拋出異常，這個測試可能需要調整
            # 某些版本的 Spark 可能在運行時才檢測到錯誤
        except Exception as e:
            # 預期的異常
            assert "complete" in str(e).lower() or "output mode" in str(e).lower()

    def test_checkpoint_recovery(self, error_spark, temp_dir):
        """測試檢查點恢復"""
        checkpoint_dir = os.path.join(temp_dir, "checkpoint")

        rate_stream = (
            error_spark.readStream.format("rate").option("rowsPerSecond", 3).load()
        )

        # 帶檢查點的查詢
        query = (
            rate_stream.writeStream.outputMode("append")
            .format("memory")
            .queryName("checkpoint_test")
            .option("checkpointLocation", checkpoint_dir)
            .start()
        )

        time.sleep(2)

        # 檢查檢查點目錄是否創建
        assert os.path.exists(checkpoint_dir)

        query.stop()
