#!/usr/bin/env python3
"""
第3章：DataFrame 和 Dataset API - 資料讀寫操作
學習各種資料格式的讀寫操作
"""

import os
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("Data I/O Operations")
        .master("local[*]")
        .getOrCreate()
    )

    print("📁 資料讀寫操作示範")
    print("=" * 40)

    # 準備示例數據
    sample_data = [
        ("Alice", 25, "Engineer", 75000.0, "IT"),
        ("Bob", 30, "Manager", 85000.0, "Sales"),
        ("Charlie", 35, "Designer", 65000.0, "Marketing"),
        ("Diana", 28, "Analyst", 70000.0, "Finance"),
        ("Eve", 32, "Engineer", 80000.0, "IT"),
    ]

    columns = ["name", "age", "job", "salary", "department"]
    df = spark.createDataFrame(sample_data, columns)

    print("原始 DataFrame:")
    df.show()

    # 創建臨時目錄用於示例
    temp_dir = tempfile.mkdtemp()
    print(f"臨時目錄: {temp_dir}")

    # 1. CSV 文件操作
    print("\n1️⃣ CSV 文件操作")

    # 寫入 CSV
    csv_path = os.path.join(temp_dir, "employees.csv")
    df.coalesce(1).write.mode("overwrite").option("header", "true").option(
        "sep", ","
    ).csv(csv_path)
    print(f"DataFrame 已寫入 CSV: {csv_path}")

    # 讀取 CSV
    df_csv = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("sep", ",")
        .csv(csv_path)
    )

    print("從 CSV 讀取的 DataFrame:")
    df_csv.show()
    df_csv.printSchema()

    # 2. JSON 文件操作
    print("\n2️⃣ JSON 文件操作")

    # 寫入 JSON
    json_path = os.path.join(temp_dir, "employees.json")
    df.coalesce(1).write.mode("overwrite").json(json_path)
    print(f"DataFrame 已寫入 JSON: {json_path}")

    # 讀取 JSON
    df_json = spark.read.json(json_path)
    print("從 JSON 讀取的 DataFrame:")
    df_json.show()
    df_json.printSchema()

    # 3. Parquet 文件操作 (推薦格式)
    print("\n3️⃣ Parquet 文件操作")

    # 寫入 Parquet
    parquet_path = os.path.join(temp_dir, "employees.parquet")
    df.write.mode("overwrite").parquet(parquet_path)
    print(f"DataFrame 已寫入 Parquet: {parquet_path}")

    # 讀取 Parquet
    df_parquet = spark.read.parquet(parquet_path)
    print("從 Parquet 讀取的 DataFrame:")
    df_parquet.show()
    df_parquet.printSchema()

    # 4. 分區寫入
    print("\n4️⃣ 分區寫入")

    # 按部門分區寫入
    partitioned_path = os.path.join(temp_dir, "employees_partitioned")
    df.write.mode("overwrite").partitionBy("department").parquet(partitioned_path)
    print(f"分區 DataFrame 已寫入: {partitioned_path}")

    # 讀取分區數據
    df_partitioned = spark.read.parquet(partitioned_path)
    print("從分區讀取的 DataFrame:")
    df_partitioned.show()

    # 讀取特定分區
    df_it_partition = spark.read.parquet(f"{partitioned_path}/department=IT")
    print("IT 部門的數據:")
    df_it_partition.show()

    # 5. 高級讀寫選項
    print("\n5️⃣ 高級讀寫選項")

    # 創建包含特殊字符的數據
    special_data = [
        ("Alice,Jr", 25, "Engineer", 75000.0),
        ("Bob\nSmith", 30, "Manager", 85000.0),
        ('Charlie"Brown', 35, "Designer", 65000.0),
    ]

    special_df = spark.createDataFrame(special_data, ["name", "age", "job", "salary"])
    print("包含特殊字符的 DataFrame:")
    special_df.show()

    # 使用自定義分隔符和引號
    special_csv_path = os.path.join(temp_dir, "special_employees.csv")
    special_df.coalesce(1).write.mode("overwrite").option("header", "true").option(
        "sep", "|"
    ).option("quote", "'").option("escape", "\\").csv(special_csv_path)

    # 讀取自定義格式的 CSV
    df_special_csv = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .option("sep", "|")
        .option("quote", "'")
        .option("escape", "\\")
        .csv(special_csv_path)
    )

    print("讀取自定義格式 CSV:")
    df_special_csv.show()

    # 6. 資料壓縮
    print("\n6️⃣ 資料壓縮")

    # 使用不同壓縮格式
    compression_formats = ["none", "gzip", "snappy"]

    for compression in compression_formats:
        compressed_path = os.path.join(temp_dir, f"employees_{compression}")
        df.write.mode("overwrite").option("compression", compression).parquet(
            compressed_path
        )
        print(f"使用 {compression} 壓縮格式寫入: {compressed_path}")

    # 7. 多檔案讀取
    print("\n7️⃣ 多檔案讀取")

    # 創建多個文件
    for i in range(3):
        file_data = [
            (f"User_{i}_{j}", 20 + i + j, f"Job_{i}", 50000.0 + i * 1000 + j * 100)
            for j in range(2)
        ]
        file_df = spark.createDataFrame(file_data, ["name", "age", "job", "salary"])
        file_path = os.path.join(temp_dir, f"batch_{i}.json")
        file_df.coalesce(1).write.mode("overwrite").json(file_path)

    # 讀取所有 JSON 文件
    all_files_path = os.path.join(temp_dir, "batch_*.json")
    df_all = spark.read.json(all_files_path)
    print("讀取所有批次文件:")
    df_all.show()

    # 8. 錯誤處理和資料品質
    print("\n8️⃣ 錯誤處理和資料品質")

    # 創建包含錯誤數據的 CSV
    bad_csv_content = """name,age,salary
Alice,25,75000
Bob,thirty,85000
Charlie,35,not_a_number
Diana,28,70000"""

    bad_csv_path = os.path.join(temp_dir, "bad_data.csv")
    with open(bad_csv_path, "w") as f:
        f.write(bad_csv_content)

    # 嘗試讀取錯誤數據
    print("嘗試讀取包含錯誤的 CSV:")
    try:
        # 設置模式為 PERMISSIVE (預設)
        df_bad = (
            spark.read.option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(bad_csv_path)
        )

        print("PERMISSIVE 模式讀取結果:")
        df_bad.show()

        # 過濾出錯誤記錄
        corrupt_records = df_bad.filter(col("_corrupt_record").isNotNull())
        if corrupt_records.count() > 0:
            print("錯誤記錄:")
            corrupt_records.show()

    except Exception as e:
        print(f"讀取錯誤: {e}")

    # 9. 資料格式轉換
    print("\n9️⃣ 資料格式轉換")

    # CSV 轉 Parquet
    df_csv.write.mode("overwrite").parquet(os.path.join(temp_dir, "csv_to_parquet"))
    print("CSV 轉 Parquet 完成")

    # JSON 轉 CSV
    df_json.coalesce(1).write.mode("overwrite").option("header", "true").csv(
        os.path.join(temp_dir, "json_to_csv")
    )
    print("JSON 轉 CSV 完成")

    # 10. 資料品質檢查
    print("\n🔟 資料品質檢查")

    # 檢查空值
    print("空值統計:")
    df.select([col(c).isNull().cast("int").alias(c) for c in df.columns]).agg(
        *[col(c).sum().alias(f"{c}_nulls") for c in df.columns]
    ).show()

    # 檢查重複記錄
    print(f"總記錄數: {df.count()}")
    print(f"去重後記錄數: {df.distinct().count()}")

    # 檢查數值範圍
    print("數值列統計:")
    df.select("age", "salary").describe().show()

    # 清理臨時目錄
    import shutil

    shutil.rmtree(temp_dir)
    print(f"清理臨時目錄: {temp_dir}")

    # 停止 SparkSession
    spark.stop()
    print("\n✅ 資料讀寫操作示範完成")


if __name__ == "__main__":
    main()
