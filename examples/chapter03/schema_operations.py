#!/usr/bin/env python3
"""
第3章：DataFrame 和 Dataset API - Schema 操作
學習 Schema 定義、類型轉換和資料驗證
"""

from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import (cast, col, regexp_extract, to_date,
                                   to_timestamp)
from pyspark.sql.types import (ArrayType, BooleanType, DateType, DoubleType,
                               IntegerType, StringType, StructField,
                               StructType, TimestampType)


def main():
    # 創建 SparkSession
    spark = (
        SparkSession.builder.appName("Schema Operations")
        .master("local[*]")
        .getOrCreate()
    )

    print("🏗️ Schema 操作示範")
    print("=" * 40)

    # 1. 定義複雜 Schema
    print("\n1️⃣ 定義複雜 Schema")

    # 定義嵌套的 Schema
    address_schema = StructType(
        [
            StructField("street", StringType(), True),
            StructField("city", StringType(), True),
            StructField("zipcode", StringType(), True),
        ]
    )

    employee_schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("hire_date", DateType(), True),
            StructField("last_login", TimestampType(), True),
            StructField("skills", ArrayType(StringType()), True),
            StructField("address", address_schema, True),
        ]
    )

    print("員工 Schema 結構:")
    print(employee_schema)

    # 2. 使用 Schema 創建 DataFrame
    print("\n2️⃣ 使用 Schema 創建 DataFrame")

    # 準備複雜數據
    complex_data = [
        (
            1,
            "Alice",
            25,
            75000.0,
            True,
            date(2020, 1, 15),
            datetime(2024, 1, 10, 9, 30, 0),
            ["Python", "Spark", "SQL"],
            ("123 Main St", "New York", "10001"),
        ),
        (
            2,
            "Bob",
            30,
            85000.0,
            True,
            date(2019, 3, 20),
            datetime(2024, 1, 9, 14, 45, 0),
            ["Java", "Scala", "Kafka"],
            ("456 Oak Ave", "Boston", "02101"),
        ),
        (
            3,
            "Charlie",
            35,
            65000.0,
            False,
            date(2021, 7, 10),
            datetime(2024, 1, 8, 11, 20, 0),
            ["JavaScript", "React", "Node.js"],
            ("789 Pine Rd", "Seattle", "98101"),
        ),
    ]

    df = spark.createDataFrame(complex_data, employee_schema)
    print("複雜 DataFrame:")
    df.show(truncate=False)

    print("\nDataFrame Schema:")
    df.printSchema()

    # 3. Schema 驗證和檢查
    print("\n3️⃣ Schema 驗證和檢查")

    # 檢查列是否存在
    required_columns = ["id", "name", "salary"]
    for col_name in required_columns:
        if col_name in df.columns:
            print(f"✅ 列 '{col_name}' 存在")
        else:
            print(f"❌ 列 '{col_name}' 不存在")

    # 檢查資料類型
    print("\n列的資料類型:")
    for field in df.schema.fields:
        print(f"  {field.name}: {field.dataType}")

    # 檢查空值約束
    print("\n空值約束:")
    for field in df.schema.fields:
        nullable = "可空" if field.nullable else "不可空"
        print(f"  {field.name}: {nullable}")

    # 4. 類型轉換
    print("\n4️⃣ 類型轉換")

    # 字串轉數字
    string_data = [
        ("1", "Alice", "25", "75000.50"),
        ("2", "Bob", "30", "85000.75"),
        ("3", "Charlie", "35", "65000.25"),
    ]

    string_df = spark.createDataFrame(
        string_data, ["id_str", "name", "age_str", "salary_str"]
    )
    print("字串格式的 DataFrame:")
    string_df.show()
    string_df.printSchema()

    # 轉換資料類型
    converted_df = string_df.select(
        cast(col("id_str"), IntegerType()).alias("id"),
        col("name"),
        cast(col("age_str"), IntegerType()).alias("age"),
        cast(col("salary_str"), DoubleType()).alias("salary"),
    )

    print("轉換後的 DataFrame:")
    converted_df.show()
    converted_df.printSchema()

    # 5. 日期時間處理
    print("\n5️⃣ 日期時間處理")

    # 字串轉日期
    date_data = [
        ("Alice", "2020-01-15", "2024-01-10 09:30:00"),
        ("Bob", "2019-03-20", "2024-01-09 14:45:00"),
        ("Charlie", "2021-07-10", "2024-01-08 11:20:00"),
    ]

    date_df = spark.createDataFrame(
        date_data, ["name", "hire_date_str", "last_login_str"]
    )
    print("字串格式的日期 DataFrame:")
    date_df.show()

    # 轉換日期格式
    date_converted_df = date_df.select(
        col("name"),
        to_date(col("hire_date_str"), "yyyy-MM-dd").alias("hire_date"),
        to_timestamp(col("last_login_str"), "yyyy-MM-dd HH:mm:ss").alias("last_login"),
    )

    print("轉換後的日期 DataFrame:")
    date_converted_df.show()
    date_converted_df.printSchema()

    # 6. 從文本中提取結構化資料
    print("\n6️⃣ 從文本中提取結構化資料")

    # 包含非結構化文本的數據
    text_data = [
        ("user001", "Name: Alice Johnson, Age: 25, Email: alice@example.com"),
        ("user002", "Name: Bob Smith, Age: 30, Email: bob@example.com"),
        ("user003", "Name: Charlie Brown, Age: 35, Email: charlie@example.com"),
    ]

    text_df = spark.createDataFrame(text_data, ["user_id", "info"])
    print("包含非結構化文本的 DataFrame:")
    text_df.show(truncate=False)

    # 使用正則表達式提取信息
    extracted_df = text_df.select(
        col("user_id"),
        regexp_extract(col("info"), r"Name: ([^,]+)", 1).alias("name"),
        regexp_extract(col("info"), r"Age: (\d+)", 1).cast(IntegerType()).alias("age"),
        regexp_extract(col("info"), r"Email: ([^\s]+)", 1).alias("email"),
    )

    print("提取後的結構化 DataFrame:")
    extracted_df.show()
    extracted_df.printSchema()

    # 7. Schema 演化和相容性
    print("\n7️⃣ Schema 演化和相容性")

    # 原始 Schema
    old_schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
        ]
    )

    # 新 Schema (添加了欄位)
    new_schema = StructType(
        [
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), False),
            StructField("age", IntegerType(), True),
            StructField("department", StringType(), True),  # 新增欄位
            StructField("salary", DoubleType(), True),  # 新增欄位
        ]
    )

    # 舊格式數據
    old_data = [(1, "Alice", 25), (2, "Bob", 30)]
    old_df = spark.createDataFrame(old_data, old_schema)

    print("舊格式 DataFrame:")
    old_df.show()

    # 使新舊格式相容
    from pyspark.sql.functions import lit

    compatible_df = old_df.select(
        col("id"),
        col("name"),
        col("age"),
        lit(None).cast(StringType()).alias("department"),
        lit(None).cast(DoubleType()).alias("salary"),
    )

    print("相容性調整後的 DataFrame:")
    compatible_df.show()
    compatible_df.printSchema()

    # 8. 動態 Schema 操作
    print("\n8️⃣ 動態 Schema 操作")

    # 動態添加列
    dynamic_df = df.select("id", "name", "age", "salary")

    # 根據條件動態添加列
    columns_to_add = ["bonus", "tax", "net_salary"]

    for col_name in columns_to_add:
        if col_name == "bonus":
            dynamic_df = dynamic_df.withColumn(col_name, col("salary") * 0.1)
        elif col_name == "tax":
            dynamic_df = dynamic_df.withColumn(col_name, col("salary") * 0.25)
        elif col_name == "net_salary":
            dynamic_df = dynamic_df.withColumn(
                col_name, col("salary") - col("tax") + col("bonus")
            )

    print("動態添加列後的 DataFrame:")
    dynamic_df.show()

    # 停止 SparkSession
    spark.stop()
    print("\n✅ Schema 操作示範完成")


if __name__ == "__main__":
    main()
