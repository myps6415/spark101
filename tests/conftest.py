"""
pytest 配置文件
提供共享的測試設置和fixture
"""

import os
import shutil
import tempfile

import pytest
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark_session():
    """創建測試用的 SparkSession"""
    os.environ["HADOOP_USER_NAME"] = "spark"
    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11/libexec/openjdk.jdk/Contents/Home"
    
    spark = (
        SparkSession.builder.appName("TestSpark")
        .master("local[2]")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
        .config("spark.hadoop.validateOutputSpecs", "false")
        .config("spark.driver.extraJavaOptions", "--add-opens=java.base/java.security=ALL-UNNAMED --add-opens=java.base/javax.security.auth=ALL-UNNAMED")
        .config("spark.executor.extraJavaOptions", "--add-opens=java.base/java.security=ALL-UNNAMED --add-opens=java.base/javax.security.auth=ALL-UNNAMED")
        .getOrCreate()
    )

    yield spark

    spark.stop()


@pytest.fixture(scope="session")
def spark_context(spark_session):
    """獲取 SparkContext"""
    return spark_session.sparkContext


@pytest.fixture(scope="function")
def temp_dir():
    """創建臨時目錄"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)


@pytest.fixture(scope="function")
def sample_data(spark_session):
    """創建示例數據"""
    data = [
        ("Alice", 25, "Engineer", 75000),
        ("Bob", 30, "Manager", 85000),
        ("Charlie", 35, "Designer", 65000),
        ("Diana", 28, "Analyst", 70000),
    ]
    columns = ["name", "age", "job", "salary"]
    return spark_session.createDataFrame(data, columns)


@pytest.fixture(scope="function")
def sample_rdd(spark_context):
    """創建示例 RDD"""
    return spark_context.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])


@pytest.fixture(scope="function")
def sample_csv_file(temp_dir):
    """創建示例 CSV 文件"""
    csv_content = """name,age,city,salary
Alice,25,Taipei,50000
Bob,30,Taichung,60000
Charlie,35,Kaohsiung,65000"""

    csv_file = os.path.join(temp_dir, "test_data.csv")
    with open(csv_file, "w") as f:
        f.write(csv_content)

    return csv_file


@pytest.fixture(scope="function")
def sample_json_file(temp_dir):
    """創建示例 JSON 文件"""
    json_content = """{"name": "Alice", "age": 25, "city": "Taipei"}
{"name": "Bob", "age": 30, "city": "Taichung"}
{"name": "Charlie", "age": 35, "city": "Kaohsiung"}"""

    json_file = os.path.join(temp_dir, "test_data.json")
    with open(json_file, "w") as f:
        f.write(json_content)

    return json_file


def pytest_configure(config):
    """pytest 配置"""
    # 設置測試環境變量
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"


def pytest_collection_modifyitems(config, items):
    """修改測試項目"""
    # 可以在這裡添加自定義的測試標記
    pass
