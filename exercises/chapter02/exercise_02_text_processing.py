#!/usr/bin/env python3
"""
第2章練習2：文本處理
RDD 文本分析和處理練習
"""

from pyspark.sql import SparkSession
import re
from collections import Counter

def main():
    # 創建 SparkSession
    spark = SparkSession.builder \
        .appName("RDD文本處理練習") \
        .master("local[*]") \
        .getOrCreate()
    
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    print("=== 第2章練習2：RDD 文本處理 ===")
    
    # 1. 創建文本數據
    print("\n1. 創建文本數據:")
    
    text_data = [
        "Apache Spark is a unified analytics engine for large-scale data processing",
        "It provides high-level APIs in Java, Scala, Python and R",
        "Spark runs on Hadoop, Apache Mesos, Kubernetes, standalone, or in the cloud",
        "It can access diverse data sources including HDFS, Alluxio, Apache Cassandra",
        "Spark's main programming abstraction is a resilient distributed dataset RDD",
        "RDDs can be created from Hadoop InputFormats or by transforming other RDDs",
        "Spark provides two types of operations: transformations and actions",
        "Transformations are lazy and actions trigger computation",
        "Spark supports both batch and streaming data processing",
        "Machine learning with Spark MLlib makes analytics fast and easy"
    ]
    
    # 創建 RDD
    text_rdd = sc.parallelize(text_data)
    
    print(f"文本行數: {text_rdd.count()}")
    print("原始文本:")
    for i, line in enumerate(text_rdd.collect(), 1):
        print(f"{i}. {line}")
    
    # 2. 基本文本統計
    print("\n2. 基本文本統計:")
    
    # 計算總字符數
    char_count = text_rdd.map(len).reduce(lambda a, b: a + b)
    print(f"總字符數: {char_count}")
    
    # 計算總單詞數
    word_count = text_rdd.flatMap(lambda line: line.split()).count()
    print(f"總單詞數: {word_count}")
    
    # 計算平均行長度
    avg_line_length = text_rdd.map(len).mean()
    print(f"平均行長度: {avg_line_length:.2f} 字符")
    
    # 最長和最短的行
    longest_line = text_rdd.max(key=len)
    shortest_line = text_rdd.min(key=len)
    print(f"最長行: {longest_line}")
    print(f"最短行: {shortest_line}")
    
    # 3. 單詞頻率分析
    print("\n3. 單詞頻率分析:")
    
    # 清理和分割單詞
    def clean_word(word):
        # 移除標點符號並轉換為小寫
        return re.sub(r'[^\w]', '', word).lower()
    
    # 提取所有單詞並清理
    words_rdd = text_rdd.flatMap(lambda line: line.split()) \
                       .map(clean_word) \
                       .filter(lambda word: len(word) > 0)
    
    print(f"清理後單詞總數: {words_rdd.count()}")
    print(f"不重複單詞數: {words_rdd.distinct().count()}")
    
    # 計算單詞頻率
    word_counts = words_rdd.map(lambda word: (word, 1)) \
                          .reduceByKey(lambda a, b: a + b)
    
    # 按頻率排序
    top_words = word_counts.sortBy(lambda x: x[1], ascending=False)
    
    print("\n最常見的10個單詞:")
    for word, count in top_words.take(10):
        print(f"{word}: {count}")
    
    # 4. 長度分析
    print("\n4. 單詞長度分析:")
    
    # 按單詞長度分組
    word_lengths = words_rdd.map(len)
    
    print(f"平均單詞長度: {word_lengths.mean():.2f}")
    print(f"最長單詞長度: {word_lengths.max()}")
    print(f"最短單詞長度: {word_lengths.min()}")
    
    # 長度分布
    length_distribution = word_lengths.map(lambda length: (length, 1)) \
                                    .reduceByKey(lambda a, b: a + b) \
                                    .sortByKey()
    
    print("\n單詞長度分布:")
    for length, count in length_distribution.collect():
        print(f"{length}字符: {count}個單詞")
    
    # 5. 搜索和過濾
    print("\n5. 文本搜索和過濾:")
    
    # 搜索包含特定關鍵詞的行
    search_keyword = "Spark"
    matching_lines = text_rdd.filter(lambda line: search_keyword in line)
    
    print(f"包含 '{search_keyword}' 的行數: {matching_lines.count()}")
    print("匹配的行:")
    for i, line in enumerate(matching_lines.collect(), 1):
        print(f"{i}. {line}")
    
    # 搜索多個關鍵詞
    keywords = ["data", "processing", "analytics"]
    multi_keyword_lines = text_rdd.filter(
        lambda line: any(keyword in line.lower() for keyword in keywords)
    )
    
    print(f"\n包含關鍵詞 {keywords} 的行:")
    for line in multi_keyword_lines.collect():
        print(f"- {line}")
    
    # 6. 文本轉換
    print("\n6. 文本轉換:")
    
    # 轉換為大寫
    uppercase_rdd = text_rdd.map(str.upper)
    print("轉換為大寫:")
    uppercase_rdd.take(3)
    
    # 替換文本
    replaced_rdd = text_rdd.map(lambda line: line.replace("Spark", "Apache Spark"))
    print("\n替換後的文本 (前3行):")
    for line in replaced_rdd.take(3):
        print(f"- {line}")
    
    # 提取句子中的數字
    number_pattern = r'\b\d+\b'
    numbers_rdd = text_rdd.flatMap(lambda line: re.findall(number_pattern, line))
    
    if numbers_rdd.count() > 0:
        print(f"\n提取的數字: {numbers_rdd.collect()}")
    else:
        print("\n文本中沒有找到數字")
    
    # 7. 高級文本分析
    print("\n7. 高級文本分析:")
    
    # N-gram 分析 (2-gram)
    def create_bigrams(line):
        words = line.lower().split()
        bigrams = []
        for i in range(len(words) - 1):
            bigram = f"{words[i]} {words[i+1]}"
            bigrams.append(bigram)
        return bigrams
    
    bigrams_rdd = text_rdd.flatMap(create_bigrams)
    bigram_counts = bigrams_rdd.map(lambda bigram: (bigram, 1)) \
                              .reduceByKey(lambda a, b: a + b) \
                              .sortBy(lambda x: x[1], ascending=False)
    
    print("最常見的5個二元詞組:")
    for bigram, count in bigram_counts.take(5):
        print(f"'{bigram}': {count}")
    
    # 8. 文本清理和標準化
    print("\n8. 文本清理和標準化:")
    
    def clean_text(line):
        # 移除多餘空格，標準化標點
        line = re.sub(r'\s+', ' ', line)  # 多個空格變成一個
        line = re.sub(r'[,;]', ',', line)  # 標準化標點
        return line.strip()
    
    cleaned_rdd = text_rdd.map(clean_text)
    
    print("清理後的文本:")
    for line in cleaned_rdd.take(3):
        print(f"- {line}")
    
    # 9. 停用詞過濾
    print("\n9. 停用詞過濾:")
    
    stop_words = {"a", "an", "and", "are", "as", "at", "be", "by", "for", 
                  "from", "has", "he", "in", "is", "it", "its", "of", "on", 
                  "that", "the", "to", "was", "will", "with", "or"}
    
    # 過濾停用詞後的單詞頻率
    filtered_words = words_rdd.filter(lambda word: word not in stop_words)
    filtered_word_counts = filtered_words.map(lambda word: (word, 1)) \
                                       .reduceByKey(lambda a, b: a + b) \
                                       .sortBy(lambda x: x[1], ascending=False)
    
    print("過濾停用詞後的前10個高頻詞:")
    for word, count in filtered_word_counts.take(10):
        print(f"{word}: {count}")
    
    # 10. 文本摘要統計
    print("\n10. 文本摘要統計:")
    
    # 創建詞彙表
    vocabulary = words_rdd.distinct().collect()
    vocabulary_size = len(vocabulary)
    
    # 計算詞彙豐富度
    total_words = words_rdd.count()
    vocabulary_richness = vocabulary_size / total_words
    
    # 統計長單詞（>5字符）
    long_words = words_rdd.filter(lambda word: len(word) > 5)
    long_word_count = long_words.count()
    long_word_ratio = long_word_count / total_words
    
    print("文本統計摘要:")
    print(f"- 總行數: {text_rdd.count()}")
    print(f"- 總單詞數: {total_words}")
    print(f"- 詞彙表大小: {vocabulary_size}")
    print(f"- 詞彙豐富度: {vocabulary_richness:.3f}")
    print(f"- 長單詞數量: {long_word_count}")
    print(f"- 長單詞比例: {long_word_ratio:.3f}")
    
    # 11. 條件統計
    print("\n11. 條件統計:")
    
    # 統計包含特定模式的行
    patterns = {
        "包含'data'": lambda line: 'data' in line.lower(),
        "包含'Apache'": lambda line: 'Apache' in line,
        "超過50字符": lambda line: len(line) > 50,
        "包含大寫詞": lambda line: any(word.isupper() for word in line.split())
    }
    
    for description, condition in patterns.items():
        count = text_rdd.filter(condition).count()
        print(f"{description}: {count} 行")
    
    # 12. 文本相似性分析
    print("\n12. 簡單相似性分析:")
    
    # 計算每行的單詞集合
    def get_word_set(line):
        words = set(clean_word(word) for word in line.split())
        return words - {''}  # 移除空字符串
    
    line_word_sets = text_rdd.map(get_word_set).collect()
    
    # 計算第一行與其他行的交集大小
    if len(line_word_sets) > 1:
        first_line_words = line_word_sets[0]
        print(f"第一行單詞集合: {sorted(first_line_words)}")
        
        for i, word_set in enumerate(line_word_sets[1:], 2):
            intersection = first_line_words.intersection(word_set)
            print(f"與第{i}行的共同單詞數: {len(intersection)}")
    
    # 清理資源
    spark.stop()
    print("\n文本處理練習完成！")

if __name__ == "__main__":
    main()