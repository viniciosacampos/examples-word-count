from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark import SQLContext
import re

sc = SparkContext("local", "Big data Word Count")
SqlContext = SQLContext(sc)

us_constitution = sc.textFile("file:///big_data_test/us_constitution.txt")
DFus = us_constitution.flatMap(lambda line: re.sub(r'[^a-zA-Z]', ' ', line).split(" ")).map(lambda word: [word]).toDF(["words"])

DFwor = sc.textFile("file:///big_data_test/top_english_words.txt").map(lambda line: [line]).toDF(["search"])

result = DFus.alias("a") \
	.join(DFwor.alias("b"), lower(trim(col("a.words"))) == lower(trim(col("b.search"))), how="inner" ) \
	.groupBy(col("b.search")) \
	.agg( count(col("a.words")).alias("qtde")) \
	.select(col("qtde"),
			concat(lower(col("search")), lit(" ") ,col("qtde")).alias("words_count")) \
	.orderBy(col("qtde").desc())

result.rdd.map(lambda row: row.__getitem__("words_count")).repartition(1).saveAsTextFile("file:///big_data_test/result")