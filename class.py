from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, avg, mean

spark = SparkSession.builder.appName("demo").getOrCreate()

# Fast food places w made up sales
df = spark.createDataFrame(
    [
        ("McDonald", 32000000),
        ("Burger king", 3000000),
        ("Wendy's", 75000000),
        ("KFC", 13000000),
    ],
    ["fast_food", "regional_sales"],
)

df.show()

# Find the average for the column
avg_sales = df.agg({"regional_sales": "mean"}).collect()[0][0]

# Show the areas with a larger than average market
df = df.withColumn(
    "large_market",
    when(avg_sales >= col("regional_sales"), True).otherwise(False)
)

df.show()

spark.sql("select fast_food from {df} where large_market = True", df=df).show()

# Of all of the words, Lorem vs ipsum, which is more common
text_file = spark.sparkContext.textFile("example.txt")

word_counts = text_file.flatMap(lambda line: line.split(" ")) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b)

counts_df = spark.createDataFrame(word_counts.collect(), ["word", "count"])

spark.sql("select word, count from {counts_df} order by count desc limit 2", counts_df=counts_df).show()