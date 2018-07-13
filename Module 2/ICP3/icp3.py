
from pyspark.sql import SparkSession

import pandas as pd

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *

print("\nRead Data from File")

df = spark.read.option("header", "true").csv("/Users/aditya/Downloads/ConsumerComplaints.csv")

print("Counting the number of duplicate rows")

repeated_count = df.count() - df.distinct().count()

print("\nCounting number of repeated rows: " + str(repeated_count))

print("Creating New DataFrame: ")

df1 = spark.read.option("header", "true").csv("/Users/aditya/Downloads/union.csv")

# unionDf.write.csv("/Users/aditya/Desktop/Output1")

print("Union the two DataFrames")

unionDf = df.unionAll(df1)

#unionDf.write.csv("/Users/aditya/Desktop/union")

print("Union Output: ")

unionDf.orderBy(unionDf['Company'].desc()).show()

print("Grouping by Zip Code and using count: ")

unionDf.groupBy("Zip Code").count().show()

print("Using Cross Join: ")

df.crossJoin(df1).show()

print("Printing out the 13th Row: ")

print(df.take(13)[-1])
