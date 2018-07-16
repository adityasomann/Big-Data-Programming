import os

os.environ["SPARK_HOME"] = "/usr/local/Cellar/spark-2.3.1-bin-hadoop2.7"

from pyspark.sql import SparkSession
from pyspark import SparkContext


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# import pyspark class Row from module sql
from pyspark.sql import *

print("\nCreating Football DataFrame: \n")

football_df = spark.read.option("header", "true").csv("/Users/aditya/Downloads/lab1 csv/WorldCups.csv")
football_df.show()

#Number of World Cups
count = football_df.count()

print("Number of World Cups that have taken place: " + str(count))

#Increase in the countries playing the sport
maxteams = football_df.select('QualifiedTeams').rdd.max()[0]
minteams = football_df.select('QualifiedTeams').rdd.min()[0]
increase = ((int(maxteams) - int(minteams))/int(minteams)) * 100

print("\nThe percentage increase in number of teams is " + str(increase) + "%")


# Maximum number of world cup wins:
football_df.groupBy(['Football Winner']).count().orderBy("count", ascending=False).show(1)

# Most unluckiest Country in the history of World Cups:

football_df.groupBy(['Football Runners-Up']).count().orderBy("count", ascending=False).show()

# Most number of times a country has hosted:

football_df.groupBy(['Countries']).count().orderBy("count", ascending=False).show(1)

# Creating another dataset

print("\nCreating Cricket DataFrame: \n")

cricket_df = spark.read.option("header", "true").csv("/Users/aditya/Downloads/lab1 csv/cric.csv")

cricket_df.show()

# Union

unionDf = football_df.unionAll(cricket_df)

unionDf.show()

unionDf.write.option("header", "true").csv("Union_Output")

# Joining on Host Countries

joinDf = football_df.join(cricket_df, ["Countries"])

joinDf.write.option("header", "true").csv("Join_Output")
joinDf.show()

#When the football winner and the cricket Runner-Up matched:

football_df.join(cricket_df, football_df['Football Winner'] == cricket_df['Cricket Runner-Up']).show(1)

#Difference in years between the 1st football world cup and the first Cricket World Cup:

yeardiff = int(cricket_df.select('Cricket_Year').rdd.min()[0]) - int(football_df.select('Football_Year').rdd.min()[0])
print(yeardiff)

# Number of distinct countries who have won the world cup:

distinctcount = football_df.select('Football Winner').distinct().count()

print("\nIn " + str(count) + " World Cups; there have been " + str(distinctcount) + " different Winners")


###### RDD's and DataFrames ####

footballrdd = football_df.select(football_df.columns)# Creating Football RDD
cricketrdd = cricket_df.select(cricket_df.columns)# Creating Cricket RDD

# Join

footballrdd.join(cricketrdd, "Countries").rdd.saveAsTextFile("joinop")

# Union

footballrdd.unionAll(cricketrdd).rdd.saveAsTextFile("unionop")

# distinct

footballrdd.select('Football Winner').distinct().rdd.saveAsTextFile("distinctop")

# Count

print("Total Number of World Cups: " + str(footballrdd.count()))

# Distinct Count

print("Distinct Count of Countries who have won the World Cup: " + str(footballrdd.select('Football Winner').distinct().count()))
