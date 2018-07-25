import sys
import os

os.environ["SPARK_HOME"] = "/usr/local/Cellar/spark-2.3.1-bin-hadoop2.7"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

"""
This is use for create streaming of text from txt files that creating dynamically 
from files.py code. This spark streaming will execute in each 3 seconds and It'll
show number of words count from each files dynamically
"""


def main():
    sc = SparkContext(appName="PysparkStreaming")
    wordcount = {}
    ssc = StreamingContext(sc, 1)   #Streaming will execute in each 3 seconds
    #lines = ssc.textFileStream('log')  #'log/ mean directory name
    lines = ssc.socketTextStream("localhost", 9999)
    counts = lines. \
        flatMap(lambda line: line.split(" ")) \
        .groupByKey() \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a+b)
    #
    char_counts = lines.flatMap(lambda each: each[0]).map(lambda char: char).map(lambda c: (c, 1))\
        .reduceByKey(lambda v1, v2: v1 + v2)

    counts.pprint()
    char_counts.pprint()
    ssc.start()
    ssc.awaitTermination()


if __name__ == "__main__":
    main()
