from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 1 second
sc = SparkContext("local[2]", "NetworkWordCount")
ssc = StreamingContext(sc, 3)

# Create a DStream that will connect to hostname:port, like localhost:9999
lines = ssc.socketTextStream("localhost", 9999)

# Split each line into words and count each word
words = lines.flatMap(lambda line: line.split(" "))\
    .map(lambda word: (len(word), word))\
    .reduceByKey(lambda a, b: a + " " + b)


# Print the first ten elements of each RDD generated in this DStream to the console
words.pprint()

ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate