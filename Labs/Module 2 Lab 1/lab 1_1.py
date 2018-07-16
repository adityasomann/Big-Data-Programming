
import os
os.environ["SPARK_HOME"] = "/usr/local/Cellar/spark-2.3.1-bin-hadoop2.7"

from pyspark import SparkContext

def friendmap(value):

    value = value.split(" ")
    user = value[0]
    friends = value[1]
    keys = []

    for friend in friends:
        keys.append((''.join(sorted(user+friend)), friends.replace(friend, "")))

    return keys


def friendreduce(key, value):
    reducer = ''
    for friend in key:
        if friend in value:
            reducer += friend
    return reducer


if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    Lines = sc.textFile("/Users/aditya/Downloads/lab21.csv", 1)
    Line = Lines.flatMap(friendmap)
    Commonfriends = Line.reduceByKey(friendreduce)
    Commonfriends.coalesce(1).saveAsTextFile("CommonFriendsOutput")
    sc.stop()

