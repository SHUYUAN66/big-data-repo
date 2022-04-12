#!/usr/bin/env python3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 5 seconds
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 1)

def setup_stream():
    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("localhost", 9999)

    # Split each line into words
    words = lines.window(41,1).flatMap(lambda line: line.split(" "))
    def helper(rdd):
        print('hello')
        if not rdd.isEmpty():
            res = rdd.collect()
            # split
            print(res)
            # do avg
    words.foreachRDD(helper)

    

def launch_stream(wait):
    ssc.start()                 # Start the computation
    ssc.awaitTermination(wait)  # Wait for the computation to terminate






