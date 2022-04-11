#!/usr/bin/env python3

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Create a local StreamingContext with two working thread and batch interval of 5 seconds
sc = SparkContext.getOrCreate()
ssc = StreamingContext(sc, 0.25)

def setup_stream():
    # Create a DStream that will connect to hostname:port, like localhost:9999
    lines = ssc.socketTextStream("localhost", 9999)

    # Split each line into words
    words = lines.flatMap(lambda line: line.split(" "))

    # Count each word in each batch
    pairs = words.map(lambda word: (word, 1))
    priceSum = pairs.reduceByWindow(
    lambda x, y: ((float(x[0])+float(y[0]))/(int(x[1])+int(y[1])), (int(x[1])+int(y[1]))),
    None,
    windowDuration=2.5,
    slideDuration=0.25)

    # Print the first ten elements of each RDD generated in this DStream to the console
    priceSum.pprint()

def launch_stream(wait):
    ssc.start()                 # Start the computation
    ssc.awaitTermination(wait)  # Wait for the computation to terminate






