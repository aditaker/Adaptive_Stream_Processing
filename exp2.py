#from __future__ import print_function

import sys
import time
import glob
import os
import shutil 
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.streaming.kafka import TopicAndPartition

off1 = 0
off2 = 0
off3 = 0
off4 = 0
startTime = 0
stopStop = 0
iteration = 1

runtime = 0

def storeOffsetRanges1(rdd):
    global off1
    global startTime
    global iteration
    startTime = time.time()
    off1 = rdd.offsetRanges()[-1].untilOffset
    print("Iteration: ", iteration)
    iteration += 1
    #print("Offset1 ", off1)
    return rdd

def storeOffsetRanges2(rdd):
    global off2
    off2 = rdd.offsetRanges()[-1].untilOffset
    #print("Offset2 ", off2)
    return rdd

def storeOffsetRanges3(rdd):
    global off3
    off3 = rdd.offsetRanges()[-1].untilOffset
    #print("Offset3 ", off3)
    return rdd

def storeOffsetRanges4(rdd):
    global off4
    off4 = rdd.offsetRanges()[-1].untilOffset
    #print("Offset3 ", off3)
    return rdd

def foo(rdd):
    x = rdd.collect()
    global startTime, runtime
    stopTime = time.time()
    runtime = stopTime - startTime
    print("Execution time: ", runtime)
    return rdd

def get_count(path):
        folders = glob.glob(path+'*')
        all_files = []
        for folder in folders :
                files = glob.glob(os.path.join(folder,'part*'))
                all_files += files
#       print(all_files)
        count = 0
        for filename in all_files:
                with open(filename) as f:
                        for l in f:
                                count += int(l)
        #print(count)
	shutil.rmtree(folders)
        return count

def get_join_ordering(c1, c2, c3):
    arr = [[c1, 1], [c2, 2], [c3, 3]]
    arr.sort()
    return [arr[0][1], arr[1][1], arr[2][1]]

def set_topology(sc, join_ordering = [1,2,3]) :
    print("Topology: ", join_ordering)
    ssc = StreamingContext(sc, 25)
    RDDs = []
    brokers, topic1, topic2, topic3, topic4 = sys.argv[1:]
    
    topicPartion1 = TopicAndPartition(topic1, 0)
    topicPartion2 = TopicAndPartition(topic2, 0)
    topicPartion3 = TopicAndPartition(topic3, 0)
    topicPartion4 = TopicAndPartition(topic4, 0)
    
    offset1 = {topicPartion1: off1}
    offset2 = {topicPartion2: off2}
    offset3 = {topicPartion3: off3}
    offset4 = {topicPartion4: off4}
    print(offset1, offset2, offset3, offset4) 
    kvs1 = KafkaUtils.createDirectStream(ssc, [topic1], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset1)
    kvs2 = KafkaUtils.createDirectStream(ssc, [topic2], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset2)
    kvs3 = KafkaUtils.createDirectStream(ssc, [topic3], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset3)
    kvs4 = KafkaUtils.createDirectStream(ssc, [topic4], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset4)
    
    lines1 = kvs1.transform(storeOffsetRanges1) \
   	.map(lambda x: x[1])
    
    counts1 = lines1.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
    
    #counts1.pprint()
    
    lines2 = kvs2.transform(storeOffsetRanges2) \
   	.map(lambda x: x[1])
    
    counts2 = lines2.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
    
    #counts2.pprint()
    
    lines3 = kvs3.transform(storeOffsetRanges3) \
   	.map(lambda x: x[1])
    
    counts3 = lines3.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
    
    #counts3.pprint()
    
    lines4 = kvs4.transform(storeOffsetRanges4) \
   	.map(lambda x: x[1])
    
    counts4 = lines4.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b) \
    
    #counts4.pprint()
    
    RDDs.append(counts1)
    RDDs.append(counts2)
    RDDs.append(counts3)
    RDDs.append(counts4)
    
    join1 = RDDs[join_ordering[0]-1].join(RDDs[join_ordering[1]-1])
    join2 = join1.join(RDDs[join_ordering[2]-1])
    join3 = join2.join(RDDs[join_ordering[3]-1])
    join3.foreachRDD(foo)
    
    ssc.start()
    
    return ssc

def evaluate_qp(sc, join_orderings):
    smallestIndex = -1
    smallestTime = sys.maxint
    global runtime
    for i, join_ordering in enumerate(join_orderings):
        print("Join Order: ", join_ordering)
        ssc = set_topology(sc, join_ordering)
        time.sleep(27)
        ssc.stop(stopSparkContext=False, stopGraceFully = False)
	print("Stop")
	if smallestTime > runtime:
	    smallestTime = runtime
	    smallestIndex = i
    print(smallestTime, smallestIndex)
    

if __name__ == "__main__":
    if len(sys.argv) != 6:
        print("Usage: exp2.py <broker_list> <topic1> <topic2> <topic3> <topic6>")
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    sc.setLogLevel("ERROR")
    join_orderings = [[1, 2, 3, 4], [1, 3, 2, 4], [2, 3, 1, 4], [4, 2, 1, 3]]
    evaluate_qp(sc, join_orderings)
    ssc.awaitTermination()
