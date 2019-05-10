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
startTime = 0
stopStop = 0
iteration = 1

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

def foo(rdd):
    x = rdd.collect()
    global startTime
    stopTime = time.time()
    print("Execution time: ", stopTime - startTime)
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
        for folder in folders :
		shutil.rmtree(folder)
        return count

def get_join_ordering(c1, c2, c3):
    arr = [[c1, 1], [c2, 2], [c3, 3]]
    arr.sort()
    return [arr[0][1], arr[1][1], arr[2][1]]

def set_topology(sc, join_ordering = [1,2,3]) :
    print("Topology: ", join_ordering)
    ssc = StreamingContext(sc, 60)
    RDDs = []
    brokers, topic1, topic2, topic3 = sys.argv[1:]
    
    topicPartion1 = TopicAndPartition(topic1, 0)
    topicPartion2 = TopicAndPartition(topic2, 0)
    topicPartion3 = TopicAndPartition(topic3, 0)
    
    offset1 = {topicPartion1: off1}
    offset2 = {topicPartion2: off2}
    offset3 = {topicPartion3: off3}
    print(offset1, offset2, offset3) 
    kvs1 = KafkaUtils.createDirectStream(ssc, [topic1], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset1)
    kvs2 = KafkaUtils.createDirectStream(ssc, [topic2], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset2)
    kvs3 = KafkaUtils.createDirectStream(ssc, [topic3], {"metadata.broker.list": brokers, "auto.offset.reset": "smallest"}, fromOffsets = offset3)
    
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
    
    RDDs.append(counts1)
    RDDs.append(counts2)
    RDDs.append(counts3)
    
    join1 = RDDs[join_ordering[0]-1].join(RDDs[join_ordering[1]-1])
    join2 = join1.join(RDDs[join_ordering[2]-1])
    #join2.pprint()
    join2.foreachRDD(foo)
    
    counts1.count().saveAsTextFiles('file:///users/nkshatri/datalogs/stream-1')
    counts2.count().saveAsTextFiles('file:///users/nkshatri/datalogs/stream-2')
    counts3.count().saveAsTextFiles('file:///users/nkshatri/datalogs/stream-3')
    
    counts1.count().pprint()
    counts2.count().pprint()
    counts3.count().pprint()
    ssc.start()
    
    return ssc


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: exp1.py <broker_list> <topic1> <topic2> <topic3>")
        exit(-1)

    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    sc.setLogLevel("ERROR")
    ssc = set_topology(sc)

    while(True):
        time.sleep(100)
        ssc.stop(stopSparkContext=False, stopGraceFully = False)
	print("STOP!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        s1_count = get_count('datalogs/stream-1')
        s2_count = get_count('datalogs/stream-2')
        s3_count = get_count('datalogs/stream-3')
        join_ordering = get_join_ordering(s1_count, s2_count, s3_count)
        ssc = set_topology(sc, join_ordering)

    ssc.awaitTermination()
