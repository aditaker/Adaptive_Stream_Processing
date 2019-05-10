import sys
import time
import threading
from kafka import KafkaProducer

def foo():
	global f, count, filename, producer, i
	print("S", time.ctime())
	while True:
		line = f.readline()
		if not line:
			f = open(filename)
		#print(line)
		producer.send(topic, line)
		i += 1
		if i == count:
			print("E", time.ctime())
			i = 0
			return

if __name__ == "__main__":
	if len(sys.argv) != 4:
		print("Usage: line-server.py <line-count/20-sec> <file> <topic>")
		sys.exit(-1)
	print("Connected")
	print(time.ctime())
	global count, filename, f, producer, topic, i
	i = 0
	count = int(sys.argv[1])
	filename = sys.argv[2]
	topic = sys.argv[3]
	f = open(filename)
	producer = KafkaProducer(bootstrap_servers='localhost:9092')
	while True:
		time.sleep(2)
		while True:
			processThread = threading.Thread(target=foo, args=())
			processThread.start()
			time.sleep(20)
