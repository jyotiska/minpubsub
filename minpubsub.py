import sys
try:
	from Queue import Queue
except ImportError:
	from queue import Queue
import tempfile
from datetime import datetime
import time

class MemorySubscriber:
	def __init__(self):
		self.messages = Queue()

	def getNext(self):
		if self.messages.qsize() == 0:
			return None
		else:
			return self.messages.get(block=False, timeout=None)

	def getAll(self):
    		items = []
    		maxItemsToRetreive = self.messages.qsize()
		for numOfItemsRetrieved in range(0, maxItemsToRetreive):
        		try:
            			if numOfItemsRetrieved == maxItemsToRetreive:
                			break
            			items.append(self.messages.get_nowait())
        		except Empty, e:
            			break
    		return items

	def getCount(self):
		return self.messages.qsize()

	def put(self, message):
		self.messages.put_nowait(message)

class MemoryPubSub:
	def __init__(self):
		self.subscribers = {}

	def publish(self, topic, message):
		subscribers = self.subscribers.get(topic, [])
		for each_subscriber in subscribers:
			each_subscriber.put(message)
		return "Published to " + str(len(subscribers)) + " subscribers"

	def subscribe(self, *topics):
		subscriber = MemorySubscriber()
		for topic in topics:
			subscribers = self.subscribers.setdefault(topic, [])
			subscribers.append(subscriber)
		return subscriber

class SQLiteSubscriber:
	def __init__(self, cursor, topics, timestamp):
		self.messages = Queue()
		self.timestamp = timestamp
		self.topics = list(topics)
		self.cursor = cursor

	def getNext(self):
		for topic in self.topics:
			self.cursor.execute("SELECT message, timestamp from mps_messages WHERE topic=:topic and timestamp>:timestamp", {"topic": topic, "timestamp": self.timestamp})
			data = self.cursor.fetchall()
			for each_record in data:
				self.messages.put_nowait(each_record[0])
		self.timestamp = datetime.utcnow()
		if self.messages.qsize() == 0:
			return None
		else:
			return self.messages.get(block=False, timeout=None)

	def getAll(self):
		for topic in self.topics:
			self.cursor.execute("SELECT message, timestamp from mps_messages WHERE topic=:topic and timestamp>:timestamp", {"topic": topic, "timestamp": self.timestamp})
			data = self.cursor.fetchall()
			for each_record in data:
				self.messages.put_nowait(each_record[0])
		self.timestamp = datetime.utcnow()
    		items = []
    		maxItemsToRetreive = self.messages.qsize()
		for numOfItemsRetrieved in range(0, maxItemsToRetreive):
        		try:
            			if numOfItemsRetrieved == maxItemsToRetreive:
                			break
            			items.append(self.messages.get_nowait())
        		except Empty, e:
            			break
    		return items

	def getCount(self):
		return self.messages.qsize()

	def closeTopic(self, topic):
		self.topics.remove(topic)

class SQLitePubSub:
	def __init__(self):
		try:
			import sqlite3
		except ImportError:
			print "sqlite3 package could not be imported. Exiting."
			sys.exit(0)
		tempdir = tempfile.gettempdir()
		self.connection = sqlite3.connect(tempdir+'/minpubsub_sqlite.db')
		self.cursor = self.connection.cursor()
		self.cursor.execute("CREATE TABLE IF NOT EXISTS mps_messages(topic VARCHAR(100), message VARCHAR(1000), timestamp VARCHAR(100))")

	def publish(self, topic, message):
		time.sleep(0.001)
		self.cursor.execute("INSERT INTO mps_messages VALUES(:topic, :message, :timestamp)", {"topic": topic, "message": message, "timestamp": datetime.utcnow()})
		self.connection.commit()
	def subscribe(self, *topics):
		timestamp = datetime.utcnow()
		subscriber = SQLiteSubscriber(self.cursor, topics, timestamp)
		return subscriber

class MySQLSubscriber:
	def __init__(self, cursor, topics, timestamp):
		self.messages = Queue()
		self.timestamp = timestamp
		self.topics = list(topics)
		self.cursor = cursor

	def getNext(self):
		for topic in self.topics:
			self.cursor.execute("SELECT message, timestamp from mps_messages WHERE topic=%s and timestamp>%s", (topic, self.timestamp))
			data = self.cursor.fetchall()
			for each_record in data:
				self.messages.put_nowait(each_record[0])
		self.timestamp = datetime.utcnow()
		if self.messages.qsize() == 0:
			return None
		else:
			return self.messages.get(block=False, timeout=None)

	def getAll(self):
		for topic in self.topics:
			self.cursor.execute("SELECT message, timestamp from mps_messages WHERE topic=%s and timestamp>%s", (topic, self.timestamp))
			data = self.cursor.fetchall()
			for each_record in data:
				self.messages.put_nowait(each_record[0])
		self.timestamp = datetime.utcnow()
    		items = []
    		maxItemsToRetreive = self.messages.qsize()
		for numOfItemsRetrieved in range(0, maxItemsToRetreive):
        		try:
            			if numOfItemsRetrieved == maxItemsToRetreive:
                			break
            			items.append(self.messages.get_nowait())
        		except Empty, e:
            			break
    		return items

	def getCount(self):
		return self.messages.qsize()

	def closeTopic(self, topic):
		self.topics.remove(topic)

class MySQLPubSub:
	def __init__(self, *argv):
		try:
			import MySQLdb
		except ImportError:
			print "MySQLdb package could not be imported. Exiting."
			sys.exit(0)
		try:
			self.connection = MySQLdb.connect(argv[0], argv[1], argv[2], argv[3])
			self.cursor = self.connection.cursor()
			self.cursor.execute("SHOW TABLES LIKE 'mps_messages'")
			table_data = self.cursor.fetchall()
			available_tables = []
			for each_item in table_data:
				available_tables.append(each_item[0])
			if "mps_messages" not in available_tables:
				self.cursor.execute("CREATE TABLE IF NOT EXISTS mps_messages(topic VARCHAR(100), message VARCHAR(1000), timestamp VARCHAR(100))")
		except:
			print "Error connecting to MySQL database"
			sys.exit(0)

	def publish(self, topic, message):
		time.sleep(0.001)
		self.cursor.execute("INSERT INTO mps_messages VALUES(%s, %s, %s)", (topic, message, datetime.utcnow()))
	def subscribe(self, *topics):
		timestamp = datetime.utcnow()
		subscriber = MySQLSubscriber(self.cursor, topics, timestamp)
		return subscriber

class MongoDBSubscriber:
	def __init__(self, collection, topics, timestamp):
		self.messages = Queue()
		self.collection = collection
		self.topics = list(topics)
		self.timestamp = timestamp

	def getNext(self):
		self.cursor = self.collection.find({'topic': {'$in': self.topics}, 'timestamp': {'$gte': self.timestamp}})
		for data in self.cursor:
			self.messages.put_nowait(data['message'])
		self.timestamp = datetime.utcnow()
		if self.messages.qsize() == 0:
			return None
		else:
			return self.messages.get(block=False, timeout=None)

	def getAll(self):
		self.cursor = self.collection.find({'topic': {'$in': self.topics}, 'timestamp': {'$gte': self.timestamp}})
		for data in self.cursor:
			self.messages.put_nowait(data['message'])
		self.timestamp = datetime.utcnow()
    		items = []
    		maxItemsToRetreive = self.messages.qsize()
		for numOfItemsRetrieved in range(0, maxItemsToRetreive):
        		try:
            			if numOfItemsRetrieved == maxItemsToRetreive:
                			break
            			items.append(self.messages.get_nowait())
        		except Empty, e:
            			break
    		return items

	def getCount(self):
		return self.messages.qsize()

	def closeTopic(self, topic):
		self.topics.remove(topic)

class MongoDBPubSub:
	def __init__(self, *argv):
		try:
			import pymongo
		except ImportError:
			print "Pymongo package could not be imported. Exiting."
			sys.exit(0)
		try:
			self.connection = pymongo.Connection(argv[0], int(argv[1]))
			self.db = self.connection.minpubsub
			self.collection = self.db['mps_messages']
		except:
			print "Failed to connect to Mongo database. Exiting."
			sys.exit(0)

	def publish(self, topic, message):
		time.sleep(0.001)
		self.collection.insert({'topic': topic, 'message': message, 'timestamp': datetime.utcnow()})

	def subscribe(self, *topics):
		timestamp = datetime.utcnow()
		subscriber = MongoDBSubscriber(self.collection, topics, timestamp)
		return subscriber

def create(name, *argv):
	if name == 'memory':
		handler = MemoryPubSub()
		return handler
	elif name == 'sqlite':
		handler = SQLitePubSub()
		return handler
	elif name == 'mysql':
		handler = MySQLPubSub(*argv)
		return handler
	elif name == 'mongo' or name == 'mongodb':
		handler = MongoDBPubSub(*argv)
		return handler
	else:
		print "Option not found! Exiting."
		sys.exit(0)

