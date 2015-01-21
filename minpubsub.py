import sys
try:
    from Queue import Queue
except ImportError:
    from queue import Queue
import tempfile
from datetime import datetime


class MemorySubscriber:
    def __init__(self):
        ''' Initializes the empty queue for a particular subscriber. '''

        self.messages = Queue()

    def getNext(self):
        ''' Returns the next message available in the queue. Returns None if queue is empty. '''

        if self.messages.qsize() == 0:
            return None
        else:
            return self.messages.get(block=False, timeout=None)

    def getAll(self):
        ''' Get all the messages available in the queue, appends them in the 'item' list and returns the list. '''

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
        ''' Returns the number of messages available in the queue. '''

        return self.messages.qsize()

    def put(self, message):
        ''' Puts the message in the queue. '''

        self.messages.put_nowait(message)

class MemoryPubSub:
    ''' This class is invoked when the user chooses to store the messages queues in the memory '''

    def __init__(self):
        ''' Intializes with no subscribers available '''

        self.subscribers = {}

    def publish(self, topic, message):
        ''' When this method is invoked with a topic name and the message, it looks for all the subscribers for that particular topic and pushes the message to the subscribers. Returns the number of subscribers available. '''

        subscribers = self.subscribers.get(topic, [])
        for each_subscriber in subscribers:
            each_subscriber.put(message)
        return "Published to " + str(len(subscribers)) + " subscribers"

    def subscribe(self, *topics):
        ''' When this method is invoked with a list of topics, it creates a MemorySubscriber() object and for all the topics appends the object into the dictionary and returns the handle to the MemorySubscriber class. '''

        subscriber = MemorySubscriber()
        for topic in topics:
            subscribers = self.subscribers.setdefault(topic, [])
            subscribers.append(subscriber)
        return subscriber

class SQLiteSubscriber:
    def __init__(self, cursor, topics, timestamp):
        ''' Initializes empty queue and gets list of topics, SQLite cursor and timestamp. '''

        self.messages = Queue()
        self.timestamp = timestamp
        self.topics = list(topics)
        self.cursor = cursor

    def getNext(self):
        ''' Get the next message from the queue for given list of topics. '''

        # Get messages from the SQLite database using the subscriber's timestamp.
        for topic in self.topics:
            self.cursor.execute("SELECT message, timestamp from mps_messages WHERE topic=:topic and timestamp>:timestamp", {"topic": topic, "timestamp": self.timestamp})
            data = self.cursor.fetchall()
            for each_record in data:
                self.messages.put_nowait(each_record[0])
        # Update the timestamp
        self.timestamp = datetime.now()
        if self.messages.qsize() == 0:
            return None
        else:
            return self.messages.get(block=False, timeout=None)

    def getAll(self):
        ''' Get all messages from the queue. '''

        # For given list of topics, get all the messages from the SQLite db using subscriber's timestamp
        for topic in self.topics:
            self.cursor.execute("SELECT message, timestamp from mps_messages WHERE topic=:topic and timestamp>:timestamp", {"topic": topic, "timestamp": self.timestamp})
            data = self.cursor.fetchall()
            for each_record in data:
                self.messages.put_nowait(each_record[0])
        # Update the timestamp
        self.timestamp = datetime.now()
        items = []
        maxItemsToRetreive = self.messages.qsize()
        # Put messages in the queue
        for numOfItemsRetrieved in range(0, maxItemsToRetreive):
            try:
                if numOfItemsRetrieved == maxItemsToRetreive:
                    break
                items.append(self.messages.get_nowait())
            except Empty, e:
                break
        return items

    def getCount(self):
        ''' Returns number of available messages in the message queue. '''

        return self.messages.qsize()

    def closeTopic(self, topic):
        ''' Stop listening to a topic by its name. '''
        self.topics.remove(topic)

class SQLitePubSub:
    def __init__(self):
        ''' Intialize the package, db connectionn and the cursor. '''

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
        ''' Publish a message for a topic and store it in db. '''

        self.cursor.execute("INSERT INTO mps_messages VALUES(:topic, :message, :timestamp)", {"topic": topic, "message": message, "timestamp": datetime.now()})
        self.connection.commit()

    def subscribe(self, *topics):
        ''' Subscribe to a list of topics. '''

        # Get the timestamp of subscription
        timestamp = datetime.now()
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
        self.timestamp = datetime.now()
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
        self.timestamp = datetime.now()
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
        self.cursor.execute("INSERT INTO mps_messages VALUES(%s, %s, %s)", (topic, message, datetime.now()))

    def subscribe(self, *topics):
        timestamp = datetime.now()
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
        self.timestamp = datetime.now()
        if self.messages.qsize() == 0:
            return None
        else:
            return self.messages.get(block=False, timeout=None)

    def getAll(self):
        self.cursor = self.collection.find({'topic': {'$in': self.topics}, 'timestamp': {'$gte': self.timestamp}})
        for data in self.cursor:
            self.messages.put_nowait(data['message'])
        self.timestamp = datetime.now()
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
        self.collection.insert({'topic': topic, 'message': message, 'timestamp': datetime.now()})

    def subscribe(self, *topics):
        timestamp = datetime.now()
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

