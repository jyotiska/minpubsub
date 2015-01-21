import unittest

class TestMongoDBPubSub(unittest.TestCase):
    def setUp(self):
        from minpubsub import create
        self.pubsub = create('mongodb', 'localhost', '27017')

    def test_pubsub(self):
        subscriber = self.pubsub.subscribe('channel')
        self.pubsub.publish('channel', 'hello')
        assert subscriber.getNext() == "hello"

    def test_multi(self):
        subscriber = self.pubsub.subscribe('channel_1', 'channel_2')
        self.pubsub.publish('channel_1', 'hi')
        self.pubsub.publish('channel_2', 'hola')
        assert subscriber.getAll() == ['hi', 'hola']

if __name__ == '__main__':
    unittest.main()
