import unittest, os.path

class TestSQLitePubSub(unittest.TestCase):
    def setUp(self):
        self.fname = 'minipubsub_test_sqlite.db'
        from minpubsub import create
        self.pubsub = create('sqlite')
        self.pubsub1 = create('sqlite', os.path.dirname(__file__), self.fname)

    def test_pubsub(self):
        subscriber = self.pubsub.subscribe('channel')
        self.pubsub.publish('channel', 'hello')
        assert subscriber.getNext() == "hello"

    def test_multi(self):
        subscriber = self.pubsub.subscribe('channel_1', 'channel_2')
        self.pubsub.publish('channel_1', 'hi')
        self.pubsub.publish('channel_2', 'hola')
        assert subscriber.getAll() == ['hi', 'hola']

    def test_pubsub1(self):
        subscriber1 = self.pubsub1.subscribe('channel')
        self.pubsub1.publish('channel', 'hello')
        assert subscriber1.getNext() == "hello"

    def test_multi1(self):
        subscriber1 = self.pubsub1.subscribe('channel_1', 'channel_2')
        self.pubsub1.publish('channel_1', 'hi')
        self.pubsub1.publish('channel_2', 'hola')
        assert subscriber1.getAll() == ['hi', 'hola']

    def test_file(self):
        assert os.path.exists(os.path.join(os.path.dirname(__file__), self.fname))

if __name__ == '__main__':
    unittest.main()
