minpubsub
=========

A minimal PubSub based distributed message queue model with multiple backend persistence modes - SQLite, MySQL, MongoDB.


Usage
------

Create a pubsub object::

    >> from minpubsub import create
    >> pubsub = create('memory') # for in-memory message queue
    >> pubsub = create('sqlite') # for sqlite based message queue
    >> pubsub = create('mysql', '<host>', '<username>', '<password>', '<schema>') # for mysql based message queue
    >> pubsub = create('mongodb', '<host>', '<port>') # for mongodb based message queue

Subscribe to a Topic::

    >> subscriber = pubsub.subscribe('topic') # for subscribing to a single topic
    >> subscriber = pubsub.subscribe('topic_1', 'topic_2', 'topic_3') # for subscribing to multiple topics

Publish a message to a topic::

    >> pubsub.publish('topic_1', 'hello world!')

Retrieve a message from the message queue::

    >> subscriber.getNext()

Retrieve all messages from the message queue::

    >> subscriber.getAll()

Retrieve number of messages available in the message queue::

    >> subscriber.getCount()

Stop listening to a Topic (not available for in-memory message queue) ::

    >> subscriber.closeTopic('<topic_name>')

Supported backends
---------------------

* memory
* sqlite
* mysql
* mongodb
