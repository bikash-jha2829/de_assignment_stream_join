import struct

import confluent_kafka

# Default test conf location
testconf_file = "tests/testconf.json"

# Kafka bootstrap server(s)
bootstrap_servers = None

# Confluent schema-registry
schema_registry_url = None

# Topic prefix to use
topic = None

# API version requests are only implemented in Kafka broker >=0.10
# but the client handles failed API version requests gracefully for older
# versions as well, except for 0.9.0.x which will stall for about 10s
# on each connect with this set to True.
api_version_request = True

# global variable to be set by stats_cb call back function
good_stats_cb_result = False

# global counter to be incremented by throttle_cb call back function
throttled_requests = 0

# global variable to track garbage collection of suppressed on_delivery callbacks
DrOnlyTestSuccess_gced = 0

# Shared between producer and consumer tests and used to verify
# that consumed headers are what was actually produced.
produce_headers = [('foo1', 'bar'),
                   ('foo1', 'bar2'),
                   ('foo2', b'1'),
                   (u'Jämtland', u'Härjedalen'),  # automatically utf-8 encoded
                   ('nullheader', None),
                   ('empty', ''),
                   ('foobin', struct.pack('hhl', 10, 20, 30))]

# Identical to produce_headers but with proper binary typing
expected_headers = [('foo1', b'bar'),
                    ('foo1', b'bar2'),
                    ('foo2', b'1'),
                    (u'Jämtland', b'H\xc3\xa4rjedalen'),  # not automatically utf-8 decoded
                    ('nullheader', None),
                    ('empty', b''),
                    ('foobin', struct.pack('hhl', 10, 20, 30))]


def print_commit_result(err, partitions):
    if err is not None:
        print('# Failed to commit offsets: %s: %s' % (err, partitions))
    else:
        print('# Committed offsets for: %s' % partitions)


class MyTestDr(object):
    """ Producer: Delivery report callback """

    def __init__(self, silent=False):
        super(MyTestDr, self).__init__()
        self.msgs_delivered = 0
        self.bytes_delivered = 0
        self.silent = silent

    @staticmethod
    def _delivery(err, msg, silent=False):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return 0
        else:
            if not silent:
                print('Message delivered to %s [%s] at offset [%s] in %.3fs: %s' %
                      (msg.topic(), msg.partition(), msg.offset(),
                       msg.latency(), msg.value()))
            return 1

    def delivery(self, err, msg):
        if err:
            print('Message delivery failed (%s [%s]): %s' %
                  (msg.topic(), str(msg.partition()), err))
            return
        elif not self.silent:
            print('Message delivered to %s [%s] at offset [%s] in %.3fs: %s' %
                  (msg.topic(), msg.partition(), msg.offset(),
                   msg.latency(), msg.value()))
        self.msgs_delivered += 1
        self.bytes_delivered += len(msg)


def verify_producer():
    """ Verify basic Producer functionality """

    # Producer config
    conf = {'bootstrap.servers': bootstrap_servers}

    # Create producer
    p = confluent_kafka.Producer(conf)
    print('producer at %s' % p)

    headers = produce_headers

    # Produce some messages
    p.produce(topic, 'Hello Python!', headers=headers)
    p.produce(topic, key='Just a key and headers', headers=headers)
    p.produce(topic, key='Just a key')
    p.produce(topic, partition=1, value='Strictly for partition 1',
              key='mykey', headers=headers)

    # Produce more messages, now with delivery report callbacks in various forms.
    mydr = MyTestDr()
    p.produce(topic, value='This one has a dr callback',
              callback=mydr.delivery)
    p.produce(topic, value='This one has a lambda',
              callback=lambda err, msg: MyTestDr._delivery(err, msg))
    p.produce(topic, value='This one has neither')

    # Try producing with a timestamp
    try:
        p.produce(topic, value='with a timestamp', timestamp=123456789000)
    except NotImplementedError:
        if confluent_kafka.libversion()[1] >= 0x00090400:
            raise

    # Produce even more messages
    for i in range(0, 10):
        p.produce(topic, value='Message #%d' % i, key=str(i),
                  callback=mydr.delivery)
        p.poll(0)

    print('Waiting for %d messages to be delivered' % len(p))

    # Block until all messages are delivered/failed
    p.flush()


# Global variable to track garbage collection of suppressed on_delivery callbacks
DrOnlyTestSuccess_gced = 0


def verify_consumer():
    """ Verify basic Consumer functionality """

    # Consumer config
    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': 'test.py',
            'session.timeout.ms': 6000,
            'enable.auto.commit': False,
            'on_commit': print_commit_result,
            'auto.offset.reset': 'earliest',
            'enable.partition.eof': True}

    # Create consumer
    c = confluent_kafka.Consumer(conf)

    def print_wmark(consumer, topic_parts):
        # Verify #294: get_watermark_offsets() should not fail on the first call
        #              This is really a librdkafka issue.
        for p in topic_parts:
            wmarks = consumer.get_watermark_offsets(topic_parts[0])
            print('Watermarks for %s: %s' % (p, wmarks))

    # Subscribe to a list of topics
    c.subscribe([topic], on_assign=print_wmark)

    max_msgcnt = 100
    msgcnt = 0

    first_msg = None

    example_headers = None

    eof_reached = dict()

    while True:
        # Consume until EOF or error

        # Consume message (error()==0) or event (error()!=0)
        msg = c.poll()
        if msg is None:
            raise Exception('Got timeout from poll() without a timeout set: %s' % msg)

        if msg.error():
            if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                print('Reached end of %s [%d] at offset %d' %
                      (msg.topic(), msg.partition(), msg.offset()))
                eof_reached[(msg.topic(), msg.partition())] = True
                if len(eof_reached) == len(c.assignment()):
                    print('EOF reached for all assigned partitions: exiting')
                    break
            else:
                print('Consumer error: %s: ignoring' % msg.error())
                break

        tstype, timestamp = msg.timestamp()
        headers = msg.headers()
        if headers:
            example_headers = headers

        msg.set_headers([('foo', 'bar')])
        assert msg.headers() == [('foo', 'bar')]

        print('%s[%d]@%d: key=%s, value=%s, tstype=%d, timestamp=%s headers=%s' %
              (msg.topic(), msg.partition(), msg.offset(),
               msg.key(), msg.value(), tstype, timestamp, headers))

        if first_msg is None:
            first_msg = msg

        if (msgcnt == 11):
            parts = c.assignment()
            print('Pausing partitions briefly')
            c.pause(parts)
            exp_None = c.poll(timeout=2.0)
            assert exp_None is None, "expected no messages during pause, got %s" % exp_None
            print('Resuming partitions')
            c.resume(parts)

        if (msg.offset() % 5) == 0:
            # Async commit
            c.commit(msg, asynchronous=True)
        elif (msg.offset() % 4) == 0:
            offsets = c.commit(msg, asynchronous=False)
            assert len(offsets) == 1, 'expected 1 offset, not %s' % (offsets)
            assert offsets[0].offset == msg.offset() + 1, \
                'expected offset %d to be committed, not %s' % \
                (msg.offset(), offsets)
            print('Sync committed offset: %s' % offsets)

        msgcnt += 1
        if msgcnt >= max_msgcnt and example_headers is not None:
            print('max_msgcnt %d reached' % msgcnt)
            break

    assert example_headers, "We should have received at least one header"
    assert example_headers == expected_headers, \
        "example header mismatch:\n{}\nexpected:\n{}".format(example_headers, expected_headers)

    # Get current assignment
    assignment = c.assignment()

    # Get cached watermark offsets
    # Since we're not making use of statistics the low offset is not known so ignore it.
    lo, hi = c.get_watermark_offsets(assignment[0], cached=True)
    print('Cached offsets for %s: %d - %d' % (assignment[0], lo, hi))

    # Query broker for offsets
    lo, hi = c.get_watermark_offsets(assignment[0], timeout=1.0)
    print('Queried offsets for %s: %d - %d' % (assignment[0], lo, hi))

    # Query offsets for timestamps by setting the topic partition offset to a timestamp. 123456789000 + 1
    topic_partions_to_search = list(map(lambda p: confluent_kafka.TopicPartition(topic, p, 123456789001), range(0, 3)))
    print("Searching for offsets with %s" % topic_partions_to_search)

    offsets = c.offsets_for_times(topic_partions_to_search, timeout=1.0)
    print("offsets_for_times results: %s" % offsets)

    # Close consumer
    c.close()

    # Start a new client and get the committed offsets
    c = confluent_kafka.Consumer(conf)
    offsets = c.committed(list(map(lambda p: confluent_kafka.TopicPartition(topic, p), range(0, 3))))
    for tp in offsets:
        print(tp)

    c.close()
