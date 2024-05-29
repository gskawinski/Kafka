from confluent_kafka import Consumer, KafkaError, KafkaException
import sys


if __name__ == '__main__':
    # Parse the command line
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <topic> <bootstrap-brokers>\n' % sys.argv[0])
        sys.exit(1)

    topic = sys.argv[1]
    broker = sys.argv[2]
    
    print(f'Kafka Consumer config => Topic: {topic}, Broker: {broker}')
    
     # Parse the configuration.
    conf = {"bootstrap.servers": "localhost:9092", 
            "group.id": 'main', 
            "session.timeout.ms": 6000,
            "auto.offset.reset": 'earliest'
    }
    # Create Consumer instance
    c = Consumer(conf)
    # Subscribe to topic
    c.subscribe([topic])
    
    # Poll for new messages from Kafka and proces them.
    try:
        while True:
            msg = c.poll(timeout=1.0)
    
            if msg is None:
                print("Waiting...")
                continue
            if msg.error(): 
                print(f"ERROR: {msg.error()}".format(msg.error()))
                raise KafkaException(msg.error())
            else:
                print(f'{msg.topic()} [{msg.partition()}] at offset {msg.offset()} with key {str(msg.key())}:\n')

                print(msg.value())
                val = msg.value().decode('utf8')
                
                topic_recived  = msg.topic()
                part_ = msg.partition()
                offset = msg.offset()
                key = msg.key()
                value = msg.value().decode('utf-8')
                print(f"Consumed event from topic {topic_recived}: key = {key} value = {value}")
    except Exception as e:
        import traceback
        print(traceback.format_exc())
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        c.close()
