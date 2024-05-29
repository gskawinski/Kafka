from confluent_kafka import Producer
import sys
from faker_gen import *   # import faker_generator data
import json
#import os
#import keyboard
import time, random
import pprint
    
# work_dir = os.path.dirname(os.path.realpath(__file__))

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')

# call back function        
def delivery_report(err, msg):
   if err is not None:
      print(f'Message delivery failed: {err}')
   else:
      print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

# ============= 
if __name__ == '__main__':
    if len(sys.argv) != 3:
        sys.stderr.write('Usage: %s <topic> <bootstrap-brokers>\n' % sys.argv[0])
        sys.exit(1)

    topic = sys.argv[1]
    broker = sys.argv[2]
    
    print(f'Kafka Producer config => Topic: {topic}, Broker: {broker}')
    
    # Producer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {'bootstrap.servers': broker,
            #'compression.type': 'gzip',
    }

    # Create Producer instance, as **kwargs
    p = Producer(**conf) 
    
    while True:
        # Check if the spacebar is pressed
        # print("Press the spacebar to exit the loop.")
        # if keyboard.is_pressed('space'):
        #    print("Spacebar pressed. Exiting the loop.")
        #    break
    
        # define message 
        message = generate_financial_transaction()
        #print(message)
        # serliazise data
        msg = json.dumps(message).encode('utf-8')
        
        #value = f'Hello {key}!'
        #p.produce(topic, value, key, on_delivery=callback)
        p.produce(topic, msg, on_delivery=callback)
        p.flush()
        
        # Generate a random delay between 0.5 and 2 seconds
        delay = random.uniform(0.5, 5.0)    
        # Sleep for the random delay
        time.sleep(delay)
    
    
    