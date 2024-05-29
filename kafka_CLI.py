import subprocess
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer, KafkaError
from confluent_kafka import KafkaException, TopicCollection
import time

def start_kafka_server():
    """ 
    Start the Kafka server using the systemctl command.
    """
    # Specify the systemctl command to start the Kafka service
    systemctl_command = "sudo systemctl start kafka"

    # Execute the systemctl command
    try:
        subprocess.run(systemctl_command, shell=True, check=True)
        print("Kafka server started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error starting Kafka server: {e}")

def stop_kafka_server():
    """ 
    Stop the Kafka server using the systemctl command.
    """
    # Specify the systemctl command to stop the Kafka service
    systemctl_command = "sudo systemctl stop kafka"

    # Execute the systemctl command
    try:
        subprocess.run(systemctl_command, shell=True, check=True)
        print("Kafka server stopped successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error stopping Kafka server: {e}")

def check_kafka_status():
    """ 
    Check the Kafka server status.
    """
    # Specify the systemctl command to check the Kafka service status
    systemctl_command = "sudo systemctl status kafka | grep 'Active:'"

    # Execute the systemctl command and capture the output
    try:
        output = subprocess.check_output(systemctl_command, shell=True, universal_newlines=True)
        
        # Check if the output contains "Active: active (running)"
        if "Active: active (running)" in output:
            print("Kafka server is active (running).")
        else:
            print("Kafka server is not active.")
    except subprocess.CalledProcessError as e:
        print(f"Error checking Kafka server status: {e}")

def list_topics(admin_client):
    """
    List existing topics.
    Parameters:
    - admin_client: Kafka AdminClient instance.
    Prints topics in the format: id: name
    """
    try:
        # List existing topics
        topics_data = admin_client.list_topics(timeout=5)
        topics = topics_data.topics

        if not topics:
            print("No topics found.")
            return

        topic_list = []
        print("Existing topics:\n-----------")
        for topic, topic_metadata in topics.items():
            topic_info = {
                #'id': topic_metadata.topic,
                'name': topic,
            }
            topic_list.append(topic_info)
            #print(f"ID: {topic_metadata.topic}, Topic Name: {topic}")
            print(f"Topic Name: {topic}")

        return topic_list
    
    except KafkaException as e:
        print(f"Error listing topics: {e}")
        return None
    

def topic_exists(admin, topic):
    """
    Check if a specific Kafka topic already exists.
    Parameters:
    - admin (object): KafkaAdminClient instance used for topic management.
    - topic (str): Name of the Kafka topic to check.
    Returns:
    - bool: True if the topic exists, False if it does not.
    """
    metadata = admin.list_topics()
    for t in iter(metadata.topics.values()):
        if t.topic == topic:
            return True
    return False

# create new topic and return results dictionary
def create_topic(admin, topic, partitions = 1, replicas=1):
    """
    Create a new Kafka topic and return the results dictionary.
    Parameters:
    - admin (object): KafkaAdminClient instance used for topic management.
    - topic (str): Name of the new Kafka topic to create.
    - partitions (int, optional): Number of partitions for the new topic (default is 1).
    - replicas (int, optional): Number of replicas for the new topic (default is 1).
    Returns:
    - dict: A dictionary containing the results of the topic creation operation.
    """
    if topic_exists(admin, topic):
        print(f"Topic '{topic}' already exists.")
        return

    # Create a new topic with parameters
    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replicas) 
    result_dict = admin.create_topics([new_topic])

    for topic, future in result_dict.items():
        try:
            future.result()  # The result itself is None
            #print("Topic {} created".format(topic))
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print("Failed to create topic {}: {}".format(topic, e))

def delete_topic(admin_client, topic_name):
    """
    Delete a Kafka topic.(asynchronously)
    Parameters:
    - admin_client: Kafka AdminClient instance.
    - topic_name: Name of the topic to be deleted.
    """
    
    # Check if the topic exists
    if not topic_exists(admin_client, topic_name):
        print(f"Topic '{topic_name}' does not exist.")
        return

    # Delete the topic
    fs = admin_client.delete_topics([topic_name], operation_timeout=20.0)
    # Returns a dict of <topic,future>.

    # Wait for operation to finish.
    for topic, f in fs.items():
        try:
            f.result()  # The result itself is None
            print("Topic {} deleted".format(topic))
        except Exception as e:
            print("Failed to delete topic {}: {}".format(topic, e))

def describe_topic(a, topic_names):
    """
    Describe a Kafka topic and return information in a dictionary.
    Parameters:
    - admin_client: Kafka AdminClient instance.
    - topic_name: Name of the topic to be described.
    Returns:
    A dictionary containing information about the topic.
    """
    topics = TopicCollection(topic_names)
    futureMap = a.describe_topics(topics, request_timeout=10)

    for topic_name, future in futureMap.items():
        try:
            # Extract information from topic metadata
            print("Topic Information:")
            t = future.result()
            print("Topic name             : {}".format(t.name))
            print("Topic id               : {}".format(t.topic_id))
            print("Partitions             : {}".format(len(t.partitions)))
            #print(f"Partitions: {len(t.partitions)}")
            #print(f"Replication Factor: {t.replication_factor}")
            print("Partition Information")
            for partition in t.partitions:
                #print("    Id                : {}".format(partition.id))
                leader = partition.leader
                print("    Replicas          : {}".format(len(partition.replicas)))
                print(f"    Leader            : {leader}")
            print("")

        except Exception as e:
            print("Error while describing topic '{}': {}".format(topic_name, e))
            raise

# Producer Delivery Callback
def callback_report(err, event):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
        return
    else:
        max_length = 10

        val = event.value().decode('utf8')[:max_length] 
        key = event.key().decode('utf8')
        print(f'Message: {val} ; with Key: {key} ; produced to Topic:{event.topic()}  Part[{event.partition()}] at offset {event.offset()}.')


def write_to_topic(producer, topic_name, message, key):
    """
    Produce a message to a specified Kafka topic using a provided Kafka producer instance.
    Parameters:
    - producer (confluent_kafka.Producer): The Kafka producer instance.
    - topic_name (str): The name of the Kafka topic to which the message will be sent.
    - message (str): The message content to be sent to the Kafka topic.
    - key (str): The key associated with the Kafka message.
    Returns: None
    Usage Example:
    write_to_topic(producer, "example_topic", "Hello, Kafka!", key="example_key")
    """
    try:
        # Produce a message to the specified topic
        producer.produce(topic=topic_name, value=message, key=key, callback=callback_report)
        producer.flush()
    except Exception as e:
        # Handle exceptions raised during message production
        print(f"Error during message production: {e}")


#  whenever topic partitions are assigned to consumer. 
def assignment_callback(consumer, partitions):
    """ 
    Callback function invoked when a Kafka consumer is assigned to partitions.
    Parameters:
    - consumer (confluent_kafka.Consumer): The Kafka consumer instance.
    - partitions (list): List of assigned partitions.
    Returns: None
    """
    for p in partitions:
        print(f'Assigned to {p.topic}, partition {p.partition}')


def read_topic(consumer, topic_name):
    # Consume messages from the specified topic
    consumer.subscribe([topic_name])

    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        print(f"Received message: {msg.value().decode('utf-8')}")


from faker import Faker
import random

# random Poem generator/iterantor 
def poem_generator():
    fake = Faker()
    
    poem_types = ['Haiku', 'Sonnet', 'FreeVerse', 'Limerick']
    
    while True:
        poem_type = random.choice(poem_types)
        poem_message = fake.sentence(nb_words=random.randint(5, 15))
        
        yield {
            'type': poem_type,
            'message': poem_message
        }

import json

def serialize_poem(poem_dict):
    """
    Serialize a poem dictionary to a JSON-formatted string.

    Parameters:
    - poem_dict (dict): The dictionary representing a poem.

    Returns:
    str: JSON-formatted string.
    """
    return json.dumps(poem_dict)

def deserialize_poem(json_string):
    """
    Deserialize a JSON-formatted string to a poem dictionary.

    Parameters:
    - json_string (str): JSON-formatted string.

    Returns:
    dict: Dictionary representing a poem.
    """
    return json.loads(json_string)


import avro.schema
from avro.io import DatumReader, DatumWriter, BinaryEncoder, BinaryDecoder
from io import BytesIO

# Avro serialization
def serialize_avro(data, schema):
    bytes_io = BytesIO()
    writer = DatumWriter(schema)
    encoder = BinaryEncoder(bytes_io)
    writer.write(data, encoder)
    avro_serialized_data = bytes_io.getvalue()
    # print(f"Avro Serialized Data: {bytes_io.getvalue()}")
    return avro_serialized_data


# Avro deserialization
def deserialize_avro(data, schema):
    reader = DatumReader(schema)
    bytes_io = BytesIO(data)
    decoder = BinaryDecoder(bytes_io)
    return reader.read(decoder)

# serialization based on the type..
def serialize_function(topic, serialization_option, data):
        
    # Check if the topic is empty and assign a default value if necessary
    # Strip leading and trailing whitespace, assign a default value if empty

    if serialization_option == 'n':
             
        if not topic:
            topic_deafult = 'gnone'
            topic = topic_deafult
            key = data["type"]
            value = data["message"]

    elif serialization_option == 's':
             
        if not topic:
            topic_deafult = 'gjson'
            topic = topic_deafult
            key = serialize_poem(data['type'])
            value = serialize_poem(data)
            
    elif serialization_option == 'a':
 
        if not topic:
            topic_deafult = 'gavro'
            topic = topic_deafult

            # Avro schema for the Poem
            poem_schema_str = """
            {
            "type": "record",
            "name": "Poem",
            "fields": [
                {"name": "type", "type": "string"},
                {"name": "message", "type": "string"}
            ]
            }
            """
            poem_avro_schema = avro.schema.parse(poem_schema_str)

            key = serialize_poem(data['type'])
            value = serialize_avro(data, poem_avro_schema)

    else:
        topic_default = 'None'
        print("Error")
        return None, None, None
    
    return topic, key, value


def deserialize_function(deserialization_option, event):
        
    # Check if the topic is empty and assign a default value if necessary
    # Strip leading and trailing whitespace, assign a default value if empty

                        #val = event.value().decode('utf8')
                        #key = event.key().decode('utf8')
                        #key, val = deserialize_function(deserialization_option, event)

    if deserialization_option == 'n':
        key = event.key().decode('utf8')
        value = event.value().decode('utf8')

    elif deserialization_option == 's':
        key = deserialize_poem( event.key().decode('utf8') )
        value = deserialize_poem( event.value().decode('utf8') )
            
    elif deserialization_option == 'a':
        # Avro schema for the Poem
        poem_schema_str = """
            {
            "type": "record",
            "name": "Poem",
            "fields": [
                {"name": "type", "type": "string"},
                {"name": "message", "type": "string"}
            ]
            }
            """
        poem_avro_schema = avro.schema.parse(poem_schema_str)

        key = deserialize_poem( event.key().decode('utf8') )
        value = deserialize_avro( event.value() , poem_avro_schema)
        
    else:
        print("Error")
        return None, None
    
    return key, value


# ============ Main =================
def main():

    # Set up AdminClient for topic management
    admin_config = {
        "bootstrap.servers": "localhost:9092"
    }
    admin_client = AdminClient(admin_config)

    while True:
        print("\nSelect an option:")
        print("1. Start Kafka server")
        print("2. Stop Kafka server")
        print("3. Check Kafka server status")
        print("4. List topics")
        print("5. Create topic")
        print("6. Delete topic")
        print("7. Describe topic")
        print("8. Write to topic")
        print("9. Read from topic")
        print("-1. Exit")

        choice = input("Enter your choice (1-9): ")

        if choice == "1":
            start_kafka_server()
        elif choice == "2":
            stop_kafka_server()
        elif choice == "3":
            check_kafka_status()
        elif choice == "4":
            list_topics(admin_client)
        elif choice == "5":
            # Create Topic input prompt
            user_input = input("Enter topic, partitions, and replicas (comma-separated): ")
            # Parse user input
            topic_name, partitions, replicas = map(str.strip, user_input.split(','))
            # Convert partitions and replicas to integers
            partitions = int(partitions)
            replicas = int(replicas)  # number of replicas depends on the broker number = 1 local
            create_topic(admin_client, topic_name, partitions, replicas )
        elif choice == "6":
            topic_name = input("Enter the topic name to delete: ")
            delete_topic(admin_client, topic_name)
        elif choice == "7":
            topic_name = input("Enter the topic name to describe: ")
            describe_topic(admin_client, [topic_name])

        elif choice == "8":
            # Get user input for serialisation method
            serialization_option = input("Choose serialization option:\nn - None\ns - JSON\na - Avro\nEnter serialization option: ")

            # Get user input for the topic name , strip leading and trailing whitespace
            topic_name = input("Enter the topic name (Enter for Default): ").strip()

            # Poem generator
            poems = poem_generator()

            # Set up Producer for producing messages
            producer_config = {
                "bootstrap.servers": "localhost:9092"
            }
            producer = Producer(producer_config)

            # Check if the topic is empty and assign a default value if necessary
            # Strip leading and trailing whitespace, assign a default value if empty

            try:
                while True:
                    poem = next(poems)
                    print(f"Type: {poem['type']}, Poem: {poem['message']}")

                    # serialize data based on the serialization option
                    topic_selected, key_ser, value_ser = serialize_function(topic_name, serialization_option, poem)
                    # Your function logic here
                    print(f"Processing topic: {topic_selected}")
                    print(f"Processing key: {key_ser}")
                    print(f"Processing value: {value_ser}")

                    # Write to Kafka topic
                    write_to_topic(producer, topic_name = topic_selected, key=key_ser, message=value_ser)
                    # Introduce a delay between 1 to 4 seconds
                    time.sleep(random.uniform(1, 4))

            except KeyboardInterrupt:
                # Handle Ctrl+C gracefully
                print("Stopping Kafka producer.")
            finally:
                # Close the Kafka producer
                producer.flush()
                producer.close()


        elif choice == "8old":
            topic_name = input("Enter the topic name to write: ")
            # Poem generator
            poems = poem_generator()

            # Set up Producer for producing messages
            producer_config = {
                "bootstrap.servers": "localhost:9092"
            }
            producer = Producer(producer_config)
            try:
                while True:
                    poem = next(poems)
                    # print(f"Type: {poem['type']}, Poem: {poem['message']}")
                    # Write to Kafka topic
                    write_to_topic(producer, topic_name, poem['message'], key=poem['type'])
                    # Introduce a delay between 1 to 4 seconds
                    time.sleep(random.uniform(1, 4))

            except KeyboardInterrupt:
                # Handle Ctrl+C gracefully
                print("Stopping Kafka producer.")
            finally:
                # Close the Kafka producer
                producer.flush()
                producer.close()

        elif choice == "9":
            # Set up Consumer for consuming messages
            consumer_config = {
                    "bootstrap.servers": "localhost:9092",
                    "group.id": "my-group",
                    "auto.offset.reset": "earliest"
            }
            consumer = Consumer(consumer_config)

            # Get user input for deserialisation method
            deserialization_option = input("Choose de-serialization option:\nn - None\ns - JSON\na - Avro\nEnter serialization option: ")

            # Get user input for the topic name , strip leading and trailing whitespace
            topic_name = input("Enter the topic name to Read from (Enter for Default): ").strip()

            if not topic_name:
                if deserialization_option == 'n':
                    topic_default = 'gnone'
                elif deserialization_option == 's':
                    topic_default = 'gjson'
                elif deserialization_option == 'a':
                    topic_default = 'gavro'
                else:
                    topic_default = 'None'
                    print("Error: Unknown serialization option")
                topic_name = topic_default
                
            print(topic_name)
            consumer.subscribe([topic_name], on_assign=assignment_callback)

            try:
                while True:
                    event = consumer.poll(1.0)
                    
                    if event is None:
                        continue
                    if event.error():
                        raise KafkaException(event.error())
                    else:
                        #val = event.value().decode('utf8')
                        #key = event.key().decode('utf8')

                        key, val = deserialize_function(deserialization_option, event)

                        partition = event.partition()
                        offset = event.offset()
                        #val =0
                        print(f'Received: {val} from partition [{partition}] at offset: {offset}     ')

                        # consumer.commit(event)
            except KeyboardInterrupt:
                print('Canceled by user.')

            finally:
                consumer.close()
      
        elif choice == "-1":
            break
        else:
            print("Invalid choice. Please enter a number between 1 and 9.")

if __name__ == "__main__":
    main()
