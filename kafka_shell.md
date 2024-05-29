##### KAFKA SHELL

These shell examples cover a wide range of interactions with Kafka, including managing topics, producing and consuming messages, controlling consumer groups, and altering configurations. You can adapt these commands based on your specific Kafka setup and requirements.


Create a Topic:
```
kafka-topics.sh --create --bootstrap-server localhost:9092 --topic my_topic --partitions 3 --replication-factor 1 --config retention.ms=604800000
```

List Existing Topics:
```
kafka-topics.sh --list --bootstrap-server localhost:9092
```

Describe Topic Details:
```
kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic my_topic
```

Alter Topic Configuration:
```
kafka-configs.sh --bootstrap-server localhost:9092 --entity-type topics --entity-name my_topic --alter --add-config retention.ms=86400000
```

Produce Messages to a Topic:
```
echo "Hello, Kafka!" | kafka-console-producer.sh --broker-list localhost:9092 --topic my_topic
```

Consume Messages from a Topic:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my_topic --from-beginning
```

Consume Messages from a Specific Partition:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my_topic --partition 0 --offset earliest
```

Consume Messages with Key Displayed:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my_topic --property print.key=true --from-beginning
```

Consume Messages with Specific Consumer Group:
```
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my_topic --group my_consumer_group
```

Offset Reset for Consumer Group:
```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my_consumer_group --reset-offsets --to-earliest --execute --topic my_topic
```

List Consumer Groups:
```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
```

Describe Consumer Group Details:
```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my_consumer_group
```

Alter Consumer Group Offsets:
```
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my_consumer_group --reset-offsets --topic my_topic:0 --to-offset 10 --execute
```
