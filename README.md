# Apache Kafka
### getting started

- change default num of topic partitions
  - config/server.properties (property: num.partitions)
- alter topic num of partitions
  - bin/kafka-topics.sh --alter --bootstrap-server localhost:9092 --topic STORE_NEW_ORDER --partitions 3
- list and describe topics
  - bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
- list and describe topics
  - bin/kafka-consumer-groups.sh  --all-groups --bootstrap-server localhost:9092 --describe