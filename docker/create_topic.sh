# create topic 'foo' in kafka
docker exec -it kafka sh -c 'kafka-topics --create --topic foo --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181'