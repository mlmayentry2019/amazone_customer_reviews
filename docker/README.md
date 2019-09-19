Make request to flume service

```
cd docker
docker-compose up

#create topic in kafka
./create_topic.sh

curl -X POST -H 'Content-Type: application/json; charset=UTF-8' -d '[{"body":"hello world"}]' localhost:1234

./view_topic.sh
```