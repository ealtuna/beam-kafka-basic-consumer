# beam-kafka-basic-consumer

## Provision

    docker-compose up -d

## Create data-gen connector

    curl -X POST -H "Content-Type: application/json" --data @schema/connector_orders.config http://localhost:8084/connectors

## Inspect topic using KSQL

````
docker exec -it ksqldb bash -c 'ksql http://ksqldb:8088'
PRINT orders FROM BEGINNING;
CREATE STREAM orders WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='Avro');
````

For additional references about KSQL: https://www.confluent.de/blog/stream-processing-twitter-data-with-ksqldb/

## Manual publish events

````
docker exec -it kafka bash
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic orders
kafka-topics --list --zookeeper zookeeper:2181
kafka-console-producer --broker-list kafka:9092 --topic orders --property "parse.key=true" --property "key.separator=:"
````

After the prompt create some events using KEY:BODY, for example 3:4

## Create Java project from Maven archetype

mvn archetype:generate \
      -DarchetypeGroupId=org.apache.beam \
      -DarchetypeArtifactId=beam-sdks-java-maven-archetypes-examples \
      -DarchetypeVersion=2.24.0 \
      -DgroupId=org.example \
      -DartifactId=word-count-beam \
      -Dversion="0.1" \
      -Dpackage=org.apache.beam.examples \
      -DinteractiveMode=false

## Execute Bean streaming application

mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
     -Dexec.args="--inputFile=pom.xml --output=./output/counts" -Pdirect-runner

## Clean up

docker-compose down
