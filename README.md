[![Build Status](https://travis-ci.org/cyngn/vertx-kafka.svg?branch=master)](https://travis-ci.org/cyngn/vertx-kafka)

# Vert.x Kafka

The Vert.x kafka library allows asynchronous publishing and receiving of messages on Kafka topic through the vert.x event bus.

####To use this library you must have kafka and zookeeper up and running. Follow instructions at [Kafka quick start guide](http://kafka.apache.org/documentation.html#quickstart)

This is a multi-threaded worker library that consumes kafka messages and then re-broadcast them on an address on the vert.x event bus.

## Getting Started

Add a dependency to vertx-kafka:

```xml
<dependency>
    <groupId>com.cyngn.vertx</groupId>
    <artifactId>vertx-kafka</artifactId>
    <version>0.1.0</version>
</dependency>
```

## Consumer

Listening for messages coming from a kafka broker.

### Configuration

```json
	{
    "consumer" :
		{
			"zookeeper.host" : "<zookeeperHost>",
			"port": "<zookeeperPort>",
			"workers.per.topic" : <totalWorkersToCreateForEachTopic>,
			"group.id" : "<kafkaConsumerGroupId>",
			"backoff.increment.ms" : "<backTimeInMilli>",
			"autooffset.reset" : "<kafkaAutoOffset>",
			"topics" : ["<topic1>", "<topic2>"],
			"eventbus.address" : "<default kafka.message>"
		}
	}
```

For example:

```json
	{
    "consumer" :
		{
			"zookeeper.host" : "localhost",
			"port": "2181",
			"workers.per.topic" : 3,
			"group.id" : "testGroup",
			"backoff.increment.ms" : "100",
			"autooffset.reset" : "smallest",
			"topics" : ["testTopic"],
			"eventbus.address" : "kafka.to.vertx.bridge"
		}
	}
```
Field breakdown:

* `zookeeper.host` a zookeeper host being used with your kafka clusters
* `port` zookeeper port to connect on
* `workers.per.topic` a thread will be spawned to consume messages up to the total number specified for each topic
* `group.id` the kafka consumer group name that will be consuming related to
* `backoff.increment.ms` backoff interval for contacting broker without messages in milliseconds
* `autooffset.reset` how to reset the offset
* `topics` the kafka topics to listen for
* `eventbus.address` the vert.x address to publish messages onto when received form kafka

For a deeper look at kafka configuration parameters check [this](https://kafka.apache.org/08/configuration.html) page out.

### Usage

You should only need one consumer per application.

#### Deploy the verticle in your server

```java

vertx = Vertx.vertx();

// sample config
JsonObject consumerConfig = new JsonObject();
consumerConfig.put(ConfigConstants.GROUP_ID, "testGroup");
List<String> topics = new ArrayList<>();
topics.add("testTopic");
consumerConfig.put("topics", new JsonArray(topics));

JsonObject config = new JsonObject().put("consumer", consumerConfig);

deployKafka(config);

public void deployKafka(JsonObject config) {
   // use your vert.x reference to deploy the consumer verticle
	 vertx.deployVerticle(MessageConsumer.class.getName(),
      new DeploymentOptions().setConfig(config),
			deploy -> {
   		     if(deploy.failed()) {
        	     System.err.println(String.format("Failed to start kafka consumer verticle, ex: %s", deploy.cause()));
               vertx.close()
               return;
            }
            System.out.println("kafka consumer verticle started");
      }
	);
}
```
#### Listen for messages

```java

vertx.eventBus().consumer(MessageConsumer.EVENTBUS_DEFAULT_ADDRESS,
	message -> {
		   System.out.println(String.format("got message: %s", message.body()))
			 // message handling code
	 });
```

## Producer

Send a message to a kafka cluster on a predefined topic.

### Configuration

```json
	{
		"producer" : {
			"serializer.class": "<serializerclass>",
			"metadata.broker.list": "<host>:<name>",
			"producer.type" : "async"
		}
	}
```

For example:

```json
	{
		"producer" : {
			"serializer.class": "kafka.serializer.StringEncoder",
			"metadata.broker.list": "localhost:9092",
			"producer.type" : "async"
		 }
	}
```

* `serializer.class` The serializer class for messages
* `metadata.broker.list`The socket connections for sending the actual data will be established based on the broker information returned in the metadata. The format is host1:port1,host2:port2, and the list can be a subset of brokers or a VIP pointing to a subset of brokers.
* `producer.type` This parameter specifies whether the messages are sent asynchronously in a background thread. Valid values are (1) async for asynchronous send and (2) sync for synchronous send. By setting the producer to async we allow batching together of requests (which is great for throughput) but open the possibility of a failure of the client machine dropping unsent data.

For a deeper look at kafka configuration parameters check [this](https://kafka.apache.org/08/configuration.html) page out.

### Usage

You should only need one producer per application.

#### Deploy the verticle in your server

```java

vertx = Vertx.vertx();

// sample config
JsonObject producerConfig = new JsonObject();
producerConfig.put("metadata.broker.list", "localhost:9092");
producerConfig.put("serializer.class", "kafka.serializer.StringEncoder");
producerConfig.put("topic", "testTopic");

JsonObject config = new JsonObject().put("producer", producerConfig);

deployKafka(config);

public void deployKafka(JsonObject config) {
   // use your vert.x reference to deploy the consumer verticle
	 vertx.deployVerticle(MessageProducer.class.getName(),
      new DeploymentOptions().setConfig(config),
			deploy -> {
   		      if(deploy.failed()) {
        	      System.err.println(String.format("Failed to start kafka producer verticle, ex: %s", deploy.cause()));
                vertx.close()
                return;
            }
            System.out.println("kafka producer verticle started");
      }
	);
}
```
#### Send message to kafka topic

```java
	vertx.eventBus().send(busAddress, "your_message")
```


### Test setup

* cd [yourKafkaInstallDir]
* bin/zookeeper-server-start.sh config/zookeeper.properties
* bin/kafka-server-start.sh config/server.properties
* bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic [yourTestTopic]
* bin/kafka-console-producer.sh --broker-list localhost:9092 --topic [yourTestTopic]
* bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic [yourTestTopic]
