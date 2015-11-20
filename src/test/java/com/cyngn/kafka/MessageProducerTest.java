package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Basic test show how to consume messages from vertx event bus and send them to Kafka topic.
 *
 * To run test you need to do the following
 *
 * 1) start zookeeper, ie from a kafka local install dir
 *      run: 'bin/zookeeper-server-start.sh  config/zookeeper.properties'
 * 2) start a message broker, ie from a kafka install dir
 *      run: 'bin/kafka-server-start.sh config/server.properties'
 * 3) setup a topic in kafka, ie from the kafka install dir
 *       run: 'bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 8 --topic testTopic'
 * 4) start the test (note it will time out and fail if it doesn't receive a message in 20 seconds)
 * 5) publish a message on the topic from the kafka CLI, ie from the kafka install dir
 *       run: bin/kafka-console-consumer.sh --topic testTopic --zookeeper localhost:2181
 *      You should see: This is the message you should see in your consumer
 *
 * @author asarda@cyngn.com (Ajay Sarda) on 8/14/15.
 */
@RunWith(VertxUnitRunner.class)
public class MessageProducerTest {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducerTest.class);

    private static Vertx vertx;

    @Ignore("This is an integration test comment out to actually run it")
    @Test
    public void testMessageSend(TestContext testContext) {
        Async async = testContext.async();
        vertx = Vertx.vertx();

        JsonObject producerConfig = new JsonObject();
        producerConfig.put(ConfigConstants.BOOTSTRAP_SERVERS, "localhost:9092");
        producerConfig.put(ConfigConstants.DEFAULT_SERIALIZER_CLASS, "org.apache.kafka.common.serialization.StringSerializer");
        producerConfig.put(ConfigConstants.MAX_BLOCK_MS, new Long(500));
        producerConfig.put("default.topic", "testGroup");

        KafkaPublisher publisher = new KafkaPublisher(vertx.eventBus());

        vertx.deployVerticle(MessageProducer.class.getName(),
        new DeploymentOptions().setConfig(producerConfig), deploy -> {
            if (deploy.failed()) {
                logger.error("", deploy.cause());
                testContext.fail("Could not deploy verticle");
                async.complete();
                vertx.close();
            } else {
                publisher.send("This is the message you should see in your consumer");
                // give vert.x some time to get the message off
                long timerId = vertx.setTimer(10000, timer -> {
                    async.complete();
                    vertx.close();
                });

                vertx.eventBus().consumer(ConfigConstants.PRODUCER_ERROR_TOPIC, reply -> {
                    testContext.fail("test has failed");
                    logger.error("error: " + reply.toString());
                    vertx.cancelTimer(timerId);
                    vertx.close();
                });
            }
        });
    }
}
