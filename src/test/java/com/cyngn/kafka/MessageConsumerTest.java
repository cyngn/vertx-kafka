package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.*;
import org.junit.runner.RunWith;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Basic test show how to consume messages from kafka and then publish them on the event bus.
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
 *       run: bin/kafka-console-producer.sh --broker-list localhost:9092 --topic testTopic
 *       run: [type text and press enter]
 *
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 2/19/15
 */
@RunWith(VertxUnitRunner.class)
public class MessageConsumerTest {
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerTest.class);

    private static Vertx vertx;

    @Ignore("This is an integration test comment out to actually run it")
    @Test
    public void testMessageReceipt(TestContext testContext) {
        Async async = testContext.async();

        vertx = Vertx.vertx();

        JsonObject consumerConfig = new JsonObject();
        consumerConfig.put(ConfigConstants.GROUP_ID, "testGroup");
        List<String> topics = new ArrayList<>();
        topics.add("testTopic");
        consumerConfig.put("topics", new JsonArray(topics));

        JsonObject config = new JsonObject().put("consumer", consumerConfig);


        vertx.deployVerticle(MessageConsumer.class.getName(),
                new DeploymentOptions().setConfig(config), deploy -> {
                    if (deploy.failed()) {
                        logger.error("", deploy.cause());
                        testContext.fail("Could not deploy verticle");
                        async.complete();
                        vertx.close();
                    } else {
                        long timerId = vertx.setTimer(20000, theTimerId ->
                        {
                            logger.info("Failed to get any messages");
                            testContext.fail("Test did not complete in 20 seconds");
                            async.complete();
                            vertx.close();
                        });

                        logger.info("Registering listener on event bus for kafka messages");

                        vertx.eventBus().consumer(MessageConsumer.EVENTBUS_DEFAULT_ADDRESS, message -> {
                            assertTrue(message.body().toString().length() > 0);
                            logger.info("got message: " + message.body());
                            vertx.cancelTimer(timerId);
                            async.complete();
                            vertx.close();
                        });
                    }
                }
        );
    }
}
