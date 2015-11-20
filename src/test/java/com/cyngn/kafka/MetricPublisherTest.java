package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import com.cyngn.kafka.produce.KafkaPublisher;
import com.cyngn.kafka.produce.MessageProducer;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Basic tests for the publisher helper.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/21/15
 */
@RunWith(VertxUnitRunner.class)
public class MetricPublisherTest {

    private static Vertx vertx;

    @Test
    public void testFailure(TestContext context) {
        Async async = context.async();
        vertx = Vertx.vertx();

        // fake verticle
        vertx.eventBus().consumer(MessageProducer.EVENTBUS_DEFAULT_ADDRESS, msg -> {
            msg.fail(-1, "I fail everything");
        });


        KafkaPublisher pub = new KafkaPublisher(vertx.eventBus());
        pub.send("foo", "an empty message");

        // listen for error message
        vertx.eventBus().consumer(ConfigConstants.PRODUCER_ERROR_TOPIC, msg -> {
            try {
                Assert.assertTrue(msg.body().toString().indexOf("I fail everything") != -1);
            } finally {
                async.complete();
                vertx.close();
            }
        });
    }
}
