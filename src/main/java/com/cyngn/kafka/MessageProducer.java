package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Module to listen for messages from vertx event bus and send them to Kafka topic.
 *
 * @author asarda@cyngn.com (Ajay Sarda) on 8/14/15.
 */
public class MessageProducer extends AbstractVerticle {
    private Producer producer;
    public static String EVENTBUS_DEFAULT_ADDRESS = "kafka.message.publisher";
    private String busAddress;
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private String topic;
    private JsonObject producerConfig;

    @Override
    public void start(final Future<Void> startedResult) {
        try {
            producerConfig = config().getJsonObject("producer");

            Properties properties = populateKafkaConfig(producerConfig);

            busAddress = producerConfig.getString(ConfigConstants.EVENTBUS_ADDRESS, EVENTBUS_DEFAULT_ADDRESS);
            topic = producerConfig.getString(ConfigConstants.TOPIC);

            producer = new Producer(new ProducerConfig(properties));

            vertx.eventBus().consumer(busAddress, new Handler<Message<String>>() {
                @Override
                public void handle(Message<String> message) {
                    sendMessage(message.body());
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread() {
                // try to disconnect from ZK as gracefully as possible
                public void run() {
                    shutdown();
                }
            });

            startedResult.complete();
        } catch (Exception ex) {
            logger.error("Message consumer initialization failed with ex: {}", ex);
            startedResult.fail(ex);
        }

    }

    /**
     * Send a message on a pre-configured topic.
     *
     * @param message the message to send
     */
    public void sendMessage(String message) {
        KeyedMessage<String, String> kafkaMessage =
                new KeyedMessage<String, String>(topic, message);

        producer.send(kafkaMessage);
    }

    private Properties populateKafkaConfig(JsonObject config) {
        Properties properties = new Properties();

        properties.put(ConfigConstants.METADATA_BROKER_LIST, getRequiredConfig(ConfigConstants.METADATA_BROKER_LIST));
        properties.put(ConfigConstants.SERIALIZER_CLASS, getRequiredConfig(ConfigConstants.SERIALIZER_CLASS));
        properties.put(ConfigConstants.PRODUCER_TYPE, config.getString(ConfigConstants.PRODUCER_TYPE, "sync"));
        return properties;
    }

    private String getRequiredConfig(String key) {
        String value = producerConfig.getString(key, null);

        if (null == value) {
            throw new IllegalArgumentException(String.format("Required config value not found key: %s", key));
        }
        return value;
    }

    private void shutdown() {
        if (producer != null) {
            producer.close();
            producer = null;
        }
    }
}
