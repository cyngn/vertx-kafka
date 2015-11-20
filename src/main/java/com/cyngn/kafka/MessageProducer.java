package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Module to listen for messages from vertx event bus and send them to Kafka defaultTopic.
 *
 * @author asarda@cyngn.com (Ajay Sarda) on 8/14/15.
 */
public class MessageProducer extends AbstractVerticle {
    private KafkaProducer producer;
    public static String EVENTBUS_DEFAULT_ADDRESS = "kafka.message.publisher";
    private String busAddress;
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);
    private String defaultTopic;
    private JsonObject producerConfig;

    @Override
    public void start(final Future<Void> startedResult) {
        try {
            producerConfig = config().getJsonObject("producer");

            Properties properties = populateKafkaConfig(producerConfig);

            busAddress = producerConfig.getString(ConfigConstants.EVENTBUS_ADDRESS, EVENTBUS_DEFAULT_ADDRESS);
            defaultTopic = producerConfig.getString(ConfigConstants.DEFAULT_TOPIC);

            producer = new KafkaProducer(properties);

            vertx.eventBus().consumer(busAddress, (Message<JsonObject> message) -> sendMessage(message.body()));

            Runtime.getRuntime().addShutdownHook(new Thread() {
                // try to disconnect from ZK as gracefully as possible
                public void run() {
                    shutdown();
                }
            });

            startedResult.complete();
        } catch (Exception ex) {
            logger.error("Message producer initialization failed with ex: {}", ex);
            startedResult.fail(ex);
        }
    }

    /**
     * Send a message on a pre-configured defaultTopic.
     *
     * @param message the message to send
     */
    public void sendMessage(JsonObject message) {

        ProducerRecord<String,String> record;

        if (!message.containsKey(KafkaPublisher.TYPE_FIELD)) {
            logger.error("Invalid message sent missing {} field, msg: {}", KafkaPublisher.TYPE_FIELD, message);
            return;
        }

        KafkaPublisher.MessageType type = KafkaPublisher.MessageType.fromInt(message.getInteger(KafkaPublisher.TYPE_FIELD));
        String value = message.getString(KafkaPublisher.VALUE_FIELD);
        switch (type) {
            case SIMPLE:
                record = new ProducerRecord(defaultTopic, value);
                break;
            case CUSTOM_TOPIC:
                record = new ProducerRecord(message.getString(KafkaPublisher.TOPIC_FIELD), value);
                break;
            case CUSTOM_KEY:
                record = new ProducerRecord(message.getString(KafkaPublisher.TOPIC_FIELD),
                        message.getString(KafkaPublisher.KEY_FIELD),
                        value);
                break;
            case CUSTOM_PARTITION:
                record = new ProducerRecord(message.getString(KafkaPublisher.TOPIC_FIELD),
                        message.getInteger(KafkaPublisher.PARTITION_FIELD),
                        message.getString(KafkaPublisher.KEY_FIELD),
                        value);
                break;
            default:
                logger.error("Invalid type submitted: {} message being thrown away: {}", type, value);
                return;
        }

        producer.send(record);
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
