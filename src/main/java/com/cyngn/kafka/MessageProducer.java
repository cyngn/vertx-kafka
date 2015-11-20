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

            Properties properties = populateKafkaConfig();

            busAddress = producerConfig.getString(ConfigConstants.EVENTBUS_ADDRESS, EVENTBUS_DEFAULT_ADDRESS);
            defaultTopic = producerConfig.getString(ConfigConstants.DEFAULT_TOPIC);

            producer = new KafkaProducer(properties);

            vertx.eventBus().consumer(busAddress, (Message<JsonObject> message) -> sendMessage(message));

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
    public void sendMessage(Message<JsonObject> message) {
        JsonObject payload = message.body();
        ProducerRecord<String,String> record;

        if (!payload.containsKey(KafkaPublisher.TYPE_FIELD)) {
            logger.error("Invalid message sent missing {} field, msg: {}", KafkaPublisher.TYPE_FIELD, message);
            return;
        }

        KafkaPublisher.MessageType type = KafkaPublisher.MessageType.fromInt(payload.getInteger(KafkaPublisher.TYPE_FIELD));
        String value = payload.getString(KafkaPublisher.VALUE_FIELD);
        switch (type) {
            case SIMPLE:
                record = new ProducerRecord(defaultTopic, value);
                break;
            case CUSTOM_TOPIC:
                record = new ProducerRecord(payload.getString(KafkaPublisher.TOPIC_FIELD), value);
                break;
            case CUSTOM_KEY:
                record = new ProducerRecord(payload.getString(KafkaPublisher.TOPIC_FIELD),
                        payload.getString(KafkaPublisher.KEY_FIELD),
                        value);
                break;
            case CUSTOM_PARTITION:
                record = new ProducerRecord(payload.getString(KafkaPublisher.TOPIC_FIELD),
                        payload.getInteger(KafkaPublisher.PARTITION_FIELD),
                        payload.getString(KafkaPublisher.KEY_FIELD),
                        value);
                break;
            default:
                String error = String.format("Invalid type submitted: {} message being thrown away: %s",
                        type.toString(), value);
                logger.error(error);
                message.fail(-1, error);
                return;
        }

        producer.send(record);
        message.reply(new JsonObject());
    }

    private Properties populateKafkaConfig() {
        Properties properties = new Properties();

        properties.put(ConfigConstants.BOOTSTRAP_SERVERS, getRequiredConfig(ConfigConstants.BOOTSTRAP_SERVERS));

        // default serializer to the String one
        String defaultSerializer = producerConfig.getString(ConfigConstants.SERIALIZER_CLASS,
                "org.apache.kafka.common.serialization.StringSerializer");

        properties.put(ConfigConstants.SERIALIZER_CLASS, defaultSerializer);
        properties.put(ConfigConstants.KEY_SERIALIZER_CLASS,
                producerConfig.getString(ConfigConstants.KEY_SERIALIZER_CLASS, defaultSerializer));
        properties.put(ConfigConstants.VALUE_SERIALIZER_CLASS,
                producerConfig.getString(ConfigConstants.VALUE_SERIALIZER_CLASS, defaultSerializer));
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
