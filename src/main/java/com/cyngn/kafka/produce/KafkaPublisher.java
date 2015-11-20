package com.cyngn.kafka.produce;

import com.cyngn.kafka.config.ConfigConstants;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Helper class for publishing kafka messages.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/19/15
 */
public class KafkaPublisher {
    private static final Logger logger = LoggerFactory.getLogger(KafkaPublisher.class);
    private EventBus bus;
    private String address;

    public static String TYPE_FIELD = "type";

    public KafkaPublisher(EventBus bus) {
        this(bus, MessageProducer.EVENTBUS_DEFAULT_ADDRESS);
    }

    public KafkaPublisher(EventBus bus, String address) {
        this.bus = bus;
        this.address = address;
    }

    /**
     * Send a message to the default topic
     *
     * @param value the string value to send
     */
    public void send(String value) {
        JsonObject obj = new JsonObject()
                .put(ConfigConstants.VALUE_FIELD, value)
                .put(TYPE_FIELD, MessageType.SIMPLE.value);
        send(obj);
    }

    /**
     * Send a message to a specific topic
     *
     * @param kafkaTopic the kafka topic to send to
     * @param value the string value to send
     */
    public void send(String kafkaTopic, String value) {
        JsonObject obj = new JsonObject()
                .put(ConfigConstants.VALUE_FIELD, value)
                .put(ConfigConstants.TOPIC_FIELD, kafkaTopic)
                .put(TYPE_FIELD, MessageType.CUSTOM_TOPIC.value);
        send(obj);
    }

    /**
     * Send a message to a specific topic
     *
     * @param kafkaTopic the kafka topic to send to
     * @param msgKey the custom key to assist in determining the partition
     * @param value the string value to send
     */
    public void send(String kafkaTopic, String msgKey, String value) {
        JsonObject obj = new JsonObject()
                .put(ConfigConstants.VALUE_FIELD, value)
                .put(ConfigConstants.TOPIC_FIELD, kafkaTopic)
                .put(ConfigConstants.KEY_FIELD, msgKey)
                .put(TYPE_FIELD, MessageType.CUSTOM_KEY.value);
        send(obj);
    }

    /**
     * Send a message to a specific topic
     *
     * @param kafkaTopic the kafka topic to send to
     * @param msgKey the custom key to assist in determining the partition
     * @param partitionKey the specific partition to use
     * @param value the string value to send
     */
    public void send(String kafkaTopic, String msgKey, Integer partitionKey, String value) {
        JsonObject obj = new JsonObject()
                .put(ConfigConstants.VALUE_FIELD, value)
                .put(ConfigConstants.TOPIC_FIELD, kafkaTopic)
                .put(ConfigConstants.PARTITION_FIELD, partitionKey)
                .put(ConfigConstants.KEY_FIELD, msgKey)
                .put(TYPE_FIELD, MessageType.CUSTOM_PARTITION.value);
        send(obj);
    }

    private void send(JsonObject message) {
        bus.send(address, message, result -> {
           if(result.failed()) {
               logger.error(result.cause());
               bus.send(ConfigConstants.PRODUCER_ERROR_TOPIC, result.cause().toString());
           }
        });
    }

    /**
     * Used to allow us to easily determine how to parse a message published
     */
    public enum MessageType {
        INVALID(0),
        SIMPLE(1),
        CUSTOM_TOPIC(2),
        CUSTOM_KEY(3),
        CUSTOM_PARTITION(4);

        public final int value;

        MessageType(int value) {
            this.value = value;
        }

        public static MessageType fromInt(int value) {
            switch(value) {
                case 1:
                    return SIMPLE;
                case 2:
                    return CUSTOM_TOPIC;
                case 3:
                    return CUSTOM_KEY;
                case 4:
                    return CUSTOM_PARTITION;
                default:
                    return INVALID;
            }
        }
    }
}
