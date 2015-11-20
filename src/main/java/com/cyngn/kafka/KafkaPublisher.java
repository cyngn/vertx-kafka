package com.cyngn.kafka;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

/**
 * Helper class for publishing kafka messages.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/19/15
 */
public class KafkaPublisher {

    private EventBus bus;
    private String address;

    public static String TOPIC_FIELD = "topic";
    public static String KEY_FIELD = "key";
    public static String PARTITION_FIELD = "partition";
    public static String VALUE_FIELD = "value";
    public static String TYPE_FIELD = "type";

    public KafkaPublisher(EventBus bus) {
        this(bus, MessageProducer.EVENTBUS_DEFAULT_ADDRESS);
    }

    public KafkaPublisher(EventBus bus, String address) {
        this.bus = bus;
        this.address = address;
    }

    public void send(String value) {
        JsonObject obj = new JsonObject()
                .put(VALUE_FIELD, value)
                .put(TYPE_FIELD, MessageType.SIMPLE.value);
        bus.send(address, obj);
    }

    public void send(String kafkaTopic, String value) {
        JsonObject obj = new JsonObject()
                .put(VALUE_FIELD, value)
                .put(TOPIC_FIELD, kafkaTopic)
                .put(TYPE_FIELD, MessageType.CUSTOM_TOPIC.value);
        bus.send(address, obj);
    }

    public void send(String kafkaTopic, String msgKey, String value) {
        JsonObject obj = new JsonObject()
                .put(VALUE_FIELD, value)
                .put(TOPIC_FIELD, kafkaTopic)
                .put(KEY_FIELD, msgKey)
                .put(TYPE_FIELD, MessageType.CUSTOM_KEY.value);
        bus.send(address, obj);
    }

    public void send(String kafkaTopic, String msgKey, Integer partitionKey, String value) {
        JsonObject obj = new JsonObject()
                .put(VALUE_FIELD, value)
                .put(TOPIC_FIELD, kafkaTopic)
                .put(PARTITION_FIELD, partitionKey)
                .put(KEY_FIELD, msgKey)
                .put(TYPE_FIELD, MessageType.CUSTOM_PARTITION.value);
        bus.send(address, obj);
    }

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
