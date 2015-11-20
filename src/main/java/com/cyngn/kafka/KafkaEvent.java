package com.cyngn.kafka;

import io.vertx.core.json.JsonObject;

/**
 * Represents a Kafka message event from the event bus.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/30/15
 */
public class KafkaEvent {
    public final String topic;
    public final String key;
    public final String value;
    public final int partition;

    public KafkaEvent(JsonObject event) {
        topic = event.getString(ConsumerUtil.TOPIC_FIELD);
        key = event.getString(ConsumerUtil.KEY_FIELD);
        value = event.getString(ConsumerUtil.VALUE_FIELD);
        partition = event.getInteger(ConsumerUtil.PARTITION_FIELD);
    }

    @Override
    public String toString() {
        return "KafkaEvent{" +
                "topic='" + topic + '\'' +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", partition=" + partition +
                '}';
    }
}
