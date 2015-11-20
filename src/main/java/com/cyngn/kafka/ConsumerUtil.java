package com.cyngn.kafka;

import io.vertx.core.json.JsonObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Util functions for dealing with Kafka messages
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 11/30/15
 */
public class ConsumerUtil {

    public static final String TOPIC_FIELD = "topic";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String PARTITION_FIELD = "partition";

    /**
     * Convert a Kafka ConsumerRecord into an event bus event.
     *
     * @param record the Kafka record
     * @return the record to send over the event bus
     */
    public static JsonObject createEvent(ConsumerRecord<String,String> record) {
        return new JsonObject()
                .put(TOPIC_FIELD, record.topic())
                .put(KEY_FIELD, record.key())
                .put(VALUE_FIELD, record.value())
                .put(PARTITION_FIELD, record.partition());
    }
}
