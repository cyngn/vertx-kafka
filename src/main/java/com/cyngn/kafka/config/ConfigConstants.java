/*
 * Copyright 2015 Cyanogen Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.cyngn.kafka.config;

/**
 * Constants to pull from config.
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 1/27/15
 */
public class ConfigConstants {
    public static String TOPICS = "topics";
    public static String GROUP_ID = "group.id";
    public static String BACKOFF_INCREMENT_MS = "backoff.increment.ms";
    public static String AUTO_OFFSET_RESET = "autooffset.reset";
    public static String EVENTBUS_ADDRESS = "eventbus.address";
    public static String CONSUMER_POLL_INTERVAL_MS = "consumer.poll.interval.ms";
    public static String ZK_CONNECT = "zookeeper.connect";
    public static String KEY_DESERIALIZER_CLASS = "key.deserializer";
    public static String VALUE_DESERIALIZER_CLASS = "value.deserializer";
    public static String DEFAULT_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";
    public static String CONSUMER_ERROR_TOPIC = "kafka.consumer.error";

    //common
    public static String BOOTSTRAP_SERVERS = "bootstrap.servers";

    // producer constants
    public static String PRODUCER_ERROR_TOPIC = "kafka.producer.error";
    public static String DEFAULT_TOPIC = "default.topic";
    public static String KEY_SERIALIZER_CLASS = "key.serializer";
    public static String PRODUCER_TYPE = "producer.type";
    public static String SERIALIZER_CLASS = "serializer.class";
    public static String VALUE_SERIALIZER_CLASS = "value.serializer";
    public static String DEFAULT_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
    public static String MAX_BLOCK_MS = "max.block.ms";

    // event bus fields
    public static final String TOPIC_FIELD = "topic";
    public static final String KEY_FIELD = "key";
    public static final String VALUE_FIELD = "value";
    public static final String PARTITION_FIELD = "partition";
}
