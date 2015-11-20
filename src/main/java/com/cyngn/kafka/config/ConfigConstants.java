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
    public static String WORKERS_PER_TOPIC = "workers.per.topic";
    public static String GROUP_ID = "group.id";
    public static String BACKOFF_INCREMENT_MS = "backoff.increment.ms";
    public static String AUTO_OFFSET_RESET = "autooffset.reset";
    public static String EVENTBUS_ADDRESS = "eventbus.address";
    public static String ZK_CONNECT = "zookeeper.connect";

    public static String ERROR_TOPIC = "kafka.producer.error";

    // producer constants
    public static String BOOTSTRAP_SERVERS = "bootstrap.servers";
    public static String SERIALIZER_CLASS = "serializer.class";
    public static String KEY_SERIALIZER_CLASS = "key.serializer";
    public static String VALUE_SERIALIZER_CLASS = "value.serializer";
    public static String DEFAULT_TOPIC = "default.topic";
}
