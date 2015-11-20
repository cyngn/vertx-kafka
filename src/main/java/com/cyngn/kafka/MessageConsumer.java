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
package com.cyngn.kafka;

import com.cyngn.kafka.config.ConfigConstants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Verticle to listen for kafka messages and republish them on the vertx event bus
 *
 * to build:
 * ./gradlew clean build
 *
 * @author truelove@cyngn.com (Jeremy Truelove) 1/28/15
 */
public class MessageConsumer extends AbstractVerticle {
    private ConsumerConnector consumer;
    private ExecutorService threadPool;
    public static String EVENTBUS_DEFAULT_ADDRESS = "kafka.message.consumer";
    private String busAddress;
    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);
    private JsonObject consumerConfig;

    @Override
    public void start(final Future<Void> startedResult) {
        try {
            consumerConfig = config().getJsonObject("consumer");

            Properties consumerProperties = populateKafkaConfig(consumerConfig);

            busAddress = consumerConfig.getString(ConfigConstants.EVENTBUS_ADDRESS, EVENTBUS_DEFAULT_ADDRESS);

            int workersPerTopic = consumerConfig.getInteger(ConfigConstants.WORKERS_PER_TOPIC, 1);
            JsonArray topics = consumerConfig.getJsonArray(ConfigConstants.TOPICS);
            int totalTopics = topics.size() * workersPerTopic;

            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProperties));

            String topicList = getTopicFilter(topics);
            TopicFilter sourceTopicFilter = new Whitelist(topicList);
            List<KafkaStream<byte[], byte[]>> streams = consumer.createMessageStreamsByFilter(sourceTopicFilter, totalTopics);

            threadPool = Executors.newFixedThreadPool(streams.size());

            logger.info("Starting to consume messages group: " + consumerConfig.getString(ConfigConstants.GROUP_ID) +
                    " topic(s): " + topicList);
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                threadPool.submit(() -> startConsumer(stream));
            }

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

    private Properties populateKafkaConfig(JsonObject config) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConfigConstants.ZK_CONNECT, config.getString(ConfigConstants.ZK_CONNECT, "localhost:2181"));
        consumerConfig.put(ConfigConstants.BACKOFF_INCREMENT_MS,
                config.getString(ConfigConstants.BACKOFF_INCREMENT_MS, "100"));
        consumerConfig.put(ConfigConstants.BACKOFF_INCREMENT_MS,
                config.getString(ConfigConstants.AUTO_OFFSET_RESET, "smallest"));

        consumerConfig.put(ConfigConstants.GROUP_ID, getRequiredConfig(ConfigConstants.GROUP_ID));
        return consumerConfig;
    }

    /**
     * Start listening for messages.
     *
     * @param stream the inbound message streams
     */
    private void startConsumer(KafkaStream<byte[],byte[]> stream) {
        try {
            ConsumerIterator<byte[], byte[]> it = stream.iterator();
            while (it.hasNext()) {
                String msg = new String(it.next().message());
                vertx.eventBus().publish(busAddress, msg);
            }
        } catch (Exception ex) {
            logger.error("Failed while consuming messages", ex);
            throw ex;
        }
    }

    private String getRequiredConfig(String key) {
        String value = consumerConfig.getString(key, null);

        if (null == value) {
            throw new IllegalArgumentException(String.format("Required config value not found key: %s", key));
        }
        return value;
    }

    /**
     * Handles creating a topic filter String to pass to kafka to define what topics to consume.
     * @param topics the topics to listen on
     * @return the special formatted topic filter string
     */
    private String getTopicFilter(JsonArray topics) {
        StringBuilder builder = new StringBuilder();
        for (Object topic : topics) {
            logger.info("Preparing to filter on topic: " + topic);
            builder.append(topic.toString()).append("|");
        }
        if (builder.length() == 0) {
            throw new IllegalArgumentException("You must specify kafka topic(s) to filter on");
        }
        return builder.substring(0, builder.length() - 1);
    }

    /**
     * Handle stopping the consumer.
     */
    private void shutdown() {
        if (threadPool != null) {
            threadPool.shutdown();
            threadPool = null;
        }

        if (consumer != null) {
            consumer.shutdown();
            consumer = null;
        }
    }
}