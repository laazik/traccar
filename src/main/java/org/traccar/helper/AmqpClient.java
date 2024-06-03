/*
 * Copyright 2023 Anton Tananaev (anton@traccar.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.traccar.helper;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.MessageProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class AmqpClient implements Closeable {

    private static final AtomicInteger INSTANCE_COUNT = new AtomicInteger();

    private Channel channel;
    private final String exchange;
    private final String topic;

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpClient.class);

    public AmqpClient(Connection connection, String exchange, String topic) {
        this.exchange = exchange;
        this.topic = topic;
        try {
            channel = connection.createChannel();
            LOGGER.atInfo().setMessage("AmqpClient instance count = {}")
                    .addArgument(INSTANCE_COUNT.incrementAndGet())
                    .log();
        } catch (IOException e) {
            LOGGER.error("RabbitMQ connection establishment failed.", e);
            throw new RuntimeException("Error while establishing connection to RabbitMQ broker", e);
        }
    }

    public void publishMessage(String message) throws IOException {
        channel.basicPublish(exchange, topic, MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
    }

    public void declareExchange(BuiltinExchangeType exchangeType, boolean durable) {
        try {
            channel.exchangeDeclare(this.exchange, exchangeType, durable);
        } catch (IOException e) {
            LOGGER.error("Failed declaring exchange in RabbitMQ. Already exists or no rights?", e);
            throw new RuntimeException("Unable to declare RabbitMQ exchange.", e);
        }
    }

    public void close() {
        try {
            if (channel.isOpen()) {
                channel.close();
            }
        } catch (IOException | TimeoutException e) {
            LOGGER.atWarn()
                    .setMessage("Got exception closing channel.")
                    .setCause(e).log();
        }

        channel = null;
        LOGGER.atInfo().setMessage("AmqpClient instance count = {}")
                .addArgument(INSTANCE_COUNT.decrementAndGet())
                .log();
    }
}
