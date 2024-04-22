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
package org.traccar.forward;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.AmqpClient;
import org.traccar.helper.AmqpConnectionManager;

import java.io.IOException;

public class EventForwarderAmqp implements EventForwarder {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventForwarderAmqp.class);

    private final AmqpClient amqpClient;
    private final ObjectMapper objectMapper;

    public EventForwarderAmqp(Config config, ObjectMapper objectMapper, AmqpConnectionManager connectionManager) {
        String connectionUrl = config.getString(Keys.EVENT_FORWARD_URL);
        String exchange = config.getString(Keys.EVENT_FORWARD_EXCHANGE);
        String topic = config.getString(Keys.EVENT_FORWARD_TOPIC);

        this.objectMapper = objectMapper;

        LOGGER.atDebug()
                .setMessage("Creating connection to RabbitMQ. URL = {}, Exchange = {}, Topic = {}")
                .addArgument(connectionUrl).addArgument(exchange).addArgument(topic).log();
        Connection connection = connectionManager.createConnection(connectionUrl);

        amqpClient = new AmqpClient(connection, exchange, topic);

        if (config.getBoolean(Keys.EVENT_FORWARD_AMQP_EXCHANGE_DECLARE)) {
            String exchangeType = config.getString(
                    config.getString(Keys.EVENT_FORWARD_AMQP_EXCHANGE_TYPE),
                    BuiltinExchangeType.TOPIC.toString());
            boolean durable = config.getBoolean(Keys.EVENT_FORWARD_AMQP_EXCHANGE_DURABLE);

            amqpClient.declareExchange(BuiltinExchangeType.valueOf(exchangeType), durable);
        }
    }

    @Override
    public void forward(EventData eventData, ResultHandler resultHandler) {
        try {
            String value = objectMapper.writeValueAsString(eventData);
            amqpClient.publishMessage(value);
            LOGGER.atDebug()
                    .setMessage("Published event forward: {}").addArgument(value).log();
            resultHandler.onResult(true, null);
        } catch (IOException e) {
            LOGGER.atWarn()
                    .setMessage("Failed event forwarding.").setCause(e).log();
            resultHandler.onResult(false, e);
        }
    }
}
