package org.traccar.helper;
import com.rabbitmq.client.Connection;

import java.util.Hashtable;

public final class AmqpConnectionStorage {

    private static final Hashtable<String, Connection> AMQP_CONNECTIONS = new Hashtable<>();

    private AmqpConnectionStorage() { }

    public static void put(String connectionUrl, Connection connection) {
        if (AMQP_CONNECTIONS.containsKey((connectionUrl))) {
            return;
        }

        AMQP_CONNECTIONS.put(connectionUrl, connection);
    }

    public static Connection get(String connectionUrl) {
        if (AMQP_CONNECTIONS.containsKey(connectionUrl)) {
            return AMQP_CONNECTIONS.get(connectionUrl);
        }

        return null;
    }
}
