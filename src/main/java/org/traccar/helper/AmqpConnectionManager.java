package org.traccar.helper;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.SslContextFactory;
import org.traccar.config.Config;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Hashtable;
import java.util.concurrent.TimeoutException;

public final class AmqpConnectionManager {

    private static final Hashtable<String, Connection> AMQP_CONNECTIONS = new Hashtable<>();
    private final Config config;

    public AmqpConnectionManager(Config config) {
        this.config = config;
    }

    public void put(String connectionUrl, Connection connection) {
        if (AMQP_CONNECTIONS.containsKey((connectionUrl))) {
            return;
        }

        AMQP_CONNECTIONS.put(connectionUrl, connection);
    }

    public Connection createConnection(String connectionUrl) {

        return createConnection(connectionUrl, null);
    }

    public Connection createConnection(String connectionUrl, AmqpSslConfiguration amqpSslConfiguration) {
        if (AMQP_CONNECTIONS.containsKey(connectionUrl)) {
            return AMQP_CONNECTIONS.get(connectionUrl);
        }

        Connection connection = null;

        try {
            // AMQP/RabbitMQ best practice states that the connections should be long-lived and
            // channels short-lived. There is no reason to establish several connections to the same
            // RabbitMQ host (identified by the URL through same uname:pwd/host:port)

            if (!AMQP_CONNECTIONS.containsKey(connectionUrl)) {
                synchronized (AmqpConnectionManager.class) {
                    if (!AMQP_CONNECTIONS.containsKey(connectionUrl)) {
                        ConnectionFactory factory = new ConnectionFactory();
                        factory.setUri(connectionUrl);

                        if (amqpSslConfiguration != null) {
                            factory.setSslContextFactory(getSslContextFactory(amqpSslConfiguration));
                        }

                        connection = factory.newConnection();
                        AMQP_CONNECTIONS.put(connectionUrl, connection);
                    }
                }
            }
        } catch (NoSuchAlgorithmException
                 | URISyntaxException
                 | KeyManagementException
                 | IOException
                 | TimeoutException e) {
            throw new RuntimeException("Unable to create connection to RabbitMQ broker.", e);
        }

        return connection;
    }

    private SslContextFactory getSslContextFactory(AmqpSslConfiguration amqpSslConfiguration) {
        throw new RuntimeException("SSL Configuration is not implemented");
    }

    public class AmqpSslConfiguration {

    }
}
