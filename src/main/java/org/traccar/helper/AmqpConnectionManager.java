package org.traccar.helper;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Hashtable;
import java.util.concurrent.TimeoutException;

public final class AmqpConnectionManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmqpConnectionManager.class);

    private static final Hashtable<String, Connection> AMQP_CONNECTIONS = new Hashtable<>();
    private final Config config;

    public AmqpConnectionManager(Config config) {
        this.config = config;
    }

    /**
     * Creates new connection to RabbitMQ server which can be then used for further channel establishment. As the
     * RabbitMQ best practices state, connections contrary to channels are long-lived objects, so they are stored based
     * on the connectionUrl as a unique identifier. Supports SSL connections, but no client certificate. TLS is
     * <strong>trusting</strong> all certificates the RabbitMQ presents, without any validation.
     *
     * @param connectionUrl connection URL for the server.
     * @return connection to a RabbitMQ server.
     */
    public Connection createConnection(String connectionUrl)
    throws AmqpConnectionManagerException {

        return createConnection(connectionUrl, null);
    }

    /**
     * Creates new connection to RabbitMQ server which can be then used for further channel establishment. As the
     * RabbitMQ best practices state, connections contrary to channels are long-lived objects, so they are stored based
     * on the connectionUrl as a unique identifier.
     *
     * @param connectionUrl connection URL for the server.
     * @param amqpSslConfiguration configuration for <strong>client certificate</strong> usage.
     * @return connection to a RabbitMQ server.
     */
    public Connection createConnection(String connectionUrl, AmqpSslConfiguration amqpSslConfiguration)
    throws AmqpConnectionManagerException {
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
                            factory.useSslProtocol(getSslContext(amqpSslConfiguration));
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
            throw new AmqpConnectionManagerException("Unable to create connection to RabbitMQ broker.", e);
        }

        return connection;
    }

    /**
     * Creates the SSL context factory used  by the RabbitMQ connection factory for establishing trusted SSL
     * connections. Note that both the server and client certificate are expected to be stored in JKS.
     *
     * @param amqpSslConfiguration SSL configuration encapsulated.
     * @return SSLContext user for AMQP connection.
     */
    private SSLContext getSslContext(AmqpSslConfiguration amqpSslConfiguration)
    throws AmqpConnectionManagerException {
        try {
            char[] keyPassphrase = amqpSslConfiguration.keyStoragePassword.toCharArray();
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(new FileInputStream(amqpSslConfiguration.keyStorageLocation), keyPassphrase);
            KeyManagerFactory kmf = KeyManagerFactory.getInstance("SunX509");
            kmf.init(ks, keyPassphrase);

            char[] trustPassphrase = amqpSslConfiguration.trustStoragePassword.toCharArray();
            KeyStore tks = KeyStore.getInstance("JKS");
            tks.load(new FileInputStream(amqpSslConfiguration.getTrustStorageLocation()), trustPassphrase);
            TrustManagerFactory tmf = TrustManagerFactory.getInstance("SunX509");
            tmf.init(tks);

            SSLContext c = SSLContext.getInstance("TLSv1.2");
            c.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
            return c;
        } catch (KeyStoreException
                 | IOException
                 | NoSuchAlgorithmException
                 | CertificateException
                 | UnrecoverableKeyException
                 | KeyManagementException e) {
            LOGGER.atError()
                    .setMessage("Exception while initializing SSL context for AMQP connection.")
                    .setCause(e).log();

            throw new AmqpConnectionManagerException("Exception while initializing SSL context.", e);
        }
    }

    /**
     * Class for encapsulating SSL configuration, which would be otherwise very verbose.
     */
    public static class AmqpSslConfiguration {
        private final String keyStorageLocation;
        private final String keyStoragePassword;
        private final String trustStorageLocation;
        private final String trustStoragePassword;

        public AmqpSslConfiguration(
                String keyStorageLocation,
                String keyStoragePassword,
                String trustStorageLocation,
                String trustStoragePassword) {

            this.keyStorageLocation = keyStorageLocation;
            this.keyStoragePassword = keyStoragePassword;
            this.trustStorageLocation = trustStorageLocation;
            this.trustStoragePassword = trustStoragePassword;
        }

        public String getKeyStorageLocation() {
            return keyStorageLocation;
        }

        public String getKeyStoragePassword() {
            return keyStoragePassword;
        }

        public String getTrustStorageLocation() {
            return trustStorageLocation;
        }

        public String getTrustStoragePassword() {
            return trustStoragePassword;
        }
    }

    public static class AmqpConnectionManagerException extends Exception {
        public AmqpConnectionManagerException() {
        }

        public AmqpConnectionManagerException(String message) {
            super(message);
        }

        public AmqpConnectionManagerException(String message, Throwable cause) {
            super(message, cause);
        }

        public AmqpConnectionManagerException(Throwable cause) {
            super(cause);
        }

        public AmqpConnectionManagerException(
                String message,
                Throwable cause,
                boolean enableSuppression,
                boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}
