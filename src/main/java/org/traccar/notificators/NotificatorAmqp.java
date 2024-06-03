package org.traccar.notificators;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Connection;
import jakarta.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.traccar.config.Config;
import org.traccar.config.Keys;
import org.traccar.helper.AmqpClient;
import org.traccar.helper.AmqpConnectionManager;
import org.traccar.model.Event;
import org.traccar.model.Notification;
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;

import java.io.IOException;

public class NotificatorAmqp extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorAmqp.class);

    private final ObjectMapper objectMapper;
    private final String connectionUrl;
    private final String exchange;
    private final String topic;

    private final AmqpConnectionManager connectionManager;

    @Inject
    public NotificatorAmqp(
            NotificationFormatter notificationFormatter,
            String templatePath,
            AmqpConnectionManager amqpConnectionManager,
            ObjectMapper objectMapper,
            Config config) {
        super(null, null);

        this.objectMapper = objectMapper;

        this.connectionUrl = config.getString(Keys.NOTIFICATOR_AMQP_URL);
        this.exchange = config.getString(Keys.NOTIFICATOR_AMQP_EXCHANGE);
        this.topic = config.getString(Keys.NOTIFICATOR_AMQP_TOPIC);

        this.connectionManager = amqpConnectionManager;

        LOGGER.atDebug()
                .setMessage("Creating connection to RabbitMQ. URL = {}, Exchange = {}, Topic = {}")
                .addArgument(this.connectionUrl).addArgument(this.exchange).addArgument(this.topic).log();
    }

    @Override
    public void send(Notification notification, User user, Event event, Position position) throws MessageException {
        AmqpClient amqpClient = null;

        try {
            Connection connection = connectionManager.createConnection(connectionUrl);
            amqpClient = new AmqpClient(connection, exchange, topic);

            String json = objectMapper
                    .writeValueAsString(new AmqpNotificationObject(user, notification, event, position));
            amqpClient.publishMessage(json);
        } catch (AmqpConnectionManager.AmqpConnectionManagerException | IOException e) {
            LOGGER.atWarn()
                    .setMessage("IOException when trying to publish notification. user = {}, notification = {}, "
                            + "eventId = {}. positionId = {}")
                    .addArgument(user.getId())
                    .addArgument(notification.getId()).addArgument(event.getId())
                    .addArgument(position.getId())
                    .setCause(e).log();

            throw new MessageException(e);
        } finally {
            if (amqpClient != null) {
                amqpClient.close();
            }
        }
    }

    protected static class AmqpNotificationObject {
        private final User user;
        private final Notification message;
        private final Event event;
        private final Position position;

        public AmqpNotificationObject(User user, Notification message, Event event, Position position) {
            this.user = user;
            this.message = message;
            this.event = event;
            this.position = position;
        }
        @JsonProperty("user")
        public User getUser() {
            return user;
        }

        @JsonProperty("notification")
        public Notification getMessage() {
            return message;
        }

        @JsonProperty("event")
        public Event getEvent() {
            return event;
        }

        @JsonProperty("position")
        public Position getPosition() {
            return position;
        }
    }
}
