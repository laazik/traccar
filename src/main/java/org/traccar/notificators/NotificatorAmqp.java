package org.traccar.notificators;

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
import org.traccar.model.Position;
import org.traccar.model.User;
import org.traccar.notification.MessageException;
import org.traccar.notification.NotificationFormatter;
import org.traccar.notification.NotificationMessage;

import java.io.IOException;

public class NotificatorAmqp extends Notificator {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificatorAmqp.class);

    private final AmqpClient amqpClient;
    private final ObjectMapper objectMapper;

    @Inject
    public NotificatorAmqp(
            NotificationFormatter notificationFormatter,
            String templatePath,
            AmqpConnectionManager amqpConnectionManager,
            ObjectMapper objectMapper,
            Config config) {
        super(notificationFormatter, templatePath);

        this.objectMapper = objectMapper;

        String connectionUrl = config.getString(Keys.NOTIFICATOR_AMQP_URL);
        String exchange = config.getString(Keys.NOTIFICATOR_AMQP_EXCHANGE);
        String topic = config.getString(Keys.NOTIFICATOR_AMQP_TOPIC);
        LOGGER.atDebug()
                .setMessage("Creating connection to RabbitMQ. URL = {}, Exchange = {}, Topic = {}")
                .addArgument(connectionUrl).addArgument(exchange).addArgument(topic).log();

        // As notifications can be user configurable, it makes no sense to throw runtime exception here when the
        // whole connection establishment fails. Thus in this case initialize the client to null AND
        // do a null check when sending.
        AmqpClient client;
        try {
            Connection connection = amqpConnectionManager.createConnection(connectionUrl);
            client = new AmqpClient(connection, exchange, topic);
        } catch (AmqpConnectionManager.AmqpConnectionManagerException e) {
            LOGGER.atWarn()
                    .setMessage("Unable to create connection to RabbitMQ host. URL={}")
                    .addArgument(connectionUrl).setCause(e).log();
            client = null;
        }
        amqpClient = client;
    }

    @Override
    public void send(User user, NotificationMessage message, Event event, Position position) throws MessageException {
        if (amqpClient == null) {
            LOGGER.atWarn()
                    .setMessage("AMQP client initialization has failed. The message WILL NOT be sent.")
                    .log();
            return;
        }

        try {
            String json = objectMapper.writeValueAsString(new AmqpNotificationObject(user, message, event, position));
            amqpClient.publishMessage(json);
        } catch (IOException e) {
            LOGGER.atWarn()
                    .setMessage("IOException when trying to publish notification. user = {}, message = {}, "
                            + "eventId = {}. positionId = {}")
                    .addArgument(user.getId()).addArgument(message.getBody()).addArgument(event.getId())
                    .addArgument(position.getId()).setCause(e).log();
        }
    }

    protected static class AmqpNotificationObject {
        private final User user;
        private final NotificationMessage message;
        private final Event event;
        private final Position position;

        public AmqpNotificationObject(User user, NotificationMessage message, Event event, Position position) {
            this.user = user;
            this.message = message;
            this.event = event;
            this.position = position;
        }

        public User getUser() {
            return user;
        }

        public NotificationMessage getMessage() {
            return message;
        }

        public Event getEvent() {
            return event;
        }

        public Position getPosition() {
            return position;
        }
    }
}
