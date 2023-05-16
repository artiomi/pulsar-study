package my.study;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumer {

  private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);
  private final PulsarClient pulsarClient;

  public MessageConsumer(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  private static void logMessage(Message<String> message) {
    log.info("Message consumed. Id {}, value: {}, topic: {}, time: {} ", message.getMessageId(), message.getValue(),
        message.getTopicName(), Instant.ofEpochMilli(message.getPublishTime()));
  }

  public void consume(String topicName) {
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-subscription");
    try (Consumer<String> consumer = consumerBuilder.subscribe()) {
      while (true) {
        Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
        if (message == null) {
          log.info("consumer closed!");
          return;
        }
        logMessage(message);
        consumer.acknowledge(message);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
