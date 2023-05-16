package my.study.async;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducerAsync {

  private static final Logger log = LoggerFactory.getLogger(MessageProducerAsync.class);
  private final PulsarClient pulsarClient;

  public MessageProducerAsync(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }


  public CompletableFuture<Void> produce(String topicName, String message) {
    ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING)
        .topic(topicName);
    return producerBuilder.createAsync()
        .thenComposeAsync(producer -> sendInternal(producer, message)
            .thenAccept(messageId -> producer.closeAsync()
                .whenComplete((res, ex) -> {
                  if (ex != null) {
                    log.error("Exception occurred during producer close.", ex);
                  } else {
                    log.info("Producer closed successfully.");
                  }
                })));

  }

  private CompletableFuture<MessageId> sendInternal(Producer<String> producer, String message) {
    log.info("Attempt to publish message.");
    return producer.sendAsync(message)
        .whenComplete((messageId, throwable) -> {
          if (throwable != null) {
            log.error("Publishing failed.", throwable);
          } else {
            log.info("Message produced:[{}] successfully", messageId);
          }
        });
  }
}
