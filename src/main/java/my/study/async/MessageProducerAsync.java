package my.study.async;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import my.study.CustomProducerInterceptor;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerAccessMode;
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

  public CompletableFuture<Void> produceInBatch(String topicName, List<String> messages) {
    ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING)
        .batchingMaxMessages(3)
        .compressionType(CompressionType.LZ4)
        .intercept(new CustomProducerInterceptor())
        .topic(topicName);
    return producerBuilder.createAsync()
        .thenComposeAsync(producer -> sendInBatchesInternal(producer, messages)
            .thenAccept(messageId -> producer.closeAsync()
                .whenComplete((res, ex) -> {
                  if (ex != null) {
                    log.error("Exception occurred during producer close.", ex);
                  } else {
                    log.info("Producer closed successfully.");
                  }
                })));

  }

  private CompletableFuture<Void> sendInBatchesInternal(Producer<String> producer, List<String> messages) {

    return CompletableFuture.allOf(messages.stream().map(producer::sendAsync).toArray(CompletableFuture<?>[]::new))
        .whenComplete((messageId, throwable) -> {
          if (throwable != null) {
            log.error("Publishing failed.", throwable);
          } else {
            log.info("Message produced:[{}] successfully", messageId);
          }
        });
  }

  private CompletableFuture<MessageId> sendInternal(Producer<String> producer, String message) {

    return producer.sendAsync(message)
        .whenComplete((messageId, throwable) -> {
          if (throwable != null) {
            log.error("Publishing failed.", throwable);
          } else {
            log.info("Message produced:[{}] successfully", messageId);
          }
        });
  }

  public void producersWithDifferentAccessMode(String topicName, String message) {
    CompletableFuture<Void> firstProducer = pulsarClient.newProducer(Schema.STRING)
        .accessMode(ProducerAccessMode.Exclusive)
        .intercept(new CustomProducerInterceptor())
        .topic(topicName)
        .createAsync()
        .thenAcceptAsync(producer -> {
          try {
            TimeUnit.SECONDS.sleep(5);
            log.info("Is producer still connected? {}", producer.isConnected());
            producer.newMessage()
                .value(message)
                .key("first producer")
                .send();
//            producer.close(); //only when secondProducer has WaitForExclusive
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })
        .whenComplete((unused, throwable) -> log.info("First Done! Exceptions?", throwable));

    CompletableFuture<Void> secondProducer = pulsarClient.newProducer(Schema.STRING)
        .accessMode(ProducerAccessMode.WaitForExclusive)
        .topic(topicName)
        .intercept(new CustomProducerInterceptor())
        .createAsync()
        .thenAcceptAsync(producer -> {
          try {
            producer.newMessage()
                .value(message)
                .key("second producer")
                .send();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        })
        .whenComplete((stringProducer, throwable) -> log.info("Second complete. Exceptions?", throwable));

    CompletableFuture.allOf(firstProducer, secondProducer)
        .whenComplete((unused, throwable) -> log.info("Both producer completed. Any exceptions?", throwable))
        .join();

  }
}
