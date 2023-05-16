package my.study.async;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumerAsync {

  public static final int MAX_WORKERS = 2;
  private static final Logger log = LoggerFactory.getLogger(MessageConsumerAsync.class);
  private final PulsarClient pulsarClient;
  Semaphore semaphore = new Semaphore(MAX_WORKERS);

  public MessageConsumerAsync(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  private static void logMessage(Message<String> message) {
    log.info("Message consumed. Id {}, value: {}, topic: {}, time: {} ", message.getMessageId(), message.getValue(),
        message.getTopicName(), Instant.ofEpochMilli(message.getPublishTime()));
  }

  public CompletableFuture<Void> consume(String topicName, long timeoutSec) {
    return pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-async-subscription")
        .subscribeAsync()
        .thenApplyAsync(consumer -> {
          log.info("Start consumer.");
          consumeInternal(timeoutSec, consumer);
          return consumer;
        })
        .thenCompose(Consumer::closeAsync)
        .whenComplete((unused, throwable) ->
        {
          if (throwable != null) {
            log.error("Exception occurred while closing consumer.", throwable);
          } else {
            log.info("consumer closed successfully");
          }
        });


  }

  private void consumeInternal(long timeoutSec, Consumer<String> consumer) {
    AtomicInteger exited = new AtomicInteger();
    while (true) {
      log.info("before semaphore, available permits:{}", semaphore.availablePermits());
      try {
        semaphore.acquire();
      } catch (InterruptedException e) {
        log.error("Thread interrupted", e);
        Thread.currentThread().interrupt();
        return;
      }
      log.info("after semaphore");

      if (exited.get() >= MAX_WORKERS) {
        log.info("time to exit!!!");
        return;
      }

      consumer.receiveAsync()
          .thenApply(m -> {
            logMessage(m);
            return m;
          })
          .thenCompose(consumer::acknowledgeAsync)
          .orTimeout(timeoutSec, TimeUnit.SECONDS)
          .whenComplete((unused, throwable) -> {
            if (throwable != null) {
              log.error("Boom exception.", throwable);
              if (exited.incrementAndGet() >= MAX_WORKERS) {
                semaphore.release(MAX_WORKERS);
                log.info("releasing all workers");
              }
            } else {
              semaphore.release();
              log.info("Semaphore released");
            }
          });
    }
  }
}
