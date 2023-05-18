package my.study.async;

import static my.study.CommonUtils.logMessage;
import static my.study.CommonUtils.safeSleep;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
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

  public CompletableFuture<Void> exclusiveSubscriptionConsumer(String topicName) throws PulsarClientException {
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-subscription")
        .subscriptionType(SubscriptionType.Exclusive);
    Consumer<String> consumerOne = consumerBuilder.subscribe();
    Consumer<String> consumerTwo = consumerBuilder.subscribe();

    CompletableFuture<Void> consumeResultFirst = consumerOne.receiveAsync()
        .thenAcceptAsync(msg -> {
          log.info("On message consumerOne");
          logMessage(msg);
          consumerOne.acknowledgeAsync(msg)
              .whenComplete((unused, throwable) -> log.error("Message ack done. Exceptions?", throwable));
        })
        .thenCompose(unused -> consumerOne.closeAsync())
        .whenComplete((unused, throwable) -> log.info("consumerOne closed. exceptions?", throwable));

    CompletableFuture<Void> consumeResultSecond = consumerTwo.receiveAsync()
        .thenAcceptAsync(msg -> {
          log.info("On message consumerTwo");
          logMessage(msg);
          consumerTwo.acknowledgeAsync(msg)
              .whenComplete((unused, throwable) -> log.error("Message ack done. Exceptions?", throwable));
        })
        .thenCompose(unused -> consumerTwo.closeAsync())
        .whenComplete((unused, throwable) -> log.info("consumerTwo closed. exceptions?", throwable));

    return CompletableFuture.allOf(consumeResultFirst, consumeResultSecond);
  }

  public CompletableFuture<Void> failoverSubscriptionConsumer(String topicName) throws PulsarClientException {
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-subscription")
        .subscriptionType(SubscriptionType.Failover);
    Consumer<String> consumerOne = consumerBuilder.subscribe();
    Consumer<String> consumerTwo = consumerBuilder.subscribe();

    CompletableFuture<Void> consumeResultFirst = consumerOne.receiveAsync()
        .thenAcceptAsync(msg -> {
//          safeSleep(1);
          log.info("On message consumerOne");
          logMessage(msg);
          consumerOne.acknowledgeAsync(msg)
              .whenComplete((unused, throwable) -> log.error("consumerOne message ack done. Exceptions?", throwable));
        })
        .thenCompose(unused -> consumerOne.closeAsync())
        .whenComplete((unused, throwable) -> log.info("consumerOne closed. exceptions?", throwable));

    CompletableFuture<Void> consumeResultSecond = consumerTwo.receiveAsync()
        .thenAcceptAsync(msg -> {
          log.info("On message consumerTwo");
          logMessage(msg);
          consumerTwo.acknowledgeAsync(msg)
              .whenComplete((unused, throwable) -> log.error("consumerTwo message ack done. Exceptions?", throwable));
        })
        .thenCompose(unused -> consumerTwo.closeAsync())
        .whenComplete((unused, throwable) -> log.info("consumerTwo closed. exceptions?", throwable));

    return CompletableFuture.allOf(consumeResultFirst, consumeResultSecond);
  }

  public CompletableFuture<Void> sharedSubscriptionConsumer(String topicName) throws PulsarClientException {

    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-subscription")
        .subscriptionType(SubscriptionType.Shared);
    Consumer<String> consumerOne = consumerBuilder.subscribe();
    Consumer<String> consumerTwo = consumerBuilder.subscribe();

    CompletableFuture<Void> consumeResultFirst = consumerOne.receiveAsync()
        .thenAcceptAsync(msg -> {
          safeSleep(5);
          log.info("On message consumerOne");
          logMessage(msg);
          consumerOne.acknowledgeAsync(msg)
              .whenComplete((unused, throwable) -> log.error("consumerOne message ack done. Exceptions?", throwable));
        })
        .thenCompose(unused -> consumerOne.closeAsync())
        .whenComplete((unused, throwable) -> log.info("consumerOne closed. exceptions?", throwable));

    CompletableFuture<Void> consumeResultSecond = consumerTwo.receiveAsync()
        .thenAcceptAsync(msg -> {
          log.info("On message consumerTwo");
          logMessage(msg);
          consumerTwo.acknowledgeAsync(msg)
              .whenComplete((unused, throwable) -> log.error("consumerTwo message ack done. Exceptions?", throwable));
        })
        .thenCompose(unused -> consumerTwo.closeAsync())
        .whenComplete((unused, throwable) -> log.info("consumerTwo closed. exceptions?", throwable));

    return CompletableFuture.allOf(consumeResultFirst, consumeResultSecond);
  }
}
