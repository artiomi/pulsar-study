package my.study.async;

import static my.study.ClientUtils.NON_PART_TOPIC_NAME;
import static my.study.ClientUtils.PART_TOPIC_NAME;
import static my.study.ClientUtils.initClient;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainAsync {


  private static final Logger log = LoggerFactory.getLogger(MainAsync.class);

  public static void main(String[] args) {
    log.info("Start");
    try (PulsarClient pulsarClient = initClient()) {

      MessageProducerAsync asyncProducer = new MessageProducerAsync(pulsarClient);
//      asyncProducer.producersWithDifferentAccessMode(NON_PART_TOPIC_NAME, "My message");
//      CompletableFuture<Void> publishEvents = CompletableFuture.allOf(
//          IntStream.range(0, 5)
//              .mapToObj(i -> asyncProducer.produce(NON_PART_TOPIC_NAME, "My message: " + 1))
//              .toArray(CompletableFuture<?>[]::new)
//      );

      List<String> messages = IntStream.range(0, 5).mapToObj(i -> "My message: " + 1).toList();
      CompletableFuture<Void> publishEvents = asyncProducer.produceInBatch(PART_TOPIC_NAME, messages);

      MessageConsumerAsync asyncConsumer = new MessageConsumerAsync(pulsarClient);

      CompletableFuture<Void> consumeEvents = asyncConsumer.consume(PART_TOPIC_NAME, 5);
//      CompletableFuture<Void> consumeEvents = asyncConsumer.exclusiveSubscriptionConsumer(NON_PART_TOPIC_NAME);
//      CompletableFuture<Void> consumeEvents = asyncConsumer.failoverSubscriptionConsumer(PART_TOPIC_NAME);
//      CompletableFuture<Void> consumeEvents = asyncConsumer.sharedSubscriptionConsumer(PART_TOPIC_NAME);
//      CompletableFuture<Void> consumeEvents = asyncConsumer.batchConsume(NON_PART_TOPIC_NAME, 5);

      CompletableFuture.allOf(publishEvents, consumeEvents).join();

      log.info("End");
    } catch (Exception e) {
      log.error("Something went wrong.", e);
    }
  }
}