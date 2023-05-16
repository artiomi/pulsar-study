package my.study.async;

import static my.study.ClientUtils.NON_PART_TOPIC_NAME;
import static my.study.ClientUtils.initClient;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MainAsync {


  private static final Logger log = LoggerFactory.getLogger(MainAsync.class);

  public static void main(String[] args) throws Exception {
    log.info("Start");
    try (PulsarClient pulsarClient = initClient()) {

      MessageProducerAsync asyncProducer = new MessageProducerAsync(pulsarClient);
      CompletableFuture<Void> publishEvents = CompletableFuture.allOf(
          CompletableFuture.supplyAsync(() -> asyncProducer.produce(NON_PART_TOPIC_NAME, "My message")),
          CompletableFuture.supplyAsync(() -> asyncProducer.produce(NON_PART_TOPIC_NAME, "My message")),
          CompletableFuture.supplyAsync(() -> asyncProducer.produce(NON_PART_TOPIC_NAME, "My message")),
          CompletableFuture.supplyAsync(() -> asyncProducer.produce(NON_PART_TOPIC_NAME, "My message")),
          CompletableFuture.supplyAsync(() -> asyncProducer.produce(NON_PART_TOPIC_NAME, "My message"))
      );

      MessageConsumerAsync asyncConsumer = new MessageConsumerAsync(pulsarClient);

      CompletableFuture<Void> consumeEvents = asyncConsumer.consume(NON_PART_TOPIC_NAME, 5);
      CompletableFuture.allOf(publishEvents, consumeEvents).join();
      log.info("End");

    }
  }
}