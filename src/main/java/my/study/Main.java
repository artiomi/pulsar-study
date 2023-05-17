package my.study;

import static my.study.ClientUtils.NON_PART_TOPIC_NAME;
import static my.study.ClientUtils.initClient;

import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    log.info("Start");
    try (PulsarClient pulsarClient = initClient()) {

      MessageProducer messageProducer = new MessageProducer(pulsarClient);
      messageProducer.produce(NON_PART_TOPIC_NAME, "My message");
      messageProducer.produceToPartitioned(ClientUtils.PART_TOPIC_NAME, "My partitioned message", "routing key");

      MessageConsumer messageConsumer = new MessageConsumer(pulsarClient);
      messageConsumer.consume(NON_PART_TOPIC_NAME);

      MessageReader reader = new MessageReader(pulsarClient);
      reader.read();
    }
    log.info("End");
  }

}