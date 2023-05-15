package my.study;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  public static final String NON_PART_TOPIC_NAME = "persistent://study/home/my-topic";
  public static final String PART_TOPIC_NAME = "persistent://study/home/first_topic";

  public static final String PULSAR_URL = "pulsar://localhost:6650";
  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws PulsarClientException {
    log.info("Start");
    try (PulsarClient pulsarClient = initClient()) {

      MessageProducer messageProducer = new MessageProducer(pulsarClient);
      messageProducer.produce(PART_TOPIC_NAME, "My message");

      MessageConsumer messageConsumer = new MessageConsumer(pulsarClient);
      messageConsumer.consume();

      MessageReader reader = new MessageReader(pulsarClient);
      reader.read();
    }
    log.info("End");
  }

  private static PulsarClient initClient() throws PulsarClientException {
    PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl(PULSAR_URL)
        .build();

    log.info("Pulsar client initialized for URL:{}", PULSAR_URL);
    return pulsarClient;

  }
}