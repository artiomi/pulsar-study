package my.study;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientUtils {

  public static final String NON_PART_TOPIC_NAME = "persistent://study/home/my-topic";
  public static final String PART_TOPIC_NAME = "persistent://study/home/my-topic-part";

  public static final String PULSAR_URL = "pulsar://localhost:6650";
  private static final Logger log = LoggerFactory.getLogger(ClientUtils.class);

  private ClientUtils() {
  }

  public static PulsarClient initClient() throws PulsarClientException {
    PulsarClient pulsarClient = PulsarClient.builder()
        .serviceUrl(PULSAR_URL)
        .build();

    log.info("Pulsar client initialized for URL:{}", PULSAR_URL);
    return pulsarClient;

  }
}
