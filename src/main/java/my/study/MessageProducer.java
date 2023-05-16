package my.study;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageProducer {

  private static final Logger log = LoggerFactory.getLogger(MessageProducer.class);
  private final PulsarClient pulsarClient;

  public MessageProducer(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  public void produce(String topicName, String message) {
    ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING)
        .topic(topicName);
    try (Producer<String> producer = producerBuilder.create()) {
      MessageId messageId = producer.send(message);
      log.info("Message produced:{}", messageId);
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

}
