package my.study;

import my.study.models.User;
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
        .intercept(new CustomProducerInterceptor())
        .topic(topicName);
    try (Producer<String> producer = producerBuilder.create()) {
      MessageId messageId = producer.newMessage()
          .value(message)
//          .deliverAfter(15, TimeUnit.SECONDS)
          .send();
      log.info("Message produced:{}", messageId);
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  public void produceToPartitioned(String topicName, String message, String key) {
    ProducerBuilder<String> producerBuilder = pulsarClient.newProducer(Schema.STRING)
//        .messageRouter(new AlwaysTwoRouter())
        .topic(topicName);
    try (Producer<String> producer = producerBuilder.create()) {
      MessageId messageId = producer.newMessage()
          .value(message)
          .key(key)
          .send();
      log.info("Message produced:{}", messageId);
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

  public void produceWithSchema(String topicName, User user) {
    ProducerBuilder<User> producerBuilder = pulsarClient.newProducer(Schema.AVRO(User.class))
        .topic(topicName);
    try (Producer<User> producer = producerBuilder.create()) {
      MessageId messageId = producer.newMessage()
          .value(user)
          .send();
      log.info("Message produced:{}", messageId);
    } catch (PulsarClientException e) {
      throw new RuntimeException(e);
    }
  }

}
