package my.study;

import static my.study.ClientUtils.USER_TOPIC_NAME;
import static my.study.ClientUtils.initClient;

import java.util.UUID;
import my.study.models.User;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) throws Exception {
    log.info("Start");
    try (PulsarClient pulsarClient = initClient()) {

      MessageProducer messageProducer = new MessageProducer(pulsarClient);
//      IntStream.range(1, 10).forEach(value -> messageProducer.produce(NON_PART_TOPIC_NAME, "My message N" + value));
//      messageProducer.produceToPartitioned(ClientUtils.PART_TOPIC_NAME, "My partitioned message", "routing key");
//      messageProducer.produce(NON_PART_TOPIC_NAME, "My message for redelivery");
//      IntStream.range(1, 10).forEach(
//          value -> messageProducer.produceToPartitioned(PART_TOPIC_NAME, "My message N" + value, "k_" + value));
      messageProducer.produceWithSchema(USER_TOPIC_NAME, new User(UUID.randomUUID().toString(), "jhony"));

      MessageConsumer messageConsumer = new MessageConsumer(pulsarClient);
//      messageConsumer.consume(NON_PART_TOPIC_NAME);
//      messageConsumer.batchConsume(NON_PART_TOPIC_NAME);
//      messageConsumer.consumeWithRedelivery(NON_PART_TOPIC_NAME);
//      messageConsumer.consumeWithListener(NON_PART_TOPIC_NAME);
//      messageConsumer.consumeWithRetryTopic(PART_TOPIC_NAME);
//      messageConsumer.consumeWithDLQTopic(PART_TOPIC_NAME);
      messageConsumer.consumeWithSchema(USER_TOPIC_NAME);

//      MessageReader reader = new MessageReader(pulsarClient);
//      reader.read();

//      MessageTableView messageTableView = new MessageTableView(pulsarClient);
//      messageTableView.readAsTableView(PART_TOPIC_NAME);
    }
    log.info("End");
  }

}