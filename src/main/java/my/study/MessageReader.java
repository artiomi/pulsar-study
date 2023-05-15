package my.study;

import java.time.Instant;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.api.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageReader {

  private static final Logger log = LoggerFactory.getLogger(MessageReader.class);


  private final PulsarClient pulsarClient;

  public MessageReader(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  public void read() {
    ReaderBuilder<String> readerBuilder = pulsarClient.newReader(Schema.STRING)
        .topic(Main.NON_PART_TOPIC_NAME)
        .startMessageId(MessageId.earliest)
        .subscriptionName("my-reader");
    try (Reader<String> reader = readerBuilder.create()) {
      while (reader.hasMessageAvailable()) {
        Message<String> message = reader.readNext();

        log.info("Message read. Id {}, value: {}, topic: {}, time: {} ", message.getMessageId(), message.getValue(),
            message.getTopicName(), Instant.ofEpochMilli(message.getPublishTime()));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
