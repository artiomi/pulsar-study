package my.study;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.avro.reflect.AvroSchema;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

public class SinkEventsProducer {

  public static void main(String[] args) throws PulsarClientException {
    try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl("pulsar://localhost:6650").build();
        Producer<User> producer = pulsarClient.newProducer(Schema.AVRO(User.class))
            .topic("public/default/pulsar-postgres-jdbc-sink-topic").create()
    ) {
      MessageId messageId = producer.newMessage()
          .value(new User(1, "hello world"))
          .send();
      System.out.println("Message published: " + messageId);
    }
  }
}

@Getter
@AllArgsConstructor
@AvroSchema("{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"id\",\"type\":[\"null\",\"int\"]},{\"name\":\"name\",\"type\":[\"null\",\"string\"]}]}")
class User {

  private int id;
  private String name;
}