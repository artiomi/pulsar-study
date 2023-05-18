package my.study;

import static my.study.CommonUtils.logMessage;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageListener;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MultiplierRedeliveryBackoff;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageConsumer {

  private static final Logger log = LoggerFactory.getLogger(MessageConsumer.class);
  private final PulsarClient pulsarClient;

  public MessageConsumer(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  public void consume(String topicName) {
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-subscription");
    try (Consumer<String> consumer = consumerBuilder.subscribe()) {
      while (true) {
        Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
        if (message == null) {
          log.info("consumer closed!");
          return;
        }
        logMessage(message);
        consumer.acknowledge(message);
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void batchConsume(String topicName) {
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-batch-subscription")
        .batchReceivePolicy(BatchReceivePolicy.builder()
            .maxNumMessages(5)
            .timeout(5, TimeUnit.SECONDS)
            .build()
        );
    try (Consumer<String> consumer = consumerBuilder.subscribe()) {
      while (true) {
        AtomicReference<MessageId> latestMessageId = new AtomicReference<>();
        Messages<String> messages = consumer.batchReceive();
        if (messages == null || messages.size() == 0) {
          log.info("No more messages, consumer closed!");
          return;
        }
        log.info("Next batch received.");
        messages.forEach(m -> {
          logMessage(m);
          latestMessageId.set(m.getMessageId());
        });
//        consumer.acknowledge(messages);
        consumer.acknowledgeCumulative(latestMessageId.get());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void consumeWithRedelivery(String topicName) {
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionType(SubscriptionType.Shared)
        .subscriptionName("my-subscription")
        .negativeAckRedeliveryBackoff(MultiplierRedeliveryBackoff.builder()
            .maxDelayMs(60 * 10000)
            .minDelayMs(1000)
            .multiplier(2.0)
            .build()
        );
    try (Consumer<String> consumer = consumerBuilder.subscribe();
        Consumer<String> consumerTwo = consumerBuilder.subscribe()) {
      //first consumer
      Message<String> message = consumer.receive(5, TimeUnit.SECONDS);
      if (message == null) {
        log.info("consumer closed!");
        return;
      }
      logMessage(message);
      consumer.negativeAcknowledge(message);
      log.info("ack negative, current redelivery count: {}", message.getRedeliveryCount());
      //second consumer
      message = consumerTwo.receive(5, TimeUnit.SECONDS);
      if (message == null) {
        log.info("consumer closed!");
        return;
      }
      logMessage(message);
      log.info(" current redelivery count: {}",
          message.getRedeliveryCount());//counter increases only when consumer change
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void consumeWithListener(String topicName) {
    AtomicReference<MessageId> latestMessage = new AtomicReference<>();
    ConsumerBuilder<String> consumerBuilder = pulsarClient.newConsumer(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-listener-subscription")
        .messageListener((MessageListener<String>) (consumer, msg) -> {
          latestMessage.set(msg.getMessageId());
          logMessage(msg);
          try {
            consumer.acknowledge(msg);
          } catch (PulsarClientException e) {
            log.error("Acknowledge of message:{} failed.", msg.getMessageId(), e);
          }
        });

    try (Consumer<String> consumer = consumerBuilder.subscribe()) {
      MessageId previousMessage = null;
      while (latestMessage.get() == null || !latestMessage.get().equals(previousMessage)) {
        log.info("Still consuming messages");
        CommonUtils.safeSleep(5);
        previousMessage = latestMessage.get();
      }
      log.info("consumption done.");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
