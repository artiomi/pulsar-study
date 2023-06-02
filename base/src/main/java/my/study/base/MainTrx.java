package my.study.base;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClientException.InvalidTopicNameException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;

public class MainTrx {

  public static void main(String[] args) throws PulsarClientException, ExecutionException, InterruptedException {

    PulsarClient client = PulsarClient.builder()
        // Step 3: create a Pulsar client and enable transactions.
        .enableTransaction(true)
        .serviceUrl(ClientUtils.PULSAR_URL)
        .build();

    // Step 4: create three producers to produce messages to input and output topics.
    ProducerBuilder<String> producerBuilder = client.newProducer(Schema.STRING);
    Producer<String> inputProducer = producerBuilder.topic("trx_test_01")
        .sendTimeout(0, TimeUnit.SECONDS).create();
    Producer<String> outputProducerOne = producerBuilder.topic("trx_test_02")
        .sendTimeout(0, TimeUnit.SECONDS).create();
    Producer<String> outputProducerTwo = producerBuilder.topic("trx_test_03")
        .sendTimeout(0, TimeUnit.SECONDS).create();
    // Step 4: create three consumers to consume messages from input and output topics.
    Consumer<String> inputConsumer = client.newConsumer(Schema.STRING)
        .subscriptionName("trx-subscription-01").topic("trx_test_01").subscribe();
    Consumer<String> outputConsumerOne = client.newConsumer(Schema.STRING)
        .subscriptionName("trx-subscription-02").topic("trx_test_02").subscribe();
    Consumer<String> outputConsumerTwo = client.newConsumer(Schema.STRING)
        .subscriptionName("trx-subscription-03").topic("trx_test_03").subscribe();

    int count = 2;
    // Step 5: produce messages to input topics.
    for (int i = 0; i < count; i++) {
      inputProducer.send("Hello Pulsar! count : " + i);
    }

    // Step 5: consume messages and produce them to output topics with transactions.
    for (int i = 0; i < count; i++) {
      System.out.println("Consume message for i:" + i);

      // Step 5: the consumer successfully receives messages.
      Message<String> message = inputConsumer.receive();

      // Step 6: create transactions.
      // The transaction timeout is specified as 10 seconds.
      // If the transaction is not committed within 10 seconds, the transaction is automatically aborted.
      Transaction txn = null;
      try {
        txn = client.newTransaction()
            .withTransactionTimeout(10, TimeUnit.SECONDS).build().get();
        // Step 6: you can process the received message with your use case and business logic.

        // Step 7: the producers produce messages to output topics with transactions
        MessageId messageId = outputProducerOne.newMessage(txn).value("Hello Pulsar! outputTopicOne count : " + i)
            .send();
        System.out.println("produced message: " + messageId);
        messageId = outputProducerTwo.newMessage(txn).value("Hello Pulsar! outputTopicTwo count : " + i).send();
        System.out.println("produced message: " + messageId);

        // Step 7: the consumers acknowledge the input message with the transactions *individually*.
        inputConsumer.acknowledgeAsync(message.getMessageId(), txn).get();
        // Step 8: commit transactions.
//        if (i == 0) {
//          throw new InterruptedException("Custom interruption");
//        }
        txn.commit().get();
      } catch (InterruptedException | ExecutionException e) {
        if (!(e.getCause() instanceof PulsarClientException.TransactionConflictException)) {
          // If TransactionConflictException is not thrown,
          // you need to redeliver or negativeAcknowledge this message,
          // or else this message will not be received again.
          System.out.println("Negative ack");
          inputConsumer.negativeAcknowledge(message);
        }

        // If a new transaction is created,
        // then the old transaction should be aborted.
        if (txn != null) {
          txn.abort().get();
          System.err.println("Transaction aborted: " + e);
        }
      }
    }
    // Final result: consume messages from output topics and print them.
    while (true) {
      Message<String> message = outputConsumerOne.receive(1, TimeUnit.SECONDS);
      if (message == null) {
        break;
      }
      System.out.println(
          "Receive transaction message: " + message.getValue() + " id:" + message.getMessageId());
      outputConsumerOne.acknowledge(message);
    }

    while (true) {
      Message<String> message = outputConsumerTwo.receive(1, TimeUnit.SECONDS);
      if (message == null) {
        break;
      }
      System.out.println("Receive transaction message: " + message.getValue() + " id:" + message.getMessageId());
      outputConsumerTwo.acknowledge(message);
    }
  }
}

