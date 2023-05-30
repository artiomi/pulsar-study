package my.study.base;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.api.TableViewBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageTableView {

  private static final Logger log = LoggerFactory.getLogger(MessageTableView.class);


  private final PulsarClient pulsarClient;

  public MessageTableView(PulsarClient pulsarClient) {
    this.pulsarClient = pulsarClient;
  }

  public void readAsTableView(String topicName) {
    TableViewBuilder<String> tableViewBuilder = pulsarClient.newTableView(Schema.STRING)
        .topic(topicName)
        .subscriptionName("my-tableview");

    try (TableView<String> tableView = tableViewBuilder.create()) {
      log.info("Table view size:{}", tableView.size());
      tableView.forEachAndListen((k, v) -> log.info("Received message with key:[{}] and value:[{}]", k, v));
      log.info("TableView iteration complete.");

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
