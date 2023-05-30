package my.study.base;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageRouter;
import org.apache.pulsar.client.api.TopicMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AlwaysTwoRouter implements MessageRouter {

  private static final Logger log = LoggerFactory.getLogger(AlwaysTwoRouter.class);

  @Override
  public int choosePartition(Message<?> msg, TopicMetadata metadata) {
    log.info("Choose partition request for message:[{}] and topic partitions:[{}] received.", msg.getValue(), metadata.numPartitions());
    return 2;
  }
}
