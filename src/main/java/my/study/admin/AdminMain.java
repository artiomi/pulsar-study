package my.study.admin;

import org.apache.pulsar.client.admin.GetStatsOptions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.policies.data.PartitionedTopicStats;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.TopicStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AdminMain {

  public static final String PULSAR_HTTP_URL = "http://localhost:8080";
  public static final String TOPIC_NAME = "persistent://study/home/my-topic";
  private static final Logger log = LoggerFactory.getLogger(AdminMain.class);

  public static void main(String[] args) throws Exception {
    log.info("Start.");
    try (PulsarAdmin adminClient = initClient()) {
//      adminClient.tenants().getTenants().forEach(t -> log.info("Defined tenant: {}", t));
//      adminClient.namespaces().getNamespaces("study").forEach(n -> log.info("Namespace in tenant `study`: {}", n));

//      nonPartitionedTopicsInfo(adminClient);
      partitionedTopicInfo(adminClient);

    }
    log.info("End.");
  }

  private static void partitionedTopicInfo(PulsarAdmin adminClient) throws PulsarAdminException {
    String partitionedTopic =TOPIC_NAME+"-part";
    adminClient.topics().createPartitionedTopic(partitionedTopic, 5);
    adminClient.topics().getPartitionedTopicList("study/home")
        .forEach(t -> log.info("Partitioned topic in namespace `study/home`: {}", t));

    PartitionedTopicStats topicStats = adminClient.topics().getPartitionedStats(partitionedTopic, false);
    log.info("Topic [{}] stats:{}", partitionedTopic, topicStats);
  }

  private static void nonPartitionedTopicsInfo(PulsarAdmin adminClient) throws PulsarAdminException {
    adminClient.topics().getList("study/home")
        .forEach(t -> log.info("Non Partitioned topic in namespace `study/home`: {}", t));
//    adminClient.topics().createNonPartitionedTopic(TOPIC_NAME);

    TopicStats topicStats = adminClient.topics().getStats(TOPIC_NAME,
        GetStatsOptions.builder()
            .getPreciseBacklog(true)
            .getEarliestTimeInBacklog(true)
            .subscriptionBacklogSize(true)
            .build());
    log.info("Topic [{}] stats: {}", TOPIC_NAME, topicStats);

    String internalInfo = adminClient.topics().getInternalInfo(TOPIC_NAME);
    log.info("Topic [{}] internal info: {}", TOPIC_NAME, internalInfo);
    PersistentTopicInternalStats internalStats = adminClient.topics().getInternalStats(TOPIC_NAME, true);
    log.info("Topic [{}] internal stats: {}", TOPIC_NAME, internalStats.numberOfEntries);
  }

  private static PulsarAdmin initClient() throws PulsarClientException {
    PulsarAdmin adminClient = PulsarAdmin.builder()
        .serviceHttpUrl(PULSAR_HTTP_URL)
        .build();
    log.info("AdminClient initialized.");

    return adminClient;
  }


}
