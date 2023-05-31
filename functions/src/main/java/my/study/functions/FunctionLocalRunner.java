package my.study.functions;

import java.util.List;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionConfig.Runtime;
import org.apache.pulsar.functions.LocalRunner;
import org.apache.pulsar.functions.LocalRunner.LocalRunnerBuilder;

public class FunctionLocalRunner {

  public static final String INPUT_TOPIC = "persistent://test/test-namespace/counter-input";
  public static final String PULSAR_HOST = "pulsar://localhost:6650";

  public static void main(String[] args) throws Exception {
    FunctionConfig functionConfig = FunctionConfig.builder()
        .name("local_agg-count-func")
        .inputs(List.of(INPUT_TOPIC))
        .className(AggregationFunction.class.getName())
        .runtime(Runtime.JAVA)
        .tenant("test")
        .namespace("test-namespace")
        .output("persistent://test/test-namespace/counter-out")
        .build();

    try (PulsarClient pulsarClient = PulsarClient.builder().serviceUrl(PULSAR_HOST).build();
        Producer<String> producer = pulsarClient.newProducer(Schema.STRING).topic(INPUT_TOPIC).create()) {
      producer.newMessage().value("Hello").send();
    }

    LocalRunnerBuilder builder = LocalRunner.builder()
        .brokerServiceUrl(PULSAR_HOST)
        .stateStorageServiceUrl("bk://localhost:4181")
        .functionConfig(functionConfig);
    try (LocalRunner localRunner = builder.build()) {
      localRunner.start(true);
      System.out.println("Done!!!!!!!!!!!!!!");
    }
    System.out.println("Exit!!!!!!!!!!!!!!");
  }

}
