package my.study.functions;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;

public class AggregationFunction implements Function<String, Record<Counter>> {

  @Override
  public Record<Counter> process(String input, Context context) {
    context.incrCounter(input, 1);
    long counter = context.getCounter(input);

    return context.newOutputRecordBuilder(Schema.JSON(Counter.class))
        .value(new Counter(input, counter))
        .build();
  }

}

record Counter(String name, Long value) {

}