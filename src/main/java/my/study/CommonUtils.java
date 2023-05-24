package my.study;

import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtils {

  private static final Logger log = LoggerFactory.getLogger(CommonUtils.class);

  private CommonUtils() {
  }

  public static void logMessage(Message<?> message) {
    log.info(
        "Message consumed. MessageId:[{}], sequenceId:[{}], key:[{}], value:[{}], topic:[{}], publishTime:[{}], "
            + "properties:{}",
        message.getMessageId(), message.getSequenceId(), message.getKey(), message.getValue(), message.getTopicName(),
        Instant.ofEpochMilli(message.getPublishTime()), message.getProperties());
  }

  public static void safeSleep(int seconds) {
    try {
      log.info("Going to sleep for {} seconds", seconds);
      TimeUnit.SECONDS.sleep(seconds);
      log.info("Wake up from sleep.");
    } catch (InterruptedException e) {
      log.error("An exception has occurred during sleep.", e);
      Thread.currentThread().interrupt();
    }
  }

}
