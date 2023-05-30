package my.study.base;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.interceptor.ProducerInterceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomProducerInterceptor implements ProducerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(CustomProducerInterceptor.class);

  @Override
  public void close() {
    log.info("Closing interceptor:{}", this.getClass().getSimpleName());
  }

  @Override
  public boolean eligible(Message message) {
    log.info("Check eligibility for messageKey:{}", message.getKey());
    return true;
  }

  @Override
  public Message beforeSend(Producer producer, Message message) {
    log.info("Call before send for messageKey:{}", message.getKey());
    return message;
  }

  @Override
  public void onSendAcknowledgement(Producer producer, Message message, MessageId msgId, Throwable exception) {
    if (exception != null) {
      log.warn("Message sending failed messageKey:{}", message.getKey(), exception);
    } else {
      log.info("Message successfully sent messageKey:{}. Returned MessageId:{}", message.getKey(), msgId);
    }
  }

}
