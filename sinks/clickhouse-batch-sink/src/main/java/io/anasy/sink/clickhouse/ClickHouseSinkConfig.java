package io.anasy.sink.clickhouse;

import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.DeserializationException;

@Configuration
@EnableConfigurationProperties(EventSinkProperties.class)
public class ClickHouseSinkConfig {

    @Bean
    public DeadLetterPublishingRecoverer dlqRecoverer(KafkaTemplate<String, String> template,
                                                     EventSinkProperties props) {
        return new DeadLetterPublishingRecoverer(template, (rec, ex) ->
                new TopicPartition(rec.topic() + props.getDlq().getSuffix(), rec.partition()));
    }

    @Bean
    public DefaultErrorHandler sinkErrorHandler(DeadLetterPublishingRecoverer recoverer,
                                               EventSinkProperties props) {
        var retry = props.getRetry();
        var backOff = new ExponentialBackOffWithMaxRetries(retry.getMaxAttempts() - 1);
        backOff.setInitialInterval(retry.getInitialInterval().toMillis());
        backOff.setMultiplier(retry.getMultiplier());
        backOff.setMaxInterval(retry.getMaxInterval().toMillis());

        var handler = new DefaultErrorHandler(recoverer, backOff);
        handler.setCommitRecovered(true);
        handler.addNotRetryableExceptions(
                PoisonEventException.class,
                DeserializationException.class,
                IllegalArgumentException.class);
        return handler;
    }
}
