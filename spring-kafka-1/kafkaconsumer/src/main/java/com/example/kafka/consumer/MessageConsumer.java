package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageConsumer.class);

    @KafkaListener(topics = "${app.kafka.topic:orders}")
    public void consume(ConsumerRecord<String, String> record) {
        LOGGER.info(
                "Received message topic={} partition={} offset={} key={} value={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                record.value());
    }
}
