package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class OrderConsumerListener implements ConsumerSeekAware {

    @Override
    public void onPartitionsAssigned(
            Map<TopicPartition, Long> assignments,
            ConsumerSeekCallback callback) {

        TopicPartition target =
                new TopicPartition("spring-orders", 0);

        if (assignments.containsKey(target)) {
            callback.seek("spring-orders", 0, 2L);
            System.out.println("Seeking to offset 2");
        }
    }

    @KafkaListener(topics = "spring-orders")
    public void consume(ConsumerRecord<String, String> record) {

        System.out.println(
            "Partition: " + record.partition()
            + ", Offset: " + record.offset()
            + ", Key: " + record.key()
            + ", Value: " + record.value()
        );
    }
}