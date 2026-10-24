package com.example.kafka.producer;

import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class MessageProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;

    public MessageProducer(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${app.kafka.topic:orders}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public CompletableFuture<MessageResponse> publish(MessageRequest request) {
        return kafkaTemplate.send(topic, request.key(), request.value())
                .thenApply(this::toResponse)
                .exceptionallyCompose(error -> CompletableFuture.failedFuture(
                        new KafkaPublishException(
                                "Failed to publish message to Kafka topic '" + topic + "'",
                                error)));
    }

    private MessageResponse toResponse(SendResult<String, String> result) {
        var metadata = result.getRecordMetadata();
        return new MessageResponse(
                metadata.topic(),
                result.getProducerRecord().key(),
                metadata.partition(),
                metadata.offset());
    }
}
