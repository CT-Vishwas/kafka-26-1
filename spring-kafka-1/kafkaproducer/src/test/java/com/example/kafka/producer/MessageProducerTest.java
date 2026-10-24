package com.example.kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

class MessageProducerTest {

    @Test
    void publishesKeyedMessageAndReturnsKafkaMetadata() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        var metadata = new RecordMetadata(new TopicPartition("orders", 2), 10, 4, 0L, 0, 5);
        @SuppressWarnings("unchecked")
        SendResult<String, String> sendResult = mock(SendResult.class);
        when(sendResult.getRecordMetadata()).thenReturn(metadata);
        when(sendResult.getProducerRecord()).thenReturn(
                new org.apache.kafka.clients.producer.ProducerRecord<>("orders", "order-42", "created"));
        when(kafkaTemplate.send("orders", "order-42", "created"))
                .thenReturn(CompletableFuture.completedFuture(sendResult));

        var response = new MessageProducer(kafkaTemplate, "orders")
                .publish(new MessageRequest("order-42", "created"))
                .join();

        assertEquals(new MessageResponse("orders", "order-42", 2, 14), response);
        verify(kafkaTemplate).send("orders", "order-42", "created");
    }

    @Test
    void reportsKafkaSendFailure() {
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        when(kafkaTemplate.send("orders", null, "created"))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));

        var exception = assertThrows(
                java.util.concurrent.CompletionException.class,
                () -> new MessageProducer(kafkaTemplate, "orders")
                        .publish(new MessageRequest(null, "created"))
                        .join());

        assertEquals(KafkaPublishException.class, exception.getCause().getClass());
        assertEquals(
                "Failed to publish message to Kafka topic 'orders'",
                exception.getCause().getMessage());
    }
}
