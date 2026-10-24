package com.example.kafka.producer;

public class KafkaPublishException extends RuntimeException {

    public KafkaPublishException(String message, Throwable cause) {
        super(message, cause);
    }
}
