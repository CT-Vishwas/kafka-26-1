package com.example.kafka.producer;

public record MessageResponse(String topic, String key, int partition, long offset) {
}
