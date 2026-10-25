package com.example.kafka.retry;

public record OrderEvent(String orderId, long amount) {}