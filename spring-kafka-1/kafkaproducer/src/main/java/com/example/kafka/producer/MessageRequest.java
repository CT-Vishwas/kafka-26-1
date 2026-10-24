package com.example.kafka.producer;

import jakarta.validation.constraints.NotBlank;

public record MessageRequest(String key, @NotBlank String value) {
}
