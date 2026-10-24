package com.example.kafka.producer;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderProducerController {

		private final KafkaTemplate<String, String> kafkaTemplate;
		private static final String TOPIC = "spring-orders";

		public OrderProducerController(KafkaTemplate<String, String> kafkaTemplate) {
				this.kafkaTemplate = kafkaTemplate;
		}

		@PostMapping("/orders/{key}")
		public String send(@PathVariable String key, @RequestBody String payload) {
				kafkaTemplate.send(TOPIC, key, payload);
				return "Published key=" + key;
		}
}