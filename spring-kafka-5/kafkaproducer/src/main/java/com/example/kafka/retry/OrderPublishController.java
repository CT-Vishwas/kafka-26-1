package com.example.kafka.retry;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderPublishController {

		private final KafkaTemplate<String, String> kafkaTemplate;

		public OrderPublishController(KafkaTemplate<String, String> kafkaTemplate) {
				this.kafkaTemplate = kafkaTemplate;
		}

		@PostMapping("/orders/{key}")
		public String publish(@PathVariable String key, @RequestBody String payload) {
				kafkaTemplate.send("retry-orders", key, payload);
				return "Published key=" + key;
		}
}