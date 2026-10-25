package com.example.kafka.retry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class OrderPublishController {

		private final KafkaTemplate<String, String> kafkaTemplate;
		private final ObjectMapper objectMapper;

		public OrderPublishController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
				this.kafkaTemplate = kafkaTemplate;
				this.objectMapper = objectMapper;
		}

		@PostMapping("/orders/{key}")
		public String publish(@PathVariable String key, @RequestBody String payload) {
			    // Extract amount from payload (assuming payload is a JSON string)
				try {
						JsonNode jsonNode = objectMapper.readTree(payload);
						String amount = jsonNode.get("amount").asText();
						kafkaTemplate.send("order-amounts", key, amount);
				} catch (Exception e) {
						throw new RuntimeException("Error processing payload", e);
				}
				return "Published key=" + key;
		}
}