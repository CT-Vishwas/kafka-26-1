package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumerListener {

		@KafkaListener(topics = "spring-orders", groupId = "spring-orders-group")
		public void consume(String value) {
				System.out.println("Consumed => " + value);
		}
}