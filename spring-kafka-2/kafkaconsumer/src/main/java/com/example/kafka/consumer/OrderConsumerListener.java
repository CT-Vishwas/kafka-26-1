package com.example.kafka.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumerListener {

		// Listener A: Analytics group
		@KafkaListener(
						topics = "spring-orders",
						groupId = "analytics-group",
						id = "analytics-listener")
		public void analyticsListener(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
						@Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[ANALYTICS] key=%s value=%s partition=%d offset=%d%n",
								key, value, partition, offset);
		}

		// Listener B: Billing group
		@KafkaListener(
						topics = "spring-orders",
						groupId = "billing-group",
						id = "billing-listener")
		public void billingListener(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
						@Header(KafkaHeaders.OFFSET) long offset) {
				System.out.printf("[BILLING] key=%s value=%s partition=%d offset=%d%n",
								key, value, partition, offset);
		}
}