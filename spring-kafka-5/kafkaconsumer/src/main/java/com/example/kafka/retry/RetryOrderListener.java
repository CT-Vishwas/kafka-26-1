package com.example.kafka.retry;

import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

@Component
public class RetryOrderListener {

		@RetryableTopic(
						attempts = "4",                // 1 initial + 3 retries
						backoff = @org.springframework.retry.annotation.Backoff(delay = 2000, multiplier = 2.0),
						topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
						autoCreateTopics = "true",
						dltTopicSuffix = "-dlt")
		@KafkaListener(topics = "retry-orders", groupId = "retry-orders-group")
		public void consume(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						@Header(KafkaHeaders.OFFSET) long offset,
						@Header(KafkaHeaders.GROUP_ID) String groupId
						) {

				System.out.printf("[CONSUME] topic=%s groupId=%s key=%s offset=%d value=%s%n",
								topic, key, offset, value);

				// Simulate transient/permanent failures using payload markers
				if (value.contains("FAIL")) {
						throw new IllegalStateException("Simulated processing failure for value=" + value);
				}

				System.out.printf("[SUCCESS] key=%s processed%n", key);
		}

		@RetryableTopic(
						attempts = "4",                // 1 initial + 3 retries
						backoff = @org.springframework.retry.annotation.Backoff(delay = 2000, multiplier = 2.0),
						topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
						autoCreateTopics = "true",
						dltTopicSuffix = "-dlt")
		@KafkaListener(topics = "retry-orders", groupId = "retry-billing-group")
		public void consumebill(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						@Header(KafkaHeaders.OFFSET) long offset,
						@Header(KafkaHeaders.GROUP_ID) String groupId) {

				System.out.printf("[CONSUME] topic=%s groupId=%s key=%s offset=%d value=%s%n",
								topic, key, offset, value);

				// Simulate transient/permanent failures using payload markers
				if (value.contains("FAIL")) {
						throw new IllegalStateException("Simulated processing failure for value=" + value);
				}

				System.out.printf("[SUCCESS] key=%s processed%n", key);
		}

		@DltHandler
		public void handleDlt(
						String value,
						@Header(KafkaHeaders.RECEIVED_KEY) String key,
						@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
						@Header(KafkaHeaders.OFFSET) long offset,
						@Header(KafkaHeaders.GROUP_ID) String groupId) {
				System.out.printf("[DLT] topic=%s groupId=%s key=%s offset=%d value=%s%n",
								topic, groupId, key, offset, value);
		}
}