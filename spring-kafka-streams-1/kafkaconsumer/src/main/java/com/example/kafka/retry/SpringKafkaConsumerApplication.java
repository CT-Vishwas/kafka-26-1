package com.example.kafka.retry;

import org.apache.kafka.streams.KafkaStreams;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class SpringKafkaConsumerApplication {

    @Bean(initMethod = "start", destroyMethod = "close")
    KafkaStreams customerOrderTotalsStreams(
        @Value("${spring.kafka.bootstrap-servers:localhost:9092}") String bootstrapServers
    ) {
        return CustomerOrderTotals.createStreams(bootstrapServers);
    }

    public static void main(String[] args) {
        SpringApplication.run(SpringKafkaConsumerApplication.class, args);
    }
}
