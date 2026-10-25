package com.example.kafka.retry;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class CustomerOrderTotals {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(
            StreamsConfig.APPLICATION_ID_CONFIG,
            "customer-order-totals"
        );

        props.put(
            StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            "localhost:9092"
        );

        props.put(
            StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        props.put(
            StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
            Serdes.String().getClass()
        );

        StreamsBuilder builder = new StreamsBuilder();

        // Read customer ID as key and order amount as value.
        // The producer sends numeric strings, so convert them to Long before aggregating.
        KStream<String, String> orders =
            builder.stream(
                "order-amounts",
                Consumed.with(Serdes.String(), Serdes.String())
            );

        KTable<String, Long> totals =
            orders
                .mapValues(value -> {
                    try {
                        return Long.parseLong(value);
                    } catch (NumberFormatException e) {
                        return 0L;
                    }
                })
                .groupByKey(
                    Grouped.with(
                        Serdes.String(),
                        Serdes.Long()
                    )
                )
                .reduce(Long::sum);

        // Publish updated customer totals
        totals.toStream().to(
            "customer-total",
            Produced.with(
                Serdes.String(),
                Serdes.Long()
            )
        );

        KafkaStreams streams =
            new KafkaStreams(builder.build(), props);

        streams.start();

        Runtime.getRuntime().addShutdownHook(
            new Thread(streams::close)
        );
    }
}