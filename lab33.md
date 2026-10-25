# Lab: Simple Kafka Streams

## Step 1: Create the Kafka topics
We'll use these topics:
- order-amounts — input topic.
- customer-total — output topic.

## Step 2: Write the Kafka Streams application
Create `CustomerOrderTotals.java`
```java

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
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
            Serdes.Long().getClass()
        );

        StreamsBuilder builder = new StreamsBuilder();

        // Read customer ID as key and order amount as value
        KStream<String, Long> orders =
            builder.stream(
                "order-amounts",
                Consumed.with(Serdes.String(), Serdes.Long())
            );

        // Group all order amounts by customer
        KTable<String, Long> totals =
            orders
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
```
