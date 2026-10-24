package com.example.kafka.consumer;


import java.util.Collection;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;


@Component
public class OrderConsumerListener implements ConsumerAwareRebalanceListener {
   @Override
    public void onPartitionsRevokedBeforeCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {

        System.out.println(
            "Partitions about to be revoked: " + partitions);
    }

    @Override
    public void onPartitionsRevokedAfterCommit(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {

        System.out.println(
            "Partitions revoked after commit: " + partitions);
    }

    @Override
    public void onPartitionsAssigned(
            Consumer<?, ?> consumer,
            Collection<TopicPartition> partitions) {

        System.out.println(
            "Partitions assigned: " + partitions);
    }

    @Override
    public void onPartitionsLost(
            Consumer<?, ?> consumer,

            Collection<TopicPartition> partitions) {

        System.out.println(
            "Partitions lost: " + partitions);
    }

}