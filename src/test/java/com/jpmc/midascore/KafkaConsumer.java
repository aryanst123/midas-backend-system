package com.jpmc.midascore;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jpmc.midascore.foundation.Transaction;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class KafkaConsumer {

    private final ObjectMapper objectMapper;
    private final List<Transaction> receivedTransactions = new ArrayList<>();

    public KafkaConsumer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    public void listen(String message) {
        try {
            Transaction tx = objectMapper.readValue(message, Transaction.class);
            receivedTransactions.add(tx);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // This is just for debugging/tests
    public List<Transaction> getReceivedTransactions() {
        return receivedTransactions;
    }
}
