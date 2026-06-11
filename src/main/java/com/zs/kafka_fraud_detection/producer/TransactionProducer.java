package com.zs.kafka_fraud_detection.producer;

import com.zs.kafka_fraud_detection.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionProducer {

    private static final String TOPIC = "transactions";

    private final KafkaTemplate<String, Transaction> kafkaTemplate;

    public void publish(Transaction transaction) {
        kafkaTemplate.send(TOPIC, transaction.userId(), transaction);
        log.info("Transaction published — id: {} | userId: {} | amount: {} | country: {}",
                transaction.id(),
                transaction.userId(),
                transaction.amount(),
                transaction.country()
        );
    }
}
