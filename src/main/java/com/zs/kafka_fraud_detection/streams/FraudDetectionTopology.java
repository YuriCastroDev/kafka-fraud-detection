package com.zs.kafka_fraud_detection.streams;

import com.zs.kafka_fraud_detection.domain.FraudAlert;
import com.zs.kafka_fraud_detection.domain.Transaction;
import com.zs.kafka_fraud_detection.fraud.FraudAnalyzer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FraudDetectionTopology {

    private static final String TRANSACTIONS_TOPIC       = "transactions";
    private static final String FRAUD_ALERTS_TOPIC       = "fraud-alerts";
    private static final String CLEAN_TRANSACTIONS_TOPIC = "clean-transactions";

    private final FraudAnalyzer fraudAnalyzer;
    private final Serde<Transaction> transactionSerde;
    private final Serde<FraudAlert> fraudAlertSerde;

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        KStream<String, Transaction> transactions = builder.stream(
                TRANSACTIONS_TOPIC,
                Consumed.with(Serdes.String(), transactionSerde)
        );

        transactions
                .filter((key, tx) -> tx != null)
                .split()
                .branch(
                        (key, tx) -> fraudAnalyzer.analyze(tx).isPresent(),
                        Branched.withConsumer(suspicious ->
                                suspicious
                                        .mapValues(tx -> fraudAnalyzer.analyze(tx).orElseThrow())
                                        .to(FRAUD_ALERTS_TOPIC, Produced.with(Serdes.String(), fraudAlertSerde))
                        )
                )
                .branch(
                        (key, tx) -> fraudAnalyzer.analyze(tx).isEmpty(),
                        Branched.withConsumer(clean ->
                                clean.to(CLEAN_TRANSACTIONS_TOPIC, Produced.with(Serdes.String(), transactionSerde))
                        )
                );

        log.info("Fraud detection topology built: transactions → [fraud-alerts | clean-transactions]");
    }
}