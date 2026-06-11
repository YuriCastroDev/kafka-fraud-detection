package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.Transaction;

public interface FraudRule {
    FraudRuleResult evaluate(Transaction transaction);
}