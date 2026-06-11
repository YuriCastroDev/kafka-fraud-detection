package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.Transaction;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Component
public class HighValueRule implements FraudRule {

    @Value("${app.fraud.high-value-threshold:10000}")
    private int threshold;

    @Override
    public FraudRuleResult evaluate(Transaction transaction) {
        if (transaction.amount().compareTo(BigDecimal.valueOf(threshold)) > 0) {
            return FraudRuleResult.suspicious(
                    "Transaction amount " + transaction.amount() + " exceeds threshold of " + threshold,
                    40
            );
        }
        return FraudRuleResult.clean();
    }
}
