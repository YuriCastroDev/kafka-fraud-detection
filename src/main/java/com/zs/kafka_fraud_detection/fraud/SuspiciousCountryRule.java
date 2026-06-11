package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.Transaction;
import org.springframework.stereotype.Component;

import java.util.Set;

@Component
public class SuspiciousCountryRule implements FraudRule {

    private static final Set<String> SUSPICIOUS_COUNTRIES = Set.of("KP", "IR", "SY", "CU");

    @Override
    public FraudRuleResult evaluate(Transaction transaction) {
        if (SUSPICIOUS_COUNTRIES.contains(transaction.country())) {
            return FraudRuleResult.suspicious(
                    "Transaction originated from suspicious country: " + transaction.country(),
                    30
            );
        }
        return FraudRuleResult.clean();
    }
}
