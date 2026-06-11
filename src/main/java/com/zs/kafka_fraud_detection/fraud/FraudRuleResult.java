package com.zs.kafka_fraud_detection.fraud;

public record FraudRuleResult(
        boolean suspicious,
        String reason,
        int riskScore
) {
    public static FraudRuleResult clean() {
        return new FraudRuleResult(false, null, 0);
    }

    public static FraudRuleResult suspicious(String reason, int riskScore) {
        return new FraudRuleResult(true, reason, riskScore);
    }
}
