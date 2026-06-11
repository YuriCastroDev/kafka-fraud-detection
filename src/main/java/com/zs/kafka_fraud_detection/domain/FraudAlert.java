package com.zs.kafka_fraud_detection.domain;

import java.time.LocalDateTime;

public record FraudAlert(
        String transactionId,
        String userId,
        String reason,
        int riskScore,
        LocalDateTime detectedAt
) {
    public static FraudAlert of(String transactionId, String userId, String reason, int riskScore) {
        return new FraudAlert(
                transactionId,
                userId,
                reason,
                riskScore,
                LocalDateTime.now()
        );
    }
}
