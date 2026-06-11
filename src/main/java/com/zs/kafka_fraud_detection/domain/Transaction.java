package com.zs.kafka_fraud_detection.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

public record Transaction(
        String id,
        String userId,
        BigDecimal amount,
        String country,
        String description,
        LocalDateTime timestamp
) {
    public static Transaction of(String userId, BigDecimal amount, String country, String description) {
        return new Transaction(
                UUID.randomUUID().toString(),
                userId,
                amount,
                country,
                description,
                LocalDateTime.now()
        );
    }
}
