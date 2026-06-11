package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.FraudAlert;
import com.zs.kafka_fraud_detection.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class FraudAnalyzer {

    private final List<FraudRule> rules;

    public Optional<FraudAlert> analyze(Transaction transaction) {
        List<FraudRuleResult> triggered = rules.stream()
                .map(rule -> rule.evaluate(transaction))
                .filter(FraudRuleResult::suspicious)
                .toList();

        if (triggered.isEmpty()) {
            return Optional.empty();
        }

        int totalRiskScore = triggered.stream()
                .mapToInt(FraudRuleResult::riskScore)
                .sum();

        // Cap em 100
        int finalScore = Math.min(totalRiskScore, 100);

        String reasons = triggered.stream()
                .map(FraudRuleResult::reason)
                .collect(Collectors.joining(" | "));

        log.warn("FRAUD DETECTED — transactionId: {} | userId: {} | riskScore: {} | reasons: {}",
                transaction.id(), transaction.userId(), finalScore, reasons);

        return Optional.of(FraudAlert.of(
                transaction.id(),
                transaction.userId(),
                reasons,
                finalScore
        ));
    }
}
