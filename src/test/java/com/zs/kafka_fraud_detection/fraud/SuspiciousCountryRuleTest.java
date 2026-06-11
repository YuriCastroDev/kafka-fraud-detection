package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class SuspiciousCountryRuleTest {

    private SuspiciousCountryRule rule;

    @BeforeEach
    void setUp() {
        rule = new SuspiciousCountryRule();
    }

    @ParameterizedTest
    @ValueSource(strings = {"KP", "IR", "SY", "CU"})
    void shouldReturnSuspiciousForBlockedCountries(String country) {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(500), country, "Test");
        FraudRuleResult result = rule.evaluate(tx);

        assertThat(result.suspicious()).isTrue();
        assertThat(result.riskScore()).isEqualTo(30);
        assertThat(result.reason()).contains(country);
    }

    @ParameterizedTest
    @ValueSource(strings = {"BR", "US", "DE", "FR", "JP"})
    void shouldReturnCleanForNormalCountries(String country) {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(500), country, "Test");
        FraudRuleResult result = rule.evaluate(tx);

        assertThat(result.suspicious()).isFalse();
        assertThat(result.riskScore()).isEqualTo(0);
    }
}
