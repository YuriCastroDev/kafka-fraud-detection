package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.Transaction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;

class HighValueRuleTest {

    private HighValueRule rule;

    @BeforeEach
    void setUp() {
        rule = new HighValueRule();
        ReflectionTestUtils.setField(rule, "threshold", 10000);
    }

    @Test
    void shouldReturnCleanForNormalAmount() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(500), "BR", "Normal purchase");
        FraudRuleResult result = rule.evaluate(tx);

        assertThat(result.suspicious()).isFalse();
        assertThat(result.riskScore()).isEqualTo(0);
    }

    @Test
    void shouldReturnSuspiciousForHighAmount() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(15000), "BR", "High value");
        FraudRuleResult result = rule.evaluate(tx);

        assertThat(result.suspicious()).isTrue();
        assertThat(result.riskScore()).isEqualTo(40);
        assertThat(result.reason()).contains("15000");
    }

    @Test
    void shouldReturnCleanForExactThreshold() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(10000), "BR", "Exact threshold");
        FraudRuleResult result = rule.evaluate(tx);

        assertThat(result.suspicious()).isFalse();
    }

    @Test
    void shouldReturnSuspiciousForOneAboveThreshold() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(10001), "BR", "Just above");
        FraudRuleResult result = rule.evaluate(tx);

        assertThat(result.suspicious()).isTrue();
    }
}
