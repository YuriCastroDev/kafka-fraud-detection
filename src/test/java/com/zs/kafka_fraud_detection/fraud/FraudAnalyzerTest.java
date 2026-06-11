package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.FraudAlert;
import com.zs.kafka_fraud_detection.domain.Transaction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FraudAnalyzerTest {

    @Mock
    private HighValueRule highValueRule;

    @Mock
    private SuspiciousCountryRule suspiciousCountryRule;

    @Mock
    private FrequencyRule frequencyRule;

    @InjectMocks
    private FraudAnalyzer fraudAnalyzer;

    @Test
    void shouldReturnEmptyWhenNoRulesTriggered() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(500), "BR", "Normal");

        when(highValueRule.evaluate(tx)).thenReturn(FraudRuleResult.clean());
        when(suspiciousCountryRule.evaluate(tx)).thenReturn(FraudRuleResult.clean());
        when(frequencyRule.evaluate(tx)).thenReturn(FraudRuleResult.clean());

        fraudAnalyzer = new FraudAnalyzer(List.of(highValueRule, suspiciousCountryRule, frequencyRule));

        Optional<FraudAlert> result = fraudAnalyzer.analyze(tx);
        assertThat(result).isEmpty();
    }

    @Test
    void shouldReturnAlertWhenHighValueRuleTriggered() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(15000), "BR", "High value");

        when(highValueRule.evaluate(tx)).thenReturn(FraudRuleResult.suspicious("High value", 40));
        when(suspiciousCountryRule.evaluate(tx)).thenReturn(FraudRuleResult.clean());
        when(frequencyRule.evaluate(tx)).thenReturn(FraudRuleResult.clean());

        fraudAnalyzer = new FraudAnalyzer(List.of(highValueRule, suspiciousCountryRule, frequencyRule));

        Optional<FraudAlert> result = fraudAnalyzer.analyze(tx);
        assertThat(result).isPresent();
        assertThat(result.get().riskScore()).isEqualTo(40);
        assertThat(result.get().reason()).contains("High value");
    }

    @Test
    void shouldSumRiskScoresFromMultipleRules() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(15000), "KP", "Suspicious");

        when(highValueRule.evaluate(tx)).thenReturn(FraudRuleResult.suspicious("High value", 40));
        when(suspiciousCountryRule.evaluate(tx)).thenReturn(FraudRuleResult.suspicious("Suspicious country", 30));
        when(frequencyRule.evaluate(tx)).thenReturn(FraudRuleResult.clean());

        fraudAnalyzer = new FraudAnalyzer(List.of(highValueRule, suspiciousCountryRule, frequencyRule));

        Optional<FraudAlert> result = fraudAnalyzer.analyze(tx);
        assertThat(result).isPresent();
        assertThat(result.get().riskScore()).isEqualTo(70);
    }

    @Test
    void shouldCapRiskScoreAt100() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(15000), "KP", "All rules");

        when(highValueRule.evaluate(tx)).thenReturn(FraudRuleResult.suspicious("High value", 40));
        when(suspiciousCountryRule.evaluate(tx)).thenReturn(FraudRuleResult.suspicious("Suspicious country", 30));
        when(frequencyRule.evaluate(tx)).thenReturn(FraudRuleResult.suspicious("High frequency", 50));

        fraudAnalyzer = new FraudAnalyzer(List.of(highValueRule, suspiciousCountryRule, frequencyRule));

        Optional<FraudAlert> result = fraudAnalyzer.analyze(tx);
        assertThat(result).isPresent();
        assertThat(result.get().riskScore()).isEqualTo(100);
    }
}
