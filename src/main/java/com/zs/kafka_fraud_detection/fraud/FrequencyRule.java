package com.zs.kafka_fraud_detection.fraud;

import com.zs.kafka_fraud_detection.domain.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class FrequencyRule implements FraudRule {

    private final StringRedisTemplate redisTemplate;

    @Value("${app.fraud.frequency-limit:5}")
    private int frequencyLimit;

    @Value("${app.fraud.frequency-window-seconds:60}")
    private int windowSeconds;

    @Override
    public FraudRuleResult evaluate(Transaction transaction) {
        String key = "tx:freq:" + transaction.userId();

        Long count = redisTemplate.opsForValue().increment(key);

        if (count == 1) {
            // Primeira transação na janela — define o TTL
            redisTemplate.expire(key, Duration.ofSeconds(windowSeconds));
        }

        log.debug("User {} transaction count in window: {}", transaction.userId(), count);

        if (count > frequencyLimit) {
            return FraudRuleResult.suspicious(
                    "User " + transaction.userId() + " made " + count + " transactions in " + windowSeconds + "s window",
                    50
            );
        }

        return FraudRuleResult.clean();
    }
}
