package com.zs.kafka_fraud_detection.simulator;

import com.zs.kafka_fraud_detection.domain.Transaction;
import com.zs.kafka_fraud_detection.producer.TransactionProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

@Slf4j
@Component
@RequiredArgsConstructor
public class TransactionSimulator {

    private final TransactionProducer producer;
    private final Random random = new Random();

    private static final List<String> USERS = List.of(
            "user-001", "user-002", "user-003", "user-004", "user-005"
    );

    private static final List<String> NORMAL_COUNTRIES = List.of("BR", "BR", "BR", "BR", "US");
    private static final List<String> SUSPICIOUS_COUNTRIES = List.of("KP", "IR", "SY", "CU");

    // Transação normal a cada 800ms
    @Scheduled(fixedDelay = 800)
    public void produceNormalTransaction() {
        String userId = USERS.get(random.nextInt(USERS.size()));
        BigDecimal amount = BigDecimal.valueOf(50 + random.nextInt(950)); // R$ 50 - R$ 1000
        String country = NORMAL_COUNTRIES.get(random.nextInt(NORMAL_COUNTRIES.size()));

        producer.publish(Transaction.of(userId, amount, country, "Normal purchase"));
    }

    // Transação de alto valor a cada 15 segundos
    @Scheduled(fixedDelay = 15000)
    public void produceHighValueTransaction() {
        String userId = USERS.get(random.nextInt(USERS.size()));
        BigDecimal amount = BigDecimal.valueOf(10000 + random.nextInt(90000)); // R$ 10k - R$ 100k
        String country = "BR";

        log.warn("Simulating HIGH VALUE transaction — userId: {} | amount: {}", userId, amount);
        producer.publish(Transaction.of(userId, amount, country, "High value transaction"));
    }

    // Burst de transações (mesmo usuário, várias em sequência) a cada 30 segundos
    @Scheduled(fixedDelay = 30000)
    public void produceBurstTransactions() {
        String userId = USERS.get(random.nextInt(USERS.size()));
        int burstCount = 6 + random.nextInt(4); // 6-9 transações em burst

        log.warn("Simulating BURST of {} transactions for userId: {}", burstCount, userId);
        for (int i = 0; i < burstCount; i++) {
            BigDecimal amount = BigDecimal.valueOf(100 + random.nextInt(500));
            producer.publish(Transaction.of(userId, amount, "BR", "Burst transaction " + (i + 1)));
        }
    }

    // Transação de país suspeito a cada 20 segundos
    @Scheduled(fixedDelay = 20000)
    public void produceSuspiciousCountryTransaction() {
        String userId = USERS.get(random.nextInt(USERS.size()));
        BigDecimal amount = BigDecimal.valueOf(500 + random.nextInt(5000));
        String country = SUSPICIOUS_COUNTRIES.get(random.nextInt(SUSPICIOUS_COUNTRIES.size()));

        log.warn("Simulating SUSPICIOUS COUNTRY transaction — userId: {} | country: {}", userId, country);
        producer.publish(Transaction.of(userId, amount, country, "Suspicious country transaction"));
    }
}
