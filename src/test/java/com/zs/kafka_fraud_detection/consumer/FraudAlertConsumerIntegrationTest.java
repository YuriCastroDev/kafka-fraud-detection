package com.zs.kafka_fraud_detection.consumer;

import com.zs.kafka_fraud_detection.domain.FraudAlert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"fraud-alerts", "transactions", "clean-transactions"})
@TestPropertySource(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.streams.bootstrap-servers=${spring.embedded.kafka.brokers}"
})
class FraudAlertConsumerIntegrationTest {

    @Autowired
    private KafkaTemplate<String, FraudAlert> kafkaTemplate;

    @SpyBean
    private FraudAlertConsumer fraudAlertConsumer;

    @Test
    void shouldConsumeFraudAlert() {
        FraudAlert alert = FraudAlert.of("tx-123", "user-001", "High value transaction", 40);

        kafkaTemplate.send("fraud-alerts", "user-001", alert);

        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() ->
                verify(fraudAlertConsumer, atLeastOnce()).consume(any(FraudAlert.class))
        );
    }
}
