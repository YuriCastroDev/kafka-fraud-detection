package com.zs.kafka_fraud_detection.consumer;

import com.zs.kafka_fraud_detection.domain.FraudAlert;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class FraudAlertConsumer {

    @KafkaListener(topics = "fraud-alerts", groupId = "fraud-alert-group")
    public void consume(FraudAlert alert) {
        log.error("🚨 FRAUD ALERT — transactionId: {} | userId: {} | riskScore: {} | reasons: {} | detectedAt: {}",
                alert.transactionId(),
                alert.userId(),
                alert.riskScore(),
                alert.reason(),
                alert.detectedAt()
        );

        //     securityNotifier.notify(alert);
    }
}
