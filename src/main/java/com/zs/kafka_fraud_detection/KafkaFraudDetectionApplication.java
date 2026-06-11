package com.zs.kafka_fraud_detection;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class KafkaFraudDetectionApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaFraudDetectionApplication.class, args);
	}

}
