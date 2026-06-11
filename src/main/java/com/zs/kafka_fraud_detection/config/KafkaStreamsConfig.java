package com.zs.kafka_fraud_detection.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zs.kafka_fraud_detection.domain.FraudAlert;
import com.zs.kafka_fraud_detection.domain.Transaction;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfiguration() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-fraud-detection");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public Serde<Transaction> transactionSerde() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        Serializer<Transaction> serializer = (topic, data) -> {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException("Error serializing Transaction", e); }
        };

        Deserializer<Transaction> deserializer = (topic, data) -> {
            try { return mapper.readValue(data, Transaction.class); }
            catch (Exception e) { throw new RuntimeException("Error deserializing Transaction", e); }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }

    @Bean
    public Serde<FraudAlert> fraudAlertSerde() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        Serializer<FraudAlert> serializer = (topic, data) -> {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException("Error serializing FraudAlert", e); }
        };

        Deserializer<FraudAlert> deserializer = (topic, data) -> {
            try { return mapper.readValue(data, FraudAlert.class); }
            catch (Exception e) { throw new RuntimeException("Error deserializing FraudAlert", e); }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
