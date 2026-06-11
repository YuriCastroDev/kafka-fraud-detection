package com.zs.kafka_fraud_detection.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zs.kafka_fraud_detection.domain.FraudAlert;
import com.zs.kafka_fraud_detection.domain.Transaction;
import com.zs.kafka_fraud_detection.fraud.*;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class FraudDetectionTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Transaction> inputTopic;
    private TestOutputTopic<String, FraudAlert> fraudAlertsTopic;
    private TestOutputTopic<String, Transaction> cleanTransactionsTopic;
    private FraudAnalyzer fraudAnalyzer;
    private Serde<Transaction> transactionSerde;
    private Serde<FraudAlert> fraudAlertSerde;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());

        transactionSerde = buildSerde(mapper, Transaction.class);
        fraudAlertSerde = buildSerde(mapper, FraudAlert.class);

        // Regras reais para o teste de topologia
        HighValueRule highValueRule = new HighValueRule();
        org.springframework.test.util.ReflectionTestUtils.setField(highValueRule, "threshold", 10000);

        SuspiciousCountryRule countryRule = new SuspiciousCountryRule();

        fraudAnalyzer = new FraudAnalyzer(List.of(highValueRule, countryRule));

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream("transactions", Consumed.with(Serdes.String(), transactionSerde))
                .filter((key, tx) -> tx != null)
                .split()
                .branch(
                        (key, tx) -> fraudAnalyzer.analyze(tx).isPresent(),
                        Branched.withConsumer(s -> s
                                .mapValues(tx -> fraudAnalyzer.analyze(tx).orElseThrow())
                                .to("fraud-alerts", Produced.with(Serdes.String(), fraudAlertSerde)))
                )
                .branch(
                        (key, tx) -> fraudAnalyzer.analyze(tx).isEmpty(),
                        Branched.withConsumer(s -> s
                                .to("clean-transactions", Produced.with(Serdes.String(), transactionSerde)))
                );

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-fraud");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic("transactions",
                Serdes.String().serializer(), transactionSerde.serializer());
        fraudAlertsTopic = testDriver.createOutputTopic("fraud-alerts",
                Serdes.String().deserializer(), fraudAlertSerde.deserializer());
        cleanTransactionsTopic = testDriver.createOutputTopic("clean-transactions",
                Serdes.String().deserializer(), transactionSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void shouldRouteHighValueTransactionToFraudAlerts() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(50000), "BR", "High value");
        inputTopic.pipeInput(tx.userId(), tx);

        assertThat(fraudAlertsTopic.isEmpty()).isFalse();
        assertThat(cleanTransactionsTopic.isEmpty()).isTrue();

        FraudAlert alert = fraudAlertsTopic.readValue();
        assertThat(alert.userId()).isEqualTo("user-001");
        assertThat(alert.riskScore()).isGreaterThan(0);
    }

    @Test
    void shouldRouteNormalTransactionToCleanTransactions() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(200), "BR", "Normal");
        inputTopic.pipeInput(tx.userId(), tx);

        assertThat(cleanTransactionsTopic.isEmpty()).isFalse();
        assertThat(fraudAlertsTopic.isEmpty()).isTrue();
    }

    @Test
    void shouldRouteSuspiciousCountryToFraudAlerts() {
        Transaction tx = Transaction.of("user-001", BigDecimal.valueOf(500), "KP", "Suspicious country");
        inputTopic.pipeInput(tx.userId(), tx);

        assertThat(fraudAlertsTopic.isEmpty()).isFalse();
        FraudAlert alert = fraudAlertsTopic.readValue();
        assertThat(alert.reason()).contains("KP");
    }

    @Test
    void shouldHandleMultipleTransactions() {
        inputTopic.pipeInput("user-001", Transaction.of("user-001", BigDecimal.valueOf(100), "BR", "Normal 1"));
        inputTopic.pipeInput("user-002", Transaction.of("user-002", BigDecimal.valueOf(50000), "BR", "High value"));
        inputTopic.pipeInput("user-003", Transaction.of("user-003", BigDecimal.valueOf(200), "BR", "Normal 2"));
        inputTopic.pipeInput("user-004", Transaction.of("user-004", BigDecimal.valueOf(300), "IR", "Suspicious"));

        assertThat(cleanTransactionsTopic.readValuesToList()).hasSize(2);
        assertThat(fraudAlertsTopic.readValuesToList()).hasSize(2);
    }

    private <T> Serde<T> buildSerde(ObjectMapper mapper, Class<T> clazz) {
        Serializer<T> serializer = (topic, data) -> {
            try { return mapper.writeValueAsBytes(data); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
        Deserializer<T> deserializer = (topic, data) -> {
            try { return mapper.readValue(data, clazz); }
            catch (Exception e) { throw new RuntimeException(e); }
        };
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
