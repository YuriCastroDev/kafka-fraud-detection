# 🔍 Kafka Fraud Detection

Real-time fraud detection system built with **Java 21**, **Spring Boot 3** and **Kafka Streams**.  
Simulates financial transactions and analyzes them using configurable rules (high value, frequency, suspicious country).
Suspicious transactions are routed to a `fraud-alerts` topic; clean ones continue to `clean-transactions`.

---

## 🏗️ Architecture

```
[TransactionSimulator — @Scheduled]
         │
         │ publishes: Transaction (normal / high-value / burst / suspicious-country)
         ▼
[Topic: transactions]
         │
         ▼
[FraudDetectionTopology — Kafka Streams]
         │
         ├──► [Topic: fraud-alerts]       ──► [FraudAlertConsumer] → log + extensible
         └──► [Topic: clean-transactions] ──► Normal processing
```

---

## 🛠️ Tech Stack

| Technology | Version | Purpose |
|---|---|---|
| Java | 21 | Language |
| Spring Boot | 3.5 | Framework |
| Kafka Streams | 3.9 | Stream processing |
| Spring Kafka | 3.3 | Kafka integration |
| Redis | 7.2 | Transaction frequency tracking |
| PostgreSQL | 16 | Persistence (extensible) |
| Docker Compose | - | Local infrastructure |
| Kafdrop | latest | Kafka UI |
| JUnit 5 + Mockito | - | Unit tests |
| TopologyTestDriver | - | Streams tests without broker |
| EmbeddedKafka | - | Integration tests |
| Awaitility | - | Async test assertions |

---

## ▶️ Running Locally

### Prerequisites

- Docker Desktop
- Java 21 (Temurin recommended)
- Maven

### Steps

**1. Start infrastructure**
```bash
docker-compose up -d
```

This starts Kafka, Zookeeper, Kafdrop, Redis and PostgreSQL.

**2. Run the application**
```bash
./mvnw spring-boot:run
```

**3. Watch fraud detection in action**

Open Kafdrop at **http://localhost:9000** and watch:
- `transactions` — all transactions published by the simulator
- `fraud-alerts` — suspicious transactions caught by the rules
- `clean-transactions` — transactions that passed all rules

**4. Run tests**
```bash
./mvnw test
```

---

## 🔎 Fraud Detection Rules

| Rule | Condition | Risk Score |
|---|---|---|
| `HighValueRule` | Amount > R$ 10,000 | +40 |
| `FrequencyRule` | More than 5 transactions in 60s (Redis) | +50 |
| `SuspiciousCountryRule` | Country in `KP, IR, SY, CU` | +30 |

Risk scores are summed across triggered rules and capped at 100.

---

## 🔄 Simulation Patterns

| Scheduler | Interval | Pattern |
|---|---|---|
| `produceNormalTransaction` | 800ms | Normal purchases R$50–1000 |
| `produceHighValueTransaction` | 15s | R$10k–100k transactions |
| `produceBurstTransactions` | 30s | 6–9 transactions from same user |
| `produceSuspiciousCountryTransaction` | 20s | Transactions from KP, IR, SY, CU |

---

## ⚙️ Configuration

```yaml
app:
  fraud:
    high-value-threshold: 10000     # amount that triggers HighValueRule
    frequency-limit: 5              # max transactions per window
    frequency-window-seconds: 60    # Redis TTL for frequency counter
```

---

## 💡 Key Concepts Demonstrated

| Concept | Where |
|---|---|
| Kafka Streams split/branch | `FraudDetectionTopology` — routes by fraud score |
| Rule engine pattern | `FraudRule` interface + `FraudAnalyzer` |
| Stateful processing with Redis | `FrequencyRule` — counts transactions per user per window |
| Risk score aggregation | `FraudAnalyzer` — sums scores, caps at 100 |
| Custom Serde | `KafkaStreamsConfig` — JSON serialization for records |
| TopologyTestDriver | Unit tests without a Kafka broker |
| EmbeddedKafka | Integration test for `FraudAlertConsumer` |

---

## 🗂️ Project Structure

```
src/
├── main/java/com/zs/kafka_fraud_detection/
│   ├── config/
│   │   └── KafkaStreamsConfig.java         # @EnableKafkaStreams + custom Serdes
│   ├── consumer/
│   │   └── FraudAlertConsumer.java         # Consumes fraud-alerts topic
│   ├── domain/
│   │   ├── Transaction.java                # record: id, userId, amount, country, timestamp
│   │   └── FraudAlert.java                 # record: transactionId, userId, reason, riskScore
│   ├── fraud/
│   │   ├── FraudRule.java                  # interface
│   │   ├── FraudRuleResult.java            # record: suspicious, reason, riskScore
│   │   ├── HighValueRule.java              # amount > threshold
│   │   ├── FrequencyRule.java              # Redis-based frequency check
│   │   ├── SuspiciousCountryRule.java      # blocked countries list
│   │   └── FraudAnalyzer.java             # applies all rules, sums scores
│   ├── producer/
│   │   └── TransactionProducer.java
│   ├── simulator/
│   │   └── TransactionSimulator.java       # 4 @Scheduled patterns
│   └── streams/
│       └── FraudDetectionTopology.java     # Kafka Streams topology
└── test/java/com/zs/kafka_fraud_detection/
    ├── fraud/
    │   ├── HighValueRuleTest.java
    │   ├── SuspiciousCountryRuleTest.java
    │   └── FraudAnalyzerTest.java
    ├── streams/
    │   └── FraudDetectionTopologyTest.java  # TopologyTestDriver
    └── consumer/
        └── FraudAlertConsumerIntegrationTest # EmbeddedKafka
```

---

## 🧪 Tests

| Test | Type | What it covers |
|---|---|---|
| `HighValueRuleTest` | Unit | threshold boundary, exact value, above/below |
| `SuspiciousCountryRuleTest` | Unit | all blocked countries, normal countries (parameterized) |
| `FraudAnalyzerTest` | Unit | no rules triggered, single rule, score sum, cap at 100 |
| `FraudDetectionTopologyTest` | Unit (TopologyTestDriver) | high value → fraud-alerts, normal → clean, multi-transaction routing |
| `FraudAlertConsumerIntegrationTest` | Integration (EmbeddedKafka) | consumer receives and processes alert |
