# Kafka Event-Driven Demo

Basit bir Spring Boot + Kafka demosu. Amaç, event-driven akışın temelini küçük ve anlaşılır bir örnek üzerinden görmek:

- HTTP isteği alınır
- Kafka'ya event yazılır
- Bir consumer bu event'i işler
- Yeni bir event üretilir
- Başka bir consumer son event'i tüketir

Bu proje öğrenme amaçlıdır. Producer ve consumer'lar aynı uygulama içinde çalışır; yani bu yapı tam bir mikroservis dağılımı değil, event akışını göstermek için sadeleştirilmiş bir demodur.

## Teknolojiler

- Java 21
- Spring Boot 4
- Spring for Apache Kafka
- Docker Compose
- Kafka UI

## Senaryo

Ödeme isteği sisteme gelir, `payment.requested.v1` topic'ine yazılır. Yetkilendirme consumer'ı bu event'i tüketir, basit bir kural ile kararı verir ve `payment.processed.v1` event'i üretir. Son olarak ledger/notification consumer'ı bu event'i okuyup loglar.

## Event Akışı

```text
POST /api/payments
        |
        v
payment.requested.v1
        |
        v
AuthorizationConsumer
        |
        v
payment.processed.v1
        |
        v
LedgerConsumer
```

## Proje Yapısı

- [PaymentController](src/main/java/com/kafka/demo/api/PaymentController.java): HTTP isteğini alır.
- [PaymentEventProducer](src/main/java/com/kafka/demo/producer/PaymentEventProducer.java): Kafka'ya event üretir.
- [AuthorizationConsumer](src/main/java/com/kafka/demo/consumer/AuthorizationConsumer.java): `payment.requested.v1` event'ini tüketir, karar verir.
- [LedgerConsumer](src/main/java/com/kafka/demo/consumer/LedgerConsumer.java): `payment.processed.v1` event'ini tüketir.
- [KafkaTopicConfig](src/main/java/com/kafka/demo/config/KafkaTopicConfig.java): Topic bean'lerini oluşturur.
- [compose.yaml](compose.yaml): Kafka ve Kafka UI altyapısını ayağa kaldırır.

## Topic'ler

- `payment.requested.v1`
- `payment.processed.v1`

## İş Kuralı

- `amount <= 10000` ise sonuç `APPROVED`
- `amount > 10000` ise sonuç `REJECTED`

Bu kural [AuthorizationConsumer](src/main/java/com/kafka/demo/consumer/AuthorizationConsumer.java) içinde uygulanır.

## Gereksinimler

- Java 21
- Docker Desktop

## Projeyi Ayağa Kaldırma

Önce Kafka ve Kafka UI:

```bash
docker compose up -d
```

Sonra Spring Boot uygulaması:

```bash
./mvnw spring-boot:run
```

Uygulama varsayılan olarak `localhost:9092` Kafka broker'ına bağlanır. Gerekirse env var ile değiştirilebilir:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 ./mvnw spring-boot:run
```

## Uygulama Adresleri

- API: [http://localhost:8080](http://localhost:8080)
- Kafka UI: [http://localhost:8081](http://localhost:8081)

## Event Üretme

Örnek başarılı ödeme isteği:

```bash
curl -X POST http://localhost:8080/api/payments \
  -H 'Content-Type: application/json' \
  -d '{
    "paymentId": "pay-1001",
    "customerId": "cust-42",
    "amount": 2500,
    "currency": "TRY"
  }'
```

Beklenen response:

```json
{
  "paymentId": "pay-1001",
  "status": "PAYMENT_REQUESTED",
  "message": "Ödeme olayı Kafka'ya bırakıldı"
}
```

Örnek reddedilen ödeme isteği:

```bash
curl -X POST http://localhost:8080/api/payments \
  -H 'Content-Type: application/json' \
  -d '{
    "paymentId": "pay-1002",
    "customerId": "cust-99",
    "amount": 15000,
    "currency": "TRY"
  }'
```

## Eventleri Nasıl Görürüm?

### 1. Uygulama logları

İstek attıktan sonra loglarda şu satırları görürsün:

```text
Authorization sonucu -> paymentId=..., status=...
Ledger/notification simülasyonu -> paymentId=..., status=..., amount=..., currency=...
```

### 2. Kafka UI

[http://localhost:8081](http://localhost:8081) adresine git ve şu adımları izle:

1. `local` cluster'ını aç
2. `Topics` bölümüne gir
3. `payment.requested.v1` topic'ini aç
4. `Messages` sekmesinden event payload'larını incele
5. Aynı işlemi `payment.processed.v1` için tekrarla

Beklenen örnek payload'lar:

`payment.requested.v1`

```json
{
  "paymentId": "pay-1001",
  "customerId": "cust-42",
  "amount": 2500,
  "currency": "TRY",
  "createdAt": "2026-03-29T12:00:00Z"
}
```

`payment.processed.v1`

```json
{
  "paymentId": "pay-1001",
  "customerId": "cust-42",
  "amount": 2500,
  "currency": "TRY",
  "status": "APPROVED",
  "reason": "Mock bank approval",
  "processedAt": "2026-03-29T12:00:01Z"
}
```

## Test Notu

[DemoApplicationTests](src/test/java/com/kafka/demo/DemoApplicationTests.java) Spring context'i ayağa kaldırır. Uygulama Kafka'ya bağlanmaya çalıştığı için testleri broker açıkken çalıştırmak gerekir.

## Kapatma

```bash
docker compose down -v
```