# Kafka Event-Driven Demo

Basit bir Spring Boot + Kafka demosu. Amaç, event-driven akışın temelini küçük ve anlaşılır bir örnek üzerinden görmek:

- HTTP isteği alınır
- Kafka'ya event yazılır
- Bir consumer bu event'i işler
- Yeni bir event üretilir
- Başka bir consumer son event'i tüketir
- Hata durumunda mesaj retry edilir
- Retry'ler tükenirse mesaj dead letter topic'e düşer

Bu proje öğrenme amaçlıdır. Producer ve consumer'lar aynı uygulama içinde çalışır; yani bu yapı tam bir mikroservis dağılımı değil, event akışını göstermek için sadeleştirilmiş bir demodur.

## Teknolojiler

- Java 21
- Spring Boot 4
- Spring for Apache Kafka
- Docker Compose
- Kafka UI

## Senaryo

Ödeme isteği sisteme gelir, `payment.requested.v1` topic'ine yazılır. Yetkilendirme consumer'ı bu event'i tüketir, basit bir kural ile kararı verir ve `payment.processed.v1` event'i üretir. Son olarak ledger/notification consumer'ı bu event'i okuyup loglar. Eğer consumer tarafında hata oluşursa mesaj belirli sayıda retry edilir; yine başarısız olursa ilgili dead letter topic'e gönderilir.

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
        |
        +--> hata olursa retry --> DLT
```

## Proje Yapısı

- [PaymentController](src/main/java/com/kafka/demo/api/PaymentController.java): HTTP isteğini alır.
- [PaymentEventProducer](src/main/java/com/kafka/demo/producer/PaymentEventProducer.java): Kafka'ya event üretir.
- [AuthorizationConsumer](src/main/java/com/kafka/demo/consumer/AuthorizationConsumer.java): `payment.requested.v1` event'ini tüketir, karar verir.
- [LedgerConsumer](src/main/java/com/kafka/demo/consumer/LedgerConsumer.java): `payment.processed.v1` event'ini tüketir.
- [DeadLetterConsumer](src/main/java/com/kafka/demo/consumer/DeadLetterConsumer.java): DLT mesajlarını loglar.
- [KafkaTopicConfig](src/main/java/com/kafka/demo/config/KafkaTopicConfig.java): Topic bean'lerini oluşturur.
- [KafkaListenerConfig](src/main/java/com/kafka/demo/config/KafkaListenerConfig.java): Retry ve DLT error handler konfigürasyonunu kurar.
- [compose.yaml](compose.yaml): Kafka ve Kafka UI altyapısını ayağa kaldırır.

## Topic'ler

- `payment.requested.v1`
- `payment.processed.v1`
- `payment.requested.v1.dlt`
- `payment.processed.v1.dlt`

## Retry ve DLT Davranışı

- `app.retry.max-attempts=3`
- `app.retry.backoff-ms=2000`
- Consumer bir hata fırlatırsa mesaj aynı consumer tarafından yeniden denenir
- Tüm denemeler başarısız olursa mesaj ilgili `.dlt` topic'ine taşınır

Bu ayarlar [application.yaml](src/main/resources/application.yaml) içinde tutulur.

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
    "currency": "TRY",
    "scenario": null
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
    "currency": "TRY",
    "scenario": null
  }'
```

## Hata Senaryosu Simülasyonu

Demo amaçlı olarak isteğe opsiyonel `scenario` alanı eklendi:

- `FAIL_AUTH`: Authorization consumer hata üretir, retry sonrası mesaj `payment.requested.v1.dlt` topic'ine düşer
- `FAIL_LEDGER`: Ledger consumer hata üretir, retry sonrası mesaj `payment.processed.v1.dlt` topic'ine düşer

### Authorization hatası simülasyonu

```bash
curl -X POST http://localhost:8080/api/payments \
  -H 'Content-Type: application/json' \
  -d '{
    "paymentId": "pay-fail-auth-1",
    "customerId": "cust-42",
    "amount": 2500,
    "currency": "TRY",
    "scenario": "FAIL_AUTH"
  }'
```

### Ledger hatası simülasyonu

```bash
curl -X POST http://localhost:8080/api/payments \
  -H 'Content-Type: application/json' \
  -d '{
    "paymentId": "pay-fail-ledger-1",
    "customerId": "cust-42",
    "amount": 2500,
    "currency": "TRY",
    "scenario": "FAIL_LEDGER"
  }'
```

## Eventleri Nasıl Görürüm?

### 1. Uygulama logları

İstek attıktan sonra loglarda şu satırları görürsün:

```text
Authorization sonucu -> paymentId=..., status=...
Ledger/notification simülasyonu -> paymentId=..., status=..., amount=..., currency=...
DLT consume -> dltTopic=..., originalTopic=..., originalOffset=..., exceptionClass=..., exceptionMessage=...
```

### 2. Kafka UI

[http://localhost:8081](http://localhost:8081) adresine git ve şu adımları izle:

1. `local` cluster'ını aç
2. `Topics` bölümüne gir
3. `payment.requested.v1` topic'ini aç
4. `Messages` sekmesinden event payload'larını incele
5. Aynı işlemi `payment.processed.v1` için tekrarla
6. Retry sonrası başarısız mesajları görmek için `payment.requested.v1.dlt` ve `payment.processed.v1.dlt` topic'lerini de incele

Beklenen örnek payload'lar:

`payment.requested.v1`

```json
{
  "paymentId": "pay-1001",
  "customerId": "cust-42",
  "amount": 2500,
  "currency": "TRY",
  "scenario": null,
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
  "scenario": null,
  "status": "APPROVED",
  "reason": "Mock bank approval",
  "processedAt": "2026-03-29T12:00:01Z"
}
```

`payment.requested.v1.dlt`

```json
{
  "paymentId": "pay-fail-auth-1",
  "customerId": "cust-42",
  "amount": 2500,
  "currency": "TRY",
  "scenario": "FAIL_AUTH",
  "createdAt": "2026-03-29T12:00:00Z"
}
```

`payment.processed.v1.dlt`

```json
{
  "paymentId": "pay-fail-ledger-1",
  "customerId": "cust-42",
  "amount": 2500,
  "currency": "TRY",
  "scenario": "FAIL_LEDGER",
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
