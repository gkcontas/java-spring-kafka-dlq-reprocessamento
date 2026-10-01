# Kafka com DLQ e Reprocessamento

Serviço de pedidos consumido a partir de um tópico Kafka, com dead-letter queue e endpoint para reprocessar sob demanda o que falhou.

O consumidor distingue falhas transitórias de permanentes: as transitórias são repetidas com backoff exponencial, as permanentes vão direto para a DLQ sem gastar tentativa. O que acaba na DLQ pode ser inspecionado e republicado no tópico original por uma chamada HTTP, com um contador de tentativas que impede o registro de ficar circulando para sempre.

## Como funciona

```
POST /orders ──► tópico "orders" ──► consumidor
                                         │
                      sucesso ───────────┤
                                         │
                  falha transitória ──► retry com backoff exponencial
                                         │ esgotou
                  falha permanente ──────┴──► tópico "orders.DLT"
                                                     │
                                       GET /dlq ◄────┤
                                                     │
                            POST /dlq/reprocess ─────┴──► volta para "orders"
```

A leitura da DLQ não consome o tópico: o endpoint faz `assign` e `seek` sem confirmar offset, então consultar a fila não a esvazia. As mensagens na DLQ carregam cabeçalhos com o tópico de origem, a partição, o offset e a exceção raiz que causou a falha.

O consumo é idempotente por `eventId` — republicar a mesma mensagem não duplica o pedido.

## Tecnologias e bibliotecas

| | |
|---|---|
| Linguagem | Java 21 |
| Framework | Spring Boot 3.5 |
| Mensageria | Apache Kafka via Spring Kafka (`DefaultErrorHandler`, `DeadLetterPublishingRecoverer`) |
| Persistência | Spring Data JPA, PostgreSQL 16 |
| Migrations | Flyway |
| Validação | Bean Validation |
| Build | Gradle Kotlin DSL (wrapper `gradlew`) |
| Testes | JUnit 5, Awaitility, Spring Kafka Test, Testcontainers (Kafka e PostgreSQL) |
| Apoio | Lombok |

## Pré-requisitos

- JDK 21 ou superior
- Docker

## Como rodar

```bash
docker compose up -d
```

```bash
./gradlew bootRun
```

A API fica em `http://localhost:8080`.

## Endpoints

| Método | Rota | Descrição |
|---|---|---|
| `POST` | `/orders` | Publica um pedido no tópico `orders` |
| `GET` | `/orders` | Lista os pedidos processados |
| `GET` | `/orders/failure-modes` | Mostra quais entradas provocam cada tipo de falha |
| `GET` | `/dlq` | Lista o conteúdo da DLQ sem consumi-la |
| `POST` | `/dlq/reprocess` | Republica registros no tópico original |
| `DELETE` | `/dlq/{partition}/{offset}` | Descarta um registro da DLQ |

## Exemplos de uso

Pedido normal:

```bash
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"customerEmail":"ana@example.com","item":"teclado","quantity":2,"unitPrice":150.00}'
```

Falha permanente — vai direto para a DLQ, sem gastar retry:

```bash
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"customerEmail":"bruno@example.com","item":"mouse","quantity":0,"unitPrice":80.00}'
```

Falha transitória longa — esgota as tentativas, e o reprocessamento resolve:

```bash
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"customerEmail":"erica@example.com","item":"recovering-widget","quantity":5,"unitPrice":30.00}'
```

Inspecionar e reprocessar:

```bash
curl -s localhost:8080/dlq
```

```bash
curl -s -X POST localhost:8080/dlq/reprocess -H "Content-Type: application/json" -d '{}'
```

```bash
curl -s -X POST localhost:8080/dlq/reprocess -H "Content-Type: application/json" \
  -d '{"records":[{"partition":2,"offset":1}]}'
```

```bash
curl -s -X DELETE "localhost:8080/dlq/2/1?reason=cliente+removido"
```

Idempotência — o mesmo `eventId` duas vezes gera um pedido só:

```bash
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"eventId":"fixo-123","customerEmail":"zoe@example.com","item":"monitor","quantity":1,"unitPrice":900.00}'
```

Consulte `GET /orders/failure-modes` para ver todas as entradas que disparam cada cenário.

## Testes

```bash
./gradlew test
```

25 testes: 7 unitários, sobre classificação de falhas e regras de validação, e 18 de integração contra Kafka e PostgreSQL reais em containers.

## Fora de escopo

Kafka Streams, Schema Registry com Avro e transações exactly-once. As duas últimas são a evolução natural: um Schema Registry barraria boa parte dos payloads malformados antes da publicação, e transações fechariam a janela entre persistir o pedido e confirmar o offset — hoje coberta por idempotência.
