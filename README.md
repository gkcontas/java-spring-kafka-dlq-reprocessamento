# Kafka com DLQ e reprocessamento sob demanda

O ciclo completo de tratamento de falha em consumo Kafka: retry com backoff, classificação entre erro transitório e permanente, envio para **dead letter queue** quando o retry se esgota, e um **endpoint que reprocessa** as mensagens da DLQ republicando-as no tópico original.

Consumir de um tópico é fácil. O conteúdo deste projeto é o que vem depois: não bloquear a partição por causa de uma mensagem ruim, não perder mensagem, não criar loop infinito de reprocessamento, e garantir que replay não duplique efeito.

## Status

✅ Implementado, testado e validado contra Kafka e Postgres reais.

## Stack

- Java 21 + Spring Boot 3.5 + Spring for Apache Kafka
- Apache Kafka (KRaft, sem ZooKeeper) + PostgreSQL 16 + Flyway
- Gradle (Kotlin DSL) + wrapper `gradlew`
- Lombok nas entidades; eventos e DTOs como records
- Testcontainers (Kafka + Postgres) + Awaitility + JUnit 5 + AssertJ

## O fluxo

```
POST /orders  ──►  orders  ──►  OrderConsumer
                                     │
                                     ├─ sucesso ──► PostgreSQL (idempotente por event_id)
                                     │
                                     ├─ erro transitório ──► backoff exponencial (3 tentativas)
                                     │                          └─ esgotou ──┐
                                     │                                       ▼
                                     └─ erro permanente ────────────────► orders.DLT
                                                                             │
                        GET    /dlq            lista sem consumir  ◄─────────┤
                        POST   /dlq/reprocess  republica em orders ◄─────────┤
                        DELETE /dlq/{p}/{off}  descarta            ◄─────────┘
```

## Rodando de verdade

Quatro pedidos publicados: um normal, um com quantidade zero, um item sem estoque permanente, e um item que recupera depois.

**A DLQ, com a causa raiz:**

```
  p1/off0 tent=0  out-of-stock-widget   InventoryUnavailableException: Inventory for 'out-of-stock-widget' is unavailable
  p2/off0 tent=0  mouse                 OrderValidationException: Quantity must be positive, got 0
```

**Reprocessando tudo:**

```json
{ "republished": 3, "discarded": 0,
  "notes": ["0/0 republished (attempt 1)", "2/0 republished (attempt 1)", "2/1 republished (attempt 1)"] }
```

Resultado: o `recovering-widget` foi persistido, e os outros dois voltaram para a DLQ com `tent=1`. Reprocessando mais duas vezes, o contador sobe `1 → 2 → 3` e na quarta rodada:

```json
{ "republished": 0, "discarded": 2,
  "notes": ["0/3 discarded: reprocessed 3 time(s), reaching the ceiling of 3", ...] }
```

A trilha de auditoria fica no banco:

```
 partition_id | record_offset | disposition |                reason
--------------+---------------+-------------+---------------------------------------
            0 |             0 | REPROCESSED | -
            2 |             3 | REPROCESSED | -
            0 |             3 | DISCARDED   | reached the reprocessing ceiling of 3
```

## Três bugs que só apareceram rodando

### 1. A DLQ mentia sobre a causa da falha

A primeira listagem mostrava isto:

```
exceptionType: org.springframework.kafka.listener.ListenerExecutionFailedException
exceptionMessage: Listener method 'public void com.gkcontas.orders.consumer.OrderConsumer.onOrderPlaced(com...
```

Os headers padrão do `DeadLetterPublishingRecoverer` registram a exceção que o Spring Kafka capturou — que é **sempre** o wrapper dele. Um operador olhando a fila recebe o nome de uma classe de framework e uma mensagem sobre assinatura de método: nada que ajude a decidir o que fazer com o registro, e nenhuma forma de distinguir falha de validação de falha de estoque.

Correção: um `headersFunction` no recoverer que grava a causa raiz em headers próprios.

```java
recoverer.setHeadersFunction((record, exception) -> {
    Throwable rootCause = rootCauseOf(exception);
    Headers headers = new RecordHeaders();
    headers.add(FAILURE_TYPE_HEADER, rootCause.getClass().getName().getBytes(UTF_8));
    headers.add(FAILURE_MESSAGE_HEADER, String.valueOf(rootCause.getMessage()).getBytes(UTF_8));
    return headers;
});
```

A listagem lê esses primeiro e cai nos headers padrão como fallback. O resultado é a saída legível mostrada acima.

### 2. `@ServiceConnection` não chega ao `KafkaProperties` — 5m17s virou 53s

A suíte de testes falhava em tudo que envolvia a DLQ, com cada chamada de listagem bloqueando 30 segundos. A causa levou um tempo para aparecer.

O consumer de inspeção era montado com `kafkaProperties.buildConsumerProperties(null)`. Mas `KafkaProperties` só sabe o que os arquivos de configuração dizem — e o `@ServiceConnection` do Testcontainers entrega o endereço do broker por **bean**, não por propriedade. O consumer apontava para o `localhost:9092` do `application.yml`, que no ambiente de teste não existia, e bloqueava até o timeout de API expirar.

O mesmo vale para qualquer `ConnectionDetails` customizado, em produção. A correção é também melhor desenho: montar o consumer a partir da configuração do `ConsumerFactory` auto-configurado, que é por definição a mesma que os consumers reais da aplicação usam.

```java
Map<String, Object> config = new HashMap<>(consumerFactory.getConfigurationProperties());
```

A suíte caiu de **5m17s para 53s**, porque cada uma das dezenas de chamadas de listagem deixou de esperar 30 segundos.

### 3. O laço de leitura custava 15 segundos por chamada, sempre

A primeira versão polava um número fixo de rodadas, ou até `consumer.position()` alcançar o offset final. Os dois têm o mesmo defeito: um poll vazio é indistinguível de "ainda não chegou", então o laço paga o custo total em toda chamada — inclusive quando a fila está vazia.

Correção: calcular quantos registros o log tem **antes** de polar, pelas marcas d'água.

```java
long expectedRecords = partitions.stream()
        .mapToLong(p -> endOffsets.get(p) - beginningOffsets.get(p)).sum();
```

Fila vazia retorna na hora; fila cheia para no último registro.

## Decisões de projeto

**Retryable x não-retryable é a decisão central.** Retentar uma falha de validação é desperdício puro: a mesma entrada produz a mesma resposta, três vezes mais devagar, com backoff no meio. Já um timeout de banco pode passar na próxima tentativa. A classificação fica em uma linha:

```java
errorHandler.addNotRetryableExceptions(OrderValidationException.class);
```

Testado nos dois sentidos: o erro de validação chega à DLQ com `tent=0` (nenhum retry gasto), e o transitório só chega depois de esgotar as tentativas.

**Poison pill bloqueia a partição inteira.** Sem um error handler que eventualmente desista, um registro que o listener não consegue processar é retentado para sempre — e porque o Kafka entrega uma partição em ordem, ele bloqueia **todos** os registros atrás dele. Esse é o motivo de a DLQ existir: um lugar para pôr o registro de forma que a partição siga. Há teste publicando um registro envenenado e um bom com a mesma chave (mesma partição), verificando que o bom é processado.

**`ErrorHandlingDeserializer` não é opcional.** A desserialização acontece **antes** do listener ser chamado, então um payload malformado nunca chegaria a error handler nenhum: o container falharia ao converter e ficaria em loop no mesmo offset, bloqueando a partição. O wrapper transforma isso numa exceção que o error handler entende.

**Ack manual.** `enable-auto-commit: false` e `ack-mode: MANUAL_IMMEDIATE`. Com auto-commit o broker pode registrar um offset de um registro que o listener não terminou de tratar, e uma queda perde a mensagem em silêncio — exatamente o modo de falha que toda essa máquina existe para evitar.

**Idempotência por dois mecanismos, e os dois são necessários.** A verificação de existência resolve o caso comum barato. A constraint `UNIQUE` resolve o caso que ela não pega: duas entregas do mesmo evento em voo ao mesmo tempo, ambas passando pela verificação antes de qualquer uma escrever. Confiar só na verificação é uma corrida; confiar só na constraint faz cada duplicata custar uma exceção. Verificado: mesmo `eventId` publicado duas vezes → **1 pedido**.

**Teto de tentativas de reprocessamento.** Sem ele, um registro que replay nunca conserta — um payload referenciando um cliente que não existe mais — circula entre o tópico e a DLQ pelo tempo que alguém continuar apertando o botão, gerando um dead letter novo a cada volta. O contador viaja em header porque não há outro lugar que sobreviva à ida e volta pelo tópico de dead letter.

**Listar a DLQ não pode consumi-la.** Três coisas garantem isso, e todas são necessárias: `assign` em vez de `subscribe`, para não entrar em grupo nem provocar rebalance do grupo real; auto-commit desligado e nenhum commit manual; e um `group.id` aleatório descartável. Errar isso é como um operador "só olhando a DLQ" marca a fila inteira como consumida. Há teste que lista duas vezes e confirma que os mesmos registros voltam.

**`DELETE /dlq/{p}/{off}` é um verbo conveniente, não preciso.** Kafka **não tem** como apagar um registro: o tópico é log append-only, e os únicos mecanismos de remoção são retenção e compactação, nenhum dos quais mira um offset. "Descartar" significa gravar a decisão numa tabela e a listagem passar a ignorar o registro. Qualquer outra coisa seria fingir.

**Partição -1 no destino.** O padrão do recoverer envia o dead letter para o mesmo número de partição de onde veio, o que falha direto se a DLT tiver menos partições que o tópico de origem. `-1` deixa o producer decidir.

**Ordem se perde.** Mandar para a DLQ e reprocessar depois **quebra a ordem** dentro da partição. Aceitável quando cada pedido é independente; inaceitável quando o consumo depende de sequência — e nesse caso a DLQ não é a ferramenta certa.

## Endpoints

| Método | Rota | Descrição |
|--------|------|-----------|
| POST | `/orders` | Publica um `OrderPlacedEvent` — responde **202**, não 201 |
| GET | `/orders` | Pedidos efetivamente processados |
| GET | `/orders/failure-modes` | Os nomes de item que acionam cada caminho de falha |
| GET | `/dlq` | Lista a DLQ com causa raiz e origem, **sem consumir** |
| POST | `/dlq/reprocess` | Reprocessa tudo, ou os offsets selecionados |
| DELETE | `/dlq/{partition}/{offset}` | Registra o descarte |

O POST responde 202 porque publica um evento: se um pedido sai do outro lado depende de um consumer que ainda não rodou, e "created" seria uma promessa que o endpoint não pode cumprir.

## Modos de falha simuláveis

Os nomes de item ficam em `DemoFailureRules`, numa classe claramente rotulada como recurso de demonstração em vez de espalhados pelo processor como se fossem regra de negócio:

| Entrada | O que acontece |
|---|---|
| `quantity <= 0` ou `> 100` | `OrderValidationException` — não-retryable, direto para a DLQ |
| `out-of-stock-widget` | falha sempre — esgota os retries e vai para a DLQ |
| `flaky-widget` | falha 2x e passa — os retries fazem o trabalho, nunca chega à DLQ |
| `recovering-widget` | falha mais que o orçamento de retry, vai para a DLQ, e **passa no reprocessamento** |
| qualquer outro | processado na primeira tentativa |

## Como rodar

```bash
docker compose up -d
```

```bash
./gradlew bootRun
```

## Como rodar os testes

```bash
./gradlew test
```

25 testes: 7 unitários (classificação e regras de validação) e 18 de integração contra Kafka e PostgreSQL reais via Testcontainers.

### Nota sobre Testcontainers e Docker Engine recente

Se os testes falharem com `client version 1.32 is too old. Minimum supported API version is 1.40`, a causa é o `docker-java` embutido no Testcontainers negociar a API 1.32, abaixo do mínimo aceito pelo Docker Engine 29+. Correção global, de uma linha:

```bash
echo 'api.version=1.44' > ~/.docker-java.properties
```

## Exemplo de uso

```bash
# Pedido normal
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"customerEmail":"ana@example.com","item":"teclado","quantity":2,"unitPrice":150.00}'

# Falha permanente: vai direto para a DLQ, sem gastar retry
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"customerEmail":"bruno@example.com","item":"mouse","quantity":0,"unitPrice":80.00}'

# Falha transitória longa: esgota os retries, e o reprocessamento resolve
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"customerEmail":"erica@example.com","item":"recovering-widget","quantity":5,"unitPrice":30.00}'

# Ver a fila
curl -s localhost:8080/dlq

# Reprocessar tudo
curl -s -X POST localhost:8080/dlq/reprocess -H "Content-Type: application/json" -d '{}'

# Reprocessar só um registro
curl -s -X POST localhost:8080/dlq/reprocess -H "Content-Type: application/json" \
  -d '{"records":[{"partition":2,"offset":1}]}'

# Descartar
curl -s -X DELETE "localhost:8080/dlq/2/1?reason=cliente+removido"

# Idempotência: o mesmo eventId duas vezes gera um pedido só
curl -s -X POST localhost:8080/orders -H "Content-Type: application/json" \
  -d '{"eventId":"fixo-123","customerEmail":"zoe@example.com","item":"monitor","quantity":1,"unitPrice":900.00}'
```

## Fora de escopo

Kafka Streams, Schema Registry/Avro e transações exactly-once. As duas últimas são a evolução natural: um Schema Registry impediria boa parte dos payloads malformados de serem publicados, e transações fechariam a janela entre persistir o pedido e confirmar o offset — que hoje é coberta por idempotência em vez de atomicidade.
