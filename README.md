# kafka-demo

Минимальный Spring Boot (Gradle) проект с подключённым Kafka consumer.

## Состав проекта

- `build.gradle`, `settings.gradle`, `gradlew` — сборка на Gradle (обёртка включена, отдельно ставить Gradle не нужно)
- `src/main/java/.../KafkaDemoApplication.java` — точка входа Spring Boot
- `src/main/java/.../model/DemoMessage.java` — DTO сообщения (id, message, timestamp)
- `src/main/java/.../config/KafkaConsumerConfig.java` — бины `ConsumerFactory`/`ConcurrentKafkaListenerContainerFactory`: явная (не через yml-строки) настройка JSON-десериализации value в `DemoMessage`
- `src/main/java/.../consumer/KafkaMessageConsumer.java` — Kafka consumer (`@KafkaListener`), десериализует value в `DemoMessage`
- `src/main/resources/application.yml` — конфигурация подключения к Kafka (bootstrap-servers, group-id, топик)
- `docker-compose.yml` — локальный однонодовый Kafka-брокер (KRaft, без Zookeeper) + веб-UI для просмотра топиков

## 1. Поднять Kafka и PostgreSQL локально

```bash
docker compose up -d
```

Брокер будет доступен на `localhost:9092`. Веб-интерфейс kafka-ui — на http://localhost:8081 (там видно топики, партиции и сообщения).

PostgreSQL поднимется на `localhost:5432` (база `message_event`, пользователь/пароль `kafka_demo`/`kafka_demo`). Таблица `message_event` создаётся автоматически при старте приложения через Flyway-миграции (`src/main/resources/db/migration/`).

Проверить, что всё поднялось:

```bash
docker compose ps
```

## 2. Собрать и запустить приложение

```bash
./gradlew bootRun
```

При старте Spring Boot создаст consumer-группу `kafka-demo-group` и подпишется на топик `demo-topic` (задан в `application.yml`, `app.kafka.topic`). Топик создастся автоматически при первом обращении (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` в docker-compose.yml).

## 3. Отправить тестовое сообщение и проверить приём

Consumer ожидает в value JSON, соответствующий `DemoMessage`:

```json
{"id": 1, "message": "привет", "timestamp": "2026-09-01T12:00:00Z"}
```

Проще всего — через контейнер с брокером:

```bash
docker exec -it kafka-demo-broker /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic demo-topic
```

Вставьте JSON из примера выше одной строкой и нажмите Enter — в логе приложения (`./gradlew bootRun`) появится строка вида:

```
Получено сообщение: partition=0, offset=0, key=null, payload=DemoMessage{id=1, message='привет', timestamp=2026-09-01T12:00:00Z}
```

Либо отправьте сообщение через kafka-ui (http://localhost:8081 → Topics → demo-topic → Produce Message), указав тот же JSON в поле Value.

Если в value придёт невалидный JSON или JSON, не подходящий под `DemoMessage`, `ErrorHandlingDeserializer` перехватит ошибку — сообщение будет пропущено с логом ошибки, а не уронит consumer.

## Где что настраивается

- **Адрес брокера**: `spring.kafka.bootstrap-servers` в `application.yml`. По умолчанию `localhost:9092`, можно переопределить переменной окружения `KAFKA_BOOTSTRAP_SERVERS` (например, при переходе на другой/облачный брокер).
- **Топик**: `app.kafka.topic` в `application.yml`.
- **Группа консьюмеров**: `app.kafka.consumer-group-id` в `application.yml` (переопределяется `KAFKA_CONSUMER_GROUP_ID`), по умолчанию `equifax-transfer-service`. Инстансы с одинаковым значением делят партиции топика между собой (см. concurrency ниже).
- **Бизнес-логика обработки**: `AntifraudEventListener.onMessage(...)`.
- **Формат сообщения / десериализация / ack-mode**: `KafkaConfig.java` — `JsonDeserializer`, `ErrorHandlingDeserializer` и `ContainerProperties.AckMode.MANUAL_IMMEDIATE` заданы в коде, а не строками в `application.yml`.
- **Параллелизм консьюмера**: `app.kafka.consumer-concurrency` в `application.yml` (переопределяется `KAFKA_CONSUMER_CONCURRENCY`), по умолчанию 3 — не должно превышать число партиций топика `ets.antifraud-event` (см. `kafka/init.sh` и `KAFKA_NUM_PARTITIONS` в `docker-compose.yml`), иначе лишние потоки будут простаивать без партиций.
- **Подключение к БД**: `spring.datasource.*` в `application.yml` (переопределяется `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USERNAME`/`DB_PASSWORD`).
- **Схема БД**: Flyway-миграции в `src/main/resources/db/migration/` — версионируются, накатываются автоматически при старте (`spring.flyway.enabled=true`).
- **Сохранение сообщений**: `AntifraudEventListener` мапит сообщение через `MessageEventMapper` и сохраняет через `MessageEventRepository` (`JpaRepository`) перед `acknowledgment.acknowledge()`; идемпотентность по `uid` защищает от дублей при повторной доставке.

## Если нужен и Producer

В проект добавлен только consumer, как и просили. Для отправки сообщений из самого приложения достаточно завести бин `KafkaTemplate<String, String>` (Spring Boot настроит его автоматически на основе `spring.kafka.bootstrap-servers`) и вызвать `kafkaTemplate.send("demo-topic", "сообщение")`. Скажите, если нужно — добавлю producer с примером REST-эндпоинта.

## Остановить брокер

```bash
docker compose down
```

Добавьте `-v`, если нужно ещё и удалить данные брокера (`docker compose down -v`).
