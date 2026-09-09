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

## 1. Поднять Kafka локально

```bash
docker compose up -d
```

Брокер будет доступен на `localhost:9092`. Веб-интерфейс kafka-ui — на http://localhost:8081 (там видно топики, партиции и сообщения).

Проверить, что брокер поднялся:

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
- **Группа консьюмеров**: `spring.kafka.consumer.group-id`.
- **Бизнес-логика обработки**: метод `listen(...)` в `KafkaMessageConsumer.java` — там, где стоит `// TODO`.
- **Формат сообщения / десериализация**: `KafkaConsumerConfig.java` — там же `JsonDeserializer`, `ErrorHandlingDeserializer` и ack-mode заданы в коде, а не строками в `application.yml`.

## Если нужен и Producer

В проект добавлен только consumer, как и просили. Для отправки сообщений из самого приложения достаточно завести бин `KafkaTemplate<String, String>` (Spring Boot настроит его автоматически на основе `spring.kafka.bootstrap-servers`) и вызвать `kafkaTemplate.send("demo-topic", "сообщение")`. Скажите, если нужно — добавлю producer с примером REST-эндпоинта.

## Остановить брокер

```bash
docker compose down
```

Добавьте `-v`, если нужно ещё и удалить данные брокера (`docker compose down -v`).
