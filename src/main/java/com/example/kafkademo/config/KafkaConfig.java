package com.example.kafkademo.config;

import com.example.kafkademo.listener.AntifraudEventListener;
import com.example.kafkademo.model.AntifraudEventMessage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.ExponentialBackOff;

import java.util.Map;

@Configuration
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    private final SslBundles sslBundles;

    @Value("${app.kafka.consumer-group-id}")
    private String groupId;

    // Не должно превышать число партиций топика ets.antifraud-event (сейчас 3, см. kafka/init.sh
    // и KAFKA_NUM_PARTITIONS в docker-compose.yml) — лишние потоки просто не получат партицию и будут простаивать.
    @Value("${app.kafka.consumer-concurrency:3}")
    private int consumerConcurrency;

    // Максимум записей за один poll() = максимальный размер батча, который увидит listener за раз.
    @Value("${app.kafka.consumer-batch-size:500}")
    private int consumerBatchSize;

    // Backoff для ретраев при исключениях, которые listener не обработал сам (например, БД недоступна).
    // Растёт экспоненциально до max-interval и НЕ останавливается сам по себе (maxElapsedTime не задан,
    // т.е. бесконечен) — сознательный выбор в пользу "лучше стоять и ждать восстановления, чем молча
    // потерять батч", т.к. дефолтный backoff Spring Kafka (FixedBackOff(0, 9) — 10 попыток почти без
    // пауз) для просадки БД длиннее пары секунд успевает исчерпаться и пропустить батч.
    @Value("${app.kafka.error-backoff-initial-interval-ms:1000}")
    private long errorBackoffInitialInterval;

    @Value("${app.kafka.error-backoff-multiplier:2.0}")
    private double errorBackoffMultiplier;

    @Value("" +
            "${app.kafka.error-backoff-max-interval-ms:30000}")
    private long errorBackoffMaxInterval;

    public KafkaConfig(KafkaProperties kafkaProperties, SslBundles sslBundles) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
    }

    @Bean
    public ConsumerFactory<String, AntifraudEventMessage> consumerFactory(
            @Qualifier("kafkaObjectMapper") ObjectMapper kafkaObjectMapper) {
        // bootstrap-servers и SASL-свойства подтягиваются из spring.kafka.* (application.yml)
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(sslBundles);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, consumerBatchSize);

        // Десериализатор value конструируем явно (со своим kafkaObjectMapper, см. JacksonConfig),
        // а не строковыми properties (TRUSTED_PACKAGES/VALUE_DEFAULT_TYPE/USE_TYPE_INFO_HEADERS) —
        // так сразу видно и тип, и маппер, которым он парсится, без reflection "по имени класса".
        // false в конце — не доверяем типу из заголовков записи, всегда десериализуем в
        // AntifraudEventMessage.
        JsonDeserializer<AntifraudEventMessage> jsonDeserializer =
                new JsonDeserializer<>(AntifraudEventMessage.class, kafkaObjectMapper, false);
        // ErrorHandlingDeserializer оборачивает делегат, чтобы "отравленное" сообщение (битый JSON,
        // не прошедшая валидацию структура) не роняло listener-контейнер, а попадало в error handler.
        ErrorHandlingDeserializer<AntifraudEventMessage> valueDeserializer =
                new ErrorHandlingDeserializer<>(jsonDeserializer);

        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), valueDeserializer);
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(errorBackoffInitialInterval, errorBackoffMultiplier);
        backOff.setMaxInterval(errorBackoffMaxInterval);
        // maxElapsedTime намеренно не ограничен (ExponentialBackOff по умолчанию — Long.MAX_VALUE):
        // ретраит бесконечно, а не "сдаётся" и не пропускает батч через несколько минут. Обратная
        // сторона: батч с "отравленной" (не десериализуемой) записью будет ретраиться бесконечно
        // и блокировать партицию — изоляции отдельной записи (BatchListenerFailedException) в
        // проекте больше нет, это осознанный компромисс.
        return new DefaultErrorHandler(backOff);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, AntifraudEventMessage> antifraudEventListenerContainer(
            ConsumerFactory<String, AntifraudEventMessage> consumerFactory,
            AntifraudEventListener antifraudEventListener,
            DefaultErrorHandler kafkaErrorHandler,
            @Value("${app.kafka.topic}") String topic) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        // Batch-режим контейнер определяет автоматически по интерфейсу listener'а: antifraudEventListener
        // реализует BatchAcknowledgingMessageListener, поэтому получит весь батч, отданный одним poll()
        // (до max.poll.records / consumer-batch-size), а не по одной записи.
        containerProperties.setMessageListener(antifraudEventListener);
        // MANUAL_IMMEDIATE: offset коммитится синхронно сразу после вызова acknowledgment.acknowledge()
        // в listener'е — но теперь один раз за весь батч, а не за одну запись (а не пачкой по offset'ам
        // с задержкой, как AckMode.BATCH по умолчанию). Обратная сторона батчинга: окно повторной
        // обработки при падении сервиса — весь батч целиком, а не одна запись.
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);

        ConcurrentMessageListenerContainer<String, AntifraudEventMessage> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setBeanName("antifraudEventListenerContainer");
        // Поднимает N независимых consumer-потоков (каждый — свой KafkaConsumer), партиции топика
        // распределяются между ними через обычный group-rebalance. Каждый поток по-прежнему
        // обрабатывает свои записи последовательно (poll -> onMessage -> commit).
        container.setConcurrency(consumerConcurrency);
        // Обрабатывает исключения, которые listener сам не поймал (например, БД недоступна) —
        // ретраит с backoff'ом выше, бесконечно (см. errorBackoffMaxInterval).
        container.setCommonErrorHandler(kafkaErrorHandler);
        return container;
    }
}
