package com.example.kafkademo.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.TimeZone;

/**
 * Приложение не подключает spring-boot-starter-web, поэтому Spring Boot не может сам
 * поднять бин ObjectMapper через JacksonAutoConfiguration — она требует на classpath
 * Jackson2ObjectMapperBuilder, а этот класс лежит в spring-web, которого здесь нет.
 * Заводим ObjectMapper явными бинами.
 */
@Configuration
public class JacksonConfig {

    /**
     * Отдельный ObjectMapper для десериализации входящих Kafka-сообщений (см. KafkaConfig).
     * FAIL_ON_UNKNOWN_PROPERTIES выключен намеренно: если продюсер добавит новое поле в схему
     * (forward-совместимая эволюция), мы не должны считать из-за этого сообщение "отравленным"
     * и терять его — просто игнорируем незнакомое поле.
     */
    @Bean
    public ObjectMapper kafkaObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
                .disable(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS)
                .setTimeZone(TimeZone.getDefault());
    }
}
