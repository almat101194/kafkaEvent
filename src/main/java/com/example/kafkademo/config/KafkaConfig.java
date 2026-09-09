package com.example.kafkademo.config;

import com.example.kafkademo.listener.AntifraudEventListener;
import com.example.kafkademo.model.AntifraudEventMessage;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.SslBundles;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.Map;

@Configuration
public class KafkaConfig {

    private static final String TRUSTED_PACKAGE = "com.example.kafkademo.model";

    private final KafkaProperties kafkaProperties;

    private final SslBundles sslBundles;

    @Value("${KAFKA_CONSUMER_GROUP_ID:equifax-transfer-service}")
    private String groupId;

    public KafkaConfig(KafkaProperties kafkaProperties, SslBundles sslBundles) {
        this.kafkaProperties = kafkaProperties;
        this.sslBundles = sslBundles;
    }

    @Bean
    public ConsumerFactory<String, AntifraudEventMessage> consumerFactory() {
        // bootstrap-servers и SASL-свойства подтягиваются из spring.kafka.* (application.yml)
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(sslBundles);

        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Значение оборачиваем в ErrorHandlingDeserializer, чтобы "отравленное" сообщение
        // не роняло listener-контейнер, а попадало в error handler
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        props.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
        props.put(JsonDeserializer.TRUSTED_PACKAGES, TRUSTED_PACKAGE);
        props.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, AntifraudEventMessage.class.getName());

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentMessageListenerContainer<String, AntifraudEventMessage> antifraudEventListenerContainer(
            ConsumerFactory<String, AntifraudEventMessage> consumerFactory,
            AntifraudEventListener antifraudEventListener,
            @Value("${app.kafka.topic}") String topic) {
        ContainerProperties containerProperties = new ContainerProperties(topic);
        containerProperties.setMessageListener(antifraudEventListener);

        ConcurrentMessageListenerContainer<String, AntifraudEventMessage> container =
                new ConcurrentMessageListenerContainer<>(consumerFactory, containerProperties);
        container.setBeanName("antifraudEventListenerContainer");
        return container;
    }
}
