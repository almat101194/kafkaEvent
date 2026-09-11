package com.example.kafkademo.listener;

import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.service.MessageEventService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AntifraudEventListener implements BatchAcknowledgingMessageListener<String, AntifraudEventMessage> {

    private final MessageEventService messageEventService;

    @Override
    public void onMessage(List<ConsumerRecord<String, AntifraudEventMessage>> records, Acknowledgment acknowledgment) {
        log.info("Received batch of {} antifraud events", records.size());

        List<AntifraudEventMessage> messages = records.stream()
                .map(ConsumerRecord::value)
                .toList();

        // Сохраняем весь батч одной транзакцией (MessageEventService.persist, @Transactional —
        // вынесено в отдельный бин намеренно: self-invocation в том же классе не проходит через
        // Spring AOP прокси, и @Transactional на приватном/локальном методе просто не сработал бы).
        // Если транзакция не закоммитится — метод выбросит исключение, acknowledge() ниже не
        // вызовется, offset не сдвинется, и DefaultErrorHandler (см. KafkaConfig) передоставит
        // этот же батч повторно.
        //
        // try/catch здесь только логирует контекст (размер батча) и пробрасывает исключение
        // дальше как есть — retry-семантику (бесконечный backoff в DefaultErrorHandler,
        // редоставка батча) НЕ меняет. Проглатывать исключение и всё равно коммитить offset
        // нельзя — это тихо потеряет батч и сломает at-least-once гарантию, описанную выше.
        try {
            messageEventService.persist(messages);
        } catch (Exception e) {
            log.error("Не удалось сохранить батч antifraud events, size={}", messages.size(), e);
            throw e;
        }

        // Коммитим offset сразу за весь батч (AckMode.MANUAL_IMMEDIATE + BatchAcknowledgingMessageListener).
        // Если сервис упадёт до этой строки — при рестарте придёт повторно весь батч целиком, а не одна
        // запись, как было при поштучной обработке.
        acknowledgment.acknowledge();
    }
}
