package com.example.kafkademo.listener;

import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.service.MessageEventService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.support.Acknowledgment;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;

/**
 * Юнит-тесты AntifraudEventListener: MessageEventService и Acknowledgment замоканы —
 * интересует только оркестрация в onMessage (что реально передаётся в persist, когда
 * вызывается/не вызывается acknowledge, что происходит с исключением из persist — см.
 * обсуждение семантики at-least-once в комментариях самого AntifraudEventListener).
 */
@ExtendWith(MockitoExtension.class)
class AntifraudEventListenerTest {

    @Mock
    private MessageEventService messageEventService;

    @Mock
    private Acknowledgment acknowledgment;

    @InjectMocks
    private AntifraudEventListener listener;

    // Страховка от недостающих verify(...) в самих тестах: если после теста на моках остался
    // неверифицированный вызов — значит тест либо забыл его проверить, либо listener сделал что-то
    // лишнее сверх ожидаемого поведения.
    @AfterEach
    void verifyNoMoreMockInteractions() {
        verifyNoMoreInteractions(messageEventService, acknowledgment);
    }

    @Test
    void onMessage_persistsValuesOfAllRecords_inOrder() {
        AntifraudEventMessage first = message("u1");
        AntifraudEventMessage second = message("u2");
        List<ConsumerRecord<String, AntifraudEventMessage>> records = List.of(record(first), record(second));

        listener.onMessage(records, acknowledgment);

        ArgumentCaptor<List<AntifraudEventMessage>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageEventService).persist(captor.capture());
        assertThat(captor.getValue()).containsExactly(first, second);
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onMessage_acknowledgesBatch_afterSuccessfulPersist() {
        AntifraudEventMessage message = message("u1");
        List<ConsumerRecord<String, AntifraudEventMessage>> records = List.of(record(message));

        listener.onMessage(records, acknowledgment);

        verify(messageEventService).persist(List.of(message));
        verify(acknowledgment).acknowledge();
    }

    @Test
    void onMessage_rethrowsException_andNeverAcknowledges_whenPersistFails() {
        AntifraudEventMessage message = message("u1");
        List<ConsumerRecord<String, AntifraudEventMessage>> records = List.of(record(message));
        RuntimeException failure = new RuntimeException("db is down");
        doThrow(failure).when(messageEventService).persist(List.of(message));

        assertThatThrownBy(() -> listener.onMessage(records, acknowledgment))
                .isSameAs(failure);

        verify(messageEventService).persist(List.of(message));
        // acknowledge() не должен вызваться вообще — иначе offset сдвинется, а батч не сохранится
        // (тихая потеря данных, о которой предупреждает комментарий в самом листенере).
        verifyNoInteractions(acknowledgment);
    }

    @Test
    void onMessage_persistsEmptyList_andAcknowledges_forEmptyBatch() {
        listener.onMessage(List.of(), acknowledgment);

        verify(messageEventService).persist(List.of());
        verify(acknowledgment).acknowledge();
    }

    private static ConsumerRecord<String, AntifraudEventMessage> record(AntifraudEventMessage value) {
        return new ConsumerRecord<>("ets.antifraud-event", 0, 0L, value.getUid(), value);
    }

    private static AntifraudEventMessage message(String uid) {
        return AntifraudEventMessage.builder().uid(uid).build();
    }
}
