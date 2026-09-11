package com.example.kafkademo.service;

import com.example.kafkademo.entity.MessageEventEntity;
import com.example.kafkademo.mapper.MessageEventMapper;
import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.model.ApplicationStatus;
import com.example.kafkademo.model.BkiType;
import com.example.kafkademo.model.Borrower;
import com.example.kafkademo.repository.MessageEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Проверяет дедупликацию и фан-аут в MessageEventService.persist — без реальной БД:
 * MessageEventRepository, MessageEventMapper и ObjectMapper замоканы; Partner/BkiType — настоящие
 * enum'ы (чистые данные, мокать их незачем) — реальный фан-аут по ним как раз и проверяется.
 * Интересует только то, какие MessageEventEntity в итоге долетают до saveAll.
 */
@ExtendWith(MockitoExtension.class)
class MessageEventServiceTest {

    @Mock
    private MessageEventRepository messageEventRepository;

    @Mock
    private MessageEventMapper messageEventMapper;

    @Mock
    private ObjectMapper objectMapper;

    @InjectMocks
    private MessageEventService messageEventService;

    // messageEventMapper тут намеренно не проверяется: его вызовы (по одному toEntity на каждый
    // применимый BkiType) уже полностью подтверждаются содержимым списка, дошедшего до saveAll —
    // дублировать это явным verify(messageEventMapper)... на каждый bkiType в каждом тесте лишнее.
    @AfterEach
    void verifyNoMoreMockInteractions() {
        verifyNoMoreInteractions(messageEventRepository);
    }

    @Test
    void persist_fansOutAndSavesAllEntities_whenNothingSavedBefore() {
        // Partner.MFI (partnerId=2) даёт 2 BkiType: SCORING_BUREAU и NBKI
        AntifraudEventMessage message = message("u1", 2);
        MessageEventEntity scoringBureau = entity("u1", "scoringBureau");
        MessageEventEntity nbki = entity("u1", "nbki");

        when(messageEventMapper.toEntity(eq(message), eq(BkiType.SCORING_BUREAU), any()))
                .thenReturn(scoringBureau);
        when(messageEventMapper.toEntity(eq(message), eq(BkiType.NBKI), any()))
                .thenReturn(nbki);
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection())).thenReturn(List.of());

        messageEventService.persist(List.of(message));

        ArgumentCaptor<List<MessageEventEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageEventRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).containsExactlyInAnyOrder(scoringBureau, nbki);
        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
    }

    @Test
    void persist_producesOneRow_forPartnerWithSingleBkiType() {
        // Partner.COOP (partnerId=4) даёт только COOP_NBKI
        AntifraudEventMessage message = message("u1", 4);
        MessageEventEntity coopNbki = entity("u1", "coopNbki");

        when(messageEventMapper.toEntity(eq(message), eq(BkiType.COOP_NBKI), any())).thenReturn(coopNbki);
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection())).thenReturn(List.of());

        messageEventService.persist(List.of(message));

        ArgumentCaptor<List<MessageEventEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageEventRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).containsExactly(coopNbki);
        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
    }

    @Test
    void persist_skipsPairAlreadyPersisted_butKeepsOthersForSameUid() {
        AntifraudEventMessage message = message("u1", 2);
        MessageEventEntity scoringBureau = entity("u1", "scoringBureau");
        MessageEventEntity nbki = entity("u1", "nbki");

        when(messageEventMapper.toEntity(eq(message), eq(BkiType.SCORING_BUREAU), any())).thenReturn(scoringBureau);
        when(messageEventMapper.toEntity(eq(message), eq(BkiType.NBKI), any())).thenReturn(nbki);
        // "nbki" для uid=u1 уже в БД (например, redelivery батча после частичного падения) —
        // "scoringBureau" для того же uid нет, это не дубль, а другая строка фан-аута.
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection()))
                .thenReturn(List.of(new Object[] {"u1", "nbki"}));

        messageEventService.persist(List.of(message));

        ArgumentCaptor<List<MessageEventEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageEventRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).containsExactly(scoringBureau);
        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
    }

    @Test
    void persist_skipsDuplicatePair_withinSameBatch() {
        // Два разных сообщения в батче (например, повторная доставка внутри одного poll'а),
        // которые после маппинга дают одинаковую пару (uid, bkiType) — должна остаться одна строка.
        // Partner.COOP (partnerId=4) даёт единственный BkiType (COOP_NBKI) — по одной entity на сообщение.
        AntifraudEventMessage first = message("u1", 4);
        AntifraudEventMessage second = message("u1", 4);
        MessageEventEntity nbki = entity("u1", "coopNbki");
        MessageEventEntity nbkiDuplicate = entity("u1", "coopNbki");

        when(messageEventMapper.toEntity(eq(first), eq(BkiType.COOP_NBKI), any())).thenReturn(nbki);
        when(messageEventMapper.toEntity(eq(second), eq(BkiType.COOP_NBKI), any())).thenReturn(nbkiDuplicate);
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection())).thenReturn(List.of());

        messageEventService.persist(List.of(first, second));

        ArgumentCaptor<List<MessageEventEntity>> captor = ArgumentCaptor.forClass(List.class);
        verify(messageEventRepository).saveAll(captor.capture());
        assertThat(captor.getValue()).containsExactly(nbki);
        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
    }

    @Test
    void persist_doesNotCallSaveAll_whenEverythingAlreadyPersisted() {
        AntifraudEventMessage message = message("u1", 4);
        MessageEventEntity coopNbki = entity("u1", "coopNbki");

        when(messageEventMapper.toEntity(eq(message), eq(BkiType.COOP_NBKI), any())).thenReturn(coopNbki);
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection()))
                .thenReturn(List.of(new Object[] {"u1", "coopNbki"}));

        messageEventService.persist(List.of(message));

        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
        verify(messageEventRepository, never()).saveAll(any());
    }

    @Test
    void persist_throws_forUnknownPartnerId() {
        AntifraudEventMessage message = message("u1", 999);
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection())).thenReturn(List.of());

        assertThatThrownBy(() -> messageEventService.persist(List.of(message)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("999");

        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
    }

    @Test
    void persist_doesNothing_forEmpty
    Batch() {
        when(messageEventRepository.findExistingUidBkiTypePairs(anyCollection())).thenReturn(List.of());

        messageEventService.persist(List.of());

        verify(messageEventRepository).findExistingUidBkiTypePairs(anyCollection());
        verify(messageEventRepository, never()).saveAll(any());
    }

    private static AntifraudEventMessage message(String uid, int partnerId) {
        return AntifraudEventMessage.builder()
                .uid(uid)
                .externalId("ext-" + uid)
                .applicationStatus(ApplicationStatus.APPROVED)
                .statusDateTime(ZonedDateTime.now())
                .loanAmount(new BigDecimal("100.00"))
                .loanRequestTimestamp(ZonedDateTime.now())
                .partnerId(partnerId)
                .borrower(Borrower.builder()
                        .birthday(LocalDate.of(1990, 1, 1))
                        .inn("1234567890")
                        .build())
                .build();
    }

    private static MessageEventEntity entity(String uid, String bkiType) {
        return MessageEventEntity.builder()
                .uid(uid)
                .bkiType(bkiType)
                .build();
    }
}
