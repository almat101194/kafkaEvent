package com.example.kafkademo.mapper;

import com.example.kafkademo.entity.MessageEventEntity;
import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.model.ApplicationStatus;
import com.example.kafkademo.model.BkiType;
import com.example.kafkademo.model.Borrower;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Юнит-тесты MessageEventMapper без Spring-контекста и без Mockito: MapStruct-процессор
 * генерирует MessageEventMapperImpl при компиляции, у интерфейса нет полей/зависимостей —
 * достаточно голого "new". payload — обычный String-параметр (не сериализация), поэтому
 * ObjectMapper тут вообще не нужен, ни настоящий, ни замоканный.
 */
class MessageEventMapperTest {

    private final MessageEventMapper mapper = new MessageEventMapperImpl();

    @Test
    void toEntity_copiesScalarFieldsFromMessage() {
        AntifraudEventMessage message = message();

        MessageEventEntity entity = mapper.toEntity(message, BkiType.NBKI, "{\"uid\":\"u1\"}");

        assertThat(entity.getUid()).isEqualTo(message.getUid());
        assertThat(entity.getExternalId()).isEqualTo(message.getExternalId());
        assertThat(entity.getApplicationStatus()).isEqualTo(message.getApplicationStatus().getStatusName());
        assertThat(entity.getStatusDateTime()).isEqualTo(message.getStatusDateTime());
        assertThat(entity.getLoanAmount()).isEqualTo(message.getLoanAmount());
        assertThat(entity.getLoanRequestTimestamp()).isEqualTo(message.getLoanRequestTimestamp());
        assertThat(entity.getPartnerId()).isEqualTo(message.getPartnerId());
    }

    @Test
    void toEntity_flattensBorrowerFields() {
        AntifraudEventMessage message = message();

        MessageEventEntity entity = mapper.toEntity(message, BkiType.NBKI, "payload");

        assertThat(entity.getBorrowerBirthday()).isEqualTo(message.getBorrower().getBirthday());
        assertThat(entity.getBorrowerInn()).isEqualTo(message.getBorrower().getInn());
        assertThat(entity.getBorrowerHasSpecialTaxRegime())
                .isEqualTo(message.getBorrower().getHasSpecialTaxRegime());
    }

    @Test
    void toEntity_mapsBkiTypeByName() {
        MessageEventEntity entity = mapper.toEntity(message(), BkiType.COOP_NBKI, "payload");

        assertThat(entity.getBkiType()).isEqualTo("coopNbki");
    }

    @Test
    void toEntity_assignsPayloadParameterDirectly() {
        MessageEventEntity entity = mapper.toEntity(message(), BkiType.NBKI, "raw-payload-123");

        assertThat(entity.getPayload()).isEqualTo("raw-payload-123");
    }

    @Test
    void toEntity_joinsRefusalReasonsWithComma() {
        AntifraudEventMessage message = message().toBuilder()
                .refusalReasons(List.of("2.19.1", "2.19.2"))
                .build();

        MessageEventEntity entity = mapper.toEntity(message, BkiType.NBKI, "payload");

        assertThat(entity.getRefusalReasons()).isEqualTo("2.19.1,2.19.2");
    }

    @Test
    void toEntity_leavesRefusalReasonsNull_whenAbsentInMessage() {
        MessageEventEntity entity = mapper.toEntity(message(), BkiType.NBKI, "payload");

        assertThat(entity.getRefusalReasons()).isNull();
    }

    @Test
    void toEntity_ignoresIdAndReceivedAt() {
        MessageEventEntity entity = mapper.toEntity(message(), BkiType.NBKI, "payload");

        assertThat(entity.getId()).isNull();
        assertThat(entity.getReceivedAt()).isNull();
    }

    private static AntifraudEventMessage message() {
        return AntifraudEventMessage.builder()
                .uid("uid-1")
                .externalId("ext-1")
                .applicationStatus(ApplicationStatus.APPROVED)
                .statusDateTime(ZonedDateTime.now())
                .loanAmount(new BigDecimal("100.00"))
                .loanRequestTimestamp(ZonedDateTime.now())
                .partnerId(2)
                .borrower(Borrower.builder()
                        .birthday(LocalDate.of(1990, 1, 1))
                        .inn("1234567890")
                        .hasSpecialTaxRegime(true)
                        .build())
                .build();
    }
}
