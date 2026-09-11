package com.example.kafkademo.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;

/**
 * JPA-сущность таблицы message_event. Схему таблицы создаёт и версионирует Flyway
 * (db/migration/V1__create_message_event_table.sql) — Hibernate её не генерирует
 * (spring.jpa.hibernate.ddl-auto=validate), а только сверяет с этой сущностью.
 */
@Entity
@Table(name = "message_event")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MessageEventEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String uid;

    @Column(name = "external_id", nullable = false)
    private String externalId;

    @Column(name = "application_status", nullable = false)
    private String applicationStatus;

    @Column(name = "status_date_time", nullable = false)
    private ZonedDateTime statusDateTime;

    @Column(name = "loan_amount", nullable = false)
    private BigDecimal loanAmount;

    @Column(name = "loan_request_timestamp", nullable = false)
    private ZonedDateTime loanRequestTimestamp;

    @Column(name = "partner_id", nullable = false)
    private Integer partnerId;

    @Column(name = "bki_type", nullable = false)
    private String bkiType;

    @Column(name = "refusal_reasons")
    private String refusalReasons;

    @Column(name = "borrower_birthday", nullable = false)
    private LocalDate borrowerBirthday;

    @Column(name = "borrower_inn", nullable = false)
    private String borrowerInn;

    @Column(name = "borrower_has_special_tax_regime")
    private Boolean borrowerHasSpecialTaxRegime;

    /** Сырой JSON сообщения — для полной сохранности данных сверх отдельных колонок. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    private String payload;

    /** Проставляется значением по умолчанию в БД (now()), в insert не участвует. */
    @Column(name = "received_at", insertable = false, updatable = false)
    private OffsetDateTime receivedAt;
}
