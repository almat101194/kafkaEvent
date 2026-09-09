package com.example.kafkademo.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;

import jakarta.validation.constraints.Pattern;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.List;

@Value
@Builder(toBuilder = true)
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AntifraudEventMessage {

    @NonNull
    Borrower borrower;

    @NonNull
    String uid;

    @NonNull
    String externalId;

    @NonNull
    ApplicationStatus applicationStatus;

    @NonNull
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    ZonedDateTime statusDateTime;

    @NonNull
    @Pattern(regexp = "^\\d{1,15}\\.\\d{2}$")
    BigDecimal loanAmount;

    @NonNull
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSZ")
    ZonedDateTime loanRequestTimestamp;

    @NonNull
    Integer partnerId;

    List<String> refusalReasons;
}
