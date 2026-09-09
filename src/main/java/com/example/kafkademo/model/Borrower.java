package com.example.kafkademo.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import java.time.LocalDate;

@Value
@Builder(toBuilder = true)
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
public class Borrower {

    @NonNull
    @JsonFormat(pattern = "yyyy-MM-dd")
    LocalDate birthday;

    @NonNull
    String inn;

    Boolean hasSpecialTaxRegime;
}
