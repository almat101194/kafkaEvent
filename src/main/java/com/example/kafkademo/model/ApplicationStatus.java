package com.example.kafkademo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@RequiredArgsConstructor
@Getter
public enum ApplicationStatus {
    APPROVED("2.13.1", "approved"),
    REJECTED("2.13.2", "rejected"),
    UNDER_REVIEW("2.13.3", "underReview");

    private final String event;

    private final String statusName;

    @JsonCreator
    public static ApplicationStatus fromStatusName(String statusName) {
        return Arrays.stream(values())
                .filter(status -> status.statusName.equalsIgnoreCase(statusName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown applicationStatus: " + statusName));
    }

    @JsonValue
    public String getStatusName() {
        return statusName;
    }
}
