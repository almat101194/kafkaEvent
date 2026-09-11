package com.example.kafkademo.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@Getter
@RequiredArgsConstructor
public enum Partner {

    MFI(2L, "mfi"),

    COOP(4L, "coop");

    private final long id;

    private final String name;

    public static Partner getById(long id) {
        return Arrays.stream(values())
                .filter(partner -> partner.id == id)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown Partner id: " + id));
    }
}
