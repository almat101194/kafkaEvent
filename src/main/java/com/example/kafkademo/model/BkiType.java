package com.example.kafkademo.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Arrays;
import java.util.List;

@Getter
@RequiredArgsConstructor
public enum BkiType {

    SCORING_BUREAU(1, "scoringBureau", Partner.MFI),

    NBKI(2, "nbki", Partner.MFI),

    COOP_NBKI(3, "coopNbki", Partner.COOP);

    private final Integer id;

    private final String name;

    /** Партнёр, к которому привязан этот тип БКИ. */
    private final Partner partner;

    public static BkiType getById(Integer id) {
        return Arrays.stream(values())
                .filter(type -> type.id.equals(id))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown BkiType id: " + id));
    }

    /**
     * Все типы БКИ, применимые к партнёру — используется, чтобы по входящему partnerId
     * сообщения определить, сколько строк message_event и с каким bki_type для него создать
     * (см. MessageEventMapper.toEntities). Например, Partner.MFI сейчас даёт 2 типа, Partner.COOP — 1.
     */
    public static List<BkiType> getByPartner(Partner partner) {
        List<BkiType> types = Arrays.stream(values())
                .filter(type -> type.partner == partner)
                .toList();
        if (types.isEmpty()) {
            throw new IllegalArgumentException("No BkiType configured for partner: " + partner);
        }
        return types;
    }

    @JsonCreator
    public static BkiType getByName(String name) {
        return Arrays.stream(values())
                .filter(type -> type.getName().equalsIgnoreCase(name))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Unknown BkiType name: " + name));
    }

    @JsonValue
    public String getName() {
        return name;
    }
}
