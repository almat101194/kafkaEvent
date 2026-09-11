-- Теперь одному antifraud-event (uid) может соответствовать несколько строк message_event —
-- по одной на каждый BkiType, применимый к партнёру события (см. BkiType.getByPartner,
-- MessageEventMapper.toEntities). UNIQUE(uid) из V2 этого не переживёт: вставка второй строки
-- с тем же uid (для другого bki_type) будет падать на constraint violation.
-- Заменяем уникальность на пару (uid, bki_type) — идемпотентность теперь на уровне (событие, БКИ).
ALTER TABLE message_event
    DROP CONSTRAINT uq_message_event_uid;

ALTER TABLE message_event
    ADD CONSTRAINT uq_message_event_uid_bki_type UNIQUE (uid, bki_type);
