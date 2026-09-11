-- uid уникально идентифицирует сообщение (само событие), в отличие от external_id
-- (бизнес-заявка), у которой может быть несколько событий со временем.
-- Ограничение защищает от дублей при повторной доставке одного и того же сообщения
-- Kafka'ой (at-least-once: сервис мог упасть между commit'ом в БД и commit'ом offset'а).
ALTER TABLE message_event
    ADD CONSTRAINT uq_message_event_uid UNIQUE (uid);
