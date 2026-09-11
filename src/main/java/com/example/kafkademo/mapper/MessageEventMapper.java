package com.example.kafkademo.mapper;

import com.example.kafkademo.entity.MessageEventEntity;
import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.model.BkiType;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.List;

/**
 * Чистый field-to-field маппинг одной пары (message, bkiType) + готовый payload в одну строку
 * message_event. Один AntifraudEventMessage даёт несколько таких пар — по одной на каждый
 * BkiType, применимый к партнёру сообщения; сам подбор BkiType по партнёру и сериализация
 * payload (нужен ObjectMapper) — это уже не маппинг, а бизнес-логика/IO, поэтому вынесены в
 * MessageEventService, а не сюда. Причина, помимо разделения ответственности: MapStruct не умеет
 * constructor injection для полей, объявленных вручную в abstract class-маппере (только для
 * своих же uses=-зависимостей) — с интерфейсом эта проблема просто не возникает, ObjectMapper
 * инжектится в MessageEventService обычным конструктором.
 */
@Mapper(componentModel = "spring")
public interface MessageEventMapper {

    @Mapping(target = "id", ignore = true)
    @Mapping(target = "receivedAt", ignore = true)
    @Mapping(source = "message.applicationStatus.statusName", target = "applicationStatus")
    @Mapping(source = "message.borrower.birthday", target = "borrowerBirthday")
    @Mapping(source = "message.borrower.inn", target = "borrowerInn")
    @Mapping(source = "message.borrower.hasSpecialTaxRegime", target = "borrowerHasSpecialTaxRegime")
    @Mapping(source = "bkiType.name", target = "bkiType")
    // "payload" — простой String-параметр с тем же именем, что и целевое поле, MapStruct
    // присваивает его напрямую, без промежуточного маппинга.
    MessageEventEntity toEntity(AntifraudEventMessage message, BkiType bkiType, String payload);

    // Список -> строка через запятую: MapStruct сам находит и применяет этот default-метод для
    // поля refusalReasons (совпадает по имени с message.refusalReasons, типы не совпадают —
    // List<String> -> String). Имя метода для MapStruct роли не играет, выбор идёт по сигнатуре.
    default String joinRefusalReasons(List<String> refusalReasons) {
        return refusalReasons == null ? null : String.join(",", refusalReasons);
    }
}
