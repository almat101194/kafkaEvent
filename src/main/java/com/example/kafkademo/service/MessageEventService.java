package com.example.kafkademo.service;

import com.example.kafkademo.entity.MessageEventEntity;
import com.example.kafkademo.mapper.MessageEventMapper;
import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.model.BkiType;
import com.example.kafkademo.model.Partner;
import com.example.kafkademo.repository.MessageEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Персистит батч antifraud-событий в message_event.
 *
 * {@link #persist(List)} должен вызываться ИЗВНЕ (из другого бина, как это делает
 * AntifraudEventListener) — self-invocation внутри одного и того же бина не проходит через
 * Spring AOP прокси, и @Transactional в этом случае просто не сработает.
 *
 * Транзакция — атомарная граница на весь батч: либо весь батч (за вычетом уже сохранённых ранее
 * дублей) коммитится целиком, либо, если что-то пошло не так, откатывается полностью — partial
 * commit невозможен. Слушатель коммитит offset в Kafka только ПОСЛЕ успешного возврата из этого
 * метода; если транзакция не закоммитилась (метод выбросил исключение), offset не двигается, и
 * Kafka передоставит этот же батч повторно — идемпотентность по паре (uid, bkiType)
 * (filterOutDuplicates) на повторной попытке сама разберётся, что уже успело закоммититься раньше
 * (в т.ч. параллельно, другим инстансом), а что нет.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageEventService {

    private final MessageEventRepository messageEventRepository;
    private final MessageEventMapper messageEventMapper;
    private final ObjectMapper objectMapper;

    @Transactional
    public void persist(List<AntifraudEventMessage> messages) {
        List<MessageEventEntity> toSave = filterOutDuplicates(messages);
        if (!toSave.isEmpty()) {
            messageEventRepository.saveAll(toSave);
        }
    }

    /**
     * Идемпотентность при повторной доставке батча: одним запросом узнаём, какие пары
     * (uid, bkiType) уже сохранены в БД, плюс отсеиваем дубли внутри самого батча. Один
     * AntifraudEventMessage маппится в несколько MessageEventEntity — по одной строке на каждый
     * BkiType, применимый к партнёру сообщения (см. toEntities ниже), поэтому дедуп идёт по паре
     * (uid, bkiType), а не по одному uid: несколько строк с одинаковым uid, но разным bkiType —
     * это не дубли, а ожидаемый фан-аут одного события.
     */
    private List<MessageEventEntity> filterOutDuplicates(List<AntifraudEventMessage> messages) {
        Set<String> uidsInBatch = messages.stream()
                .map(AntifraudEventMessage::getUid)
                .collect(Collectors.toSet());
        Set<String> alreadySaved = messageEventRepository.findExistingUidBkiTypePairs(uidsInBatch).stream()
                .map(pair -> dedupKey((String) pair[0], (String) pair[1]))
                .collect(Collectors.toSet());

        Set<String> seenInThisBatch = new HashSet<>();
        List<MessageEventEntity> toSave = new ArrayList<>();
        for (AntifraudEventMessage message : messages) {
            log.info("Antifraud event: uid={}, externalId={}, applicationStatus={}, statusDateTime={}, "
                            + "loanAmount={}, loanRequestTimestamp={}, partnerId={}, refusalReasons={}, "
                            + "borrower.birthday={}, borrower.inn={}, borrower.hasSpecialTaxRegime={}",
                    message.getUid(),
                    message.getExternalId(),
                    message.getApplicationStatus(),
                    message.getStatusDateTime(),
                    message.getLoanAmount(),
                    message.getLoanRequestTimestamp(),
                    message.getPartnerId(),
                    message.getRefusalReasons(),
                    message.getBorrower().getBirthday(),
                    message.getBorrower().getInn(),
                    message.getBorrower().getHasSpecialTaxRegime());

            for (MessageEventEntity entity : toEntities(message)) {
                String key = dedupKey(entity.getUid(), entity.getBkiType());
                if (alreadySaved.contains(key) || !seenInThisBatch.add(key)) {
                    log.info("Antifraud event uid={}, bkiType={} уже сохранён — пропускаем дубль "
                            + "(redelivery/дубль в батче)", entity.getUid(), entity.getBkiType());
                    continue;
                }
                toSave.add(entity);
            }
        }
        return toSave;
    }

    /**
     * Один AntifraudEventMessage даёт несколько строк message_event — по одной на каждый
     * BkiType, применимый к партнёру сообщения (partnerId -> Partner -> BkiType.getByPartner).
     * MessageEventMapper маппит только одну пару (message, bkiType, payload) за раз — подбор
     * применимых BkiType и сериализация payload сюда не входят, это бизнес-логика/IO, не маппинг.
     */
    private List<MessageEventEntity> toEntities(AntifraudEventMessage message) {
        Partner partner = Partner.getById(message.getPartnerId());
        List<BkiType> bkiTypes = BkiType.getByPartner(partner);
        String payload = toJson(message);

        return bkiTypes.stream()
                .map(bkiType -> messageEventMapper.toEntity(message, bkiType, payload))
                .toList();
    }

    private String toJson(AntifraudEventMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Не удалось сериализовать AntifraudEventMessage в JSON", e);
        }
    }

    private static String dedupKey(String uid, String bkiType) {
        return uid + "::" + bkiType;
    }
}
