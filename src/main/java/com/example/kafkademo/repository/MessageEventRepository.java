package com.example.kafkademo.repository;

import com.example.kafkademo.entity.MessageEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Collection;
import java.util.List;

@Repository
public interface MessageEventRepository extends JpaRepository<MessageEventEntity, Long> {

    /**
     * Для идемпотентности при повторной доставке одного и того же сообщения Kafka'ой: прежде
     * чем сохранять, listener проверяет, не сохранена ли уже пара (uid, bki_type). Пара, а не
     * один uid — потому что одному uid (сообщению) теперь соответствует несколько строк, по
     * одной на каждый BkiType партнёра (см. MessageEventMapper.toEntities), и это не дубли.
     * Каждый элемент результата — Object[]{uid, bkiType}.
     */
    @Query("select m.uid, m.bkiType from MessageEventEntity m where m.uid in :uids")
    List<Object[]> findExistingUidBkiTypePairs(@Param("uids") Collection<String> uids);
}
