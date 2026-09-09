package com.example.kafkademo.listener;

import com.example.kafkademo.model.AntifraudEventMessage;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AntifraudEventListener implements MessageListener<String, AntifraudEventMessage> {

    @Override
    public void onMessage(ConsumerRecord<String, AntifraudEventMessage> record) {
        String uid = record.key();
        AntifraudEventMessage message = record.value();

        log.info("Received antifraud event: uid={}, externalId={}, applicationStatus={}, statusDateTime={}, "
                        + "loanAmount={}, loanRequestTimestamp={}, partnerId={}, refusalReasons={}, "
                        + "borrower.birthday={}, borrower.inn={}, borrower.hasSpecialTaxRegime={}",
                uid,
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

        // TODO: обработка события
    }
}
