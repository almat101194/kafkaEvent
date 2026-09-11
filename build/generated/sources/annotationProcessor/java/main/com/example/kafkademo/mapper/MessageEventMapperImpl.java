package com.example.kafkademo.mapper;

import com.example.kafkademo.entity.MessageEventEntity;
import com.example.kafkademo.model.AntifraudEventMessage;
import com.example.kafkademo.model.ApplicationStatus;
import com.example.kafkademo.model.BkiType;
import com.example.kafkademo.model.Borrower;
import java.time.LocalDate;
import javax.annotation.processing.Generated;
import org.springframework.stereotype.Component;

@Generated(
    value = "org.mapstruct.ap.MappingProcessor",
    date = "2026-09-10T20:36:08+0500",
    comments = "version: 1.6.3, compiler: IncrementalProcessingEnvironment from gradle-language-java-8.10.jar, environment: Java 17.0.20.1 (BellSoft)"
)
@Component
public class MessageEventMapperImpl implements MessageEventMapper {

    @Override
    public MessageEventEntity toEntity(AntifraudEventMessage message, BkiType bkiType, String payload) {
        if ( message == null && bkiType == null && payload == null ) {
            return null;
        }

        MessageEventEntity.MessageEventEntityBuilder messageEventEntity = MessageEventEntity.builder();

        if ( message != null ) {
            messageEventEntity.applicationStatus( messageApplicationStatusStatusName( message ) );
            messageEventEntity.borrowerBirthday( messageBorrowerBirthday( message ) );
            messageEventEntity.borrowerInn( messageBorrowerInn( message ) );
            messageEventEntity.borrowerHasSpecialTaxRegime( messageBorrowerHasSpecialTaxRegime( message ) );
            messageEventEntity.uid( message.getUid() );
            messageEventEntity.externalId( message.getExternalId() );
            messageEventEntity.statusDateTime( message.getStatusDateTime() );
            messageEventEntity.loanAmount( message.getLoanAmount() );
            messageEventEntity.loanRequestTimestamp( message.getLoanRequestTimestamp() );
            messageEventEntity.partnerId( message.getPartnerId() );
            messageEventEntity.refusalReasons( joinRefusalReasons( message.getRefusalReasons() ) );
        }
        if ( bkiType != null ) {
            messageEventEntity.bkiType( bkiType.getName() );
        }
        messageEventEntity.payload( payload );

        return messageEventEntity.build();
    }

    private String messageApplicationStatusStatusName(AntifraudEventMessage antifraudEventMessage) {
        ApplicationStatus applicationStatus = antifraudEventMessage.getApplicationStatus();
        if ( applicationStatus == null ) {
            return null;
        }
        return applicationStatus.getStatusName();
    }

    private LocalDate messageBorrowerBirthday(AntifraudEventMessage antifraudEventMessage) {
        Borrower borrower = antifraudEventMessage.getBorrower();
        if ( borrower == null ) {
            return null;
        }
        return borrower.getBirthday();
    }

    private String messageBorrowerInn(AntifraudEventMessage antifraudEventMessage) {
        Borrower borrower = antifraudEventMessage.getBorrower();
        if ( borrower == null ) {
            return null;
        }
        return borrower.getInn();
    }

    private Boolean messageBorrowerHasSpecialTaxRegime(AntifraudEventMessage antifraudEventMessage) {
        Borrower borrower = antifraudEventMessage.getBorrower();
        if ( borrower == null ) {
            return null;
        }
        return borrower.getHasSpecialTaxRegime();
    }
}
