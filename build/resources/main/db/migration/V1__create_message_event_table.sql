CREATE TABLE message_event (
    id                               BIGSERIAL PRIMARY KEY,
    uid                              VARCHAR(255)  NOT NULL,
    external_id                      VARCHAR(255)  NOT NULL,
    application_status               VARCHAR(64)   NOT NULL,
    status_date_time                 TIMESTAMPTZ   NOT NULL,
    loan_amount                      NUMERIC(17,2) NOT NULL,
    loan_request_timestamp           TIMESTAMPTZ   NOT NULL,
    partner_id                       INTEGER       NOT NULL,
    refusal_reasons                  TEXT,
    borrower_birthday                DATE          NOT NULL,
    borrower_inn                     VARCHAR(32)   NOT NULL,
    borrower_has_special_tax_regime  BOOLEAN,
    payload                          JSONB         NOT NULL,
    received_at                      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX idx_message_event_uid ON message_event (uid);
CREATE INDEX idx_message_event_external_id ON message_event (external_id);
