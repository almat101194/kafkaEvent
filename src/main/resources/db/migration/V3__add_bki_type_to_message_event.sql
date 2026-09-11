-- Тип БКИ (бюро кредитных историй), от которого получен antifraud-скоринг (BkiType.getName()).
-- NOT NULL DEFAULT на существующих строках нет исторических данных — проставляем плейсхолдер
-- 'scoringBureau' задним числом, чтобы не ломать constraint, дальше колонка обязательна для новых строк.
ALTER TABLE message_event
    ADD COLUMN bki_type VARCHAR(64) NOT NULL DEFAULT 'scoringBureau';

ALTER TABLE message_event
    ALTER COLUMN bki_type DROP DEFAULT;
