CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS transformed;

CREATE SCHEMA IF NOT EXISTS data_quality;

CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE raw.credit_fraud (
    -- Nome/ID da pipeline que processou/carregou os dados, ex: 'dag_credit_fraud'
    source_pipeline VARCHAR(150) NOT NULL,
    -- ID único da execução no ambiente de orquestração da pipeline
    execution_id VARCHAR(255) NOT NULL,
    -- Timestamp de execução da pipeline
    execution_timestamp TIMESTAMP NOT NULL,
    -- Nome do arquivo de origem
    source_file_name VARCHAR(150),
    -- Número da linha no arquivo de origem que resultou nesse registro
    source_row_number BIGINT,
    -- Timestamp de quando o registro foi inserido
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- Hash das colunas do registro
    source_hash CHAR(64) NOT NULL,
    ---------------------------------------------------
    transaction_timestamp BIGINT,
    sending_address TEXT,
    receiving_address TEXT,
    amount TEXT,
    transaction_type TEXT,
    location_region TEXT,
    ip_prefix TEXT,
    login_frequency INTEGER,
    session_duration INTEGER,
    purchase_pattern TEXT,
    age_group TEXT,
    risk_score TEXT,
    anomaly TEXT
);

CREATE INDEX idx_raw_credit_fraud_execution_id ON raw.credit_fraud (execution_id);

CREATE INDEX idx_raw_credit_fraud_source_hash ON raw.credit_fraud (source_hash);

CREATE INDEX idx_raw_credit_fraud_source_file_row ON raw.credit_fraud (
    source_file_name,
    source_row_number
);

CREATE TABLE transformed.credit_fraud (
    -- ID único da execução no ambiente de orquestração da pipeline
    execution_id VARCHAR(255) NOT NULL,
    -- Hash calculado com dados não tratados da RAW
    source_hash CHAR(64) NOT NULL,
    -- Hash calculado com dados já tratados da TRANSFORMED
    record_hash CHAR(64) NOT NULL PRIMARY KEY,
    -- Timestamp de quando o registro foi transformado
    transformed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ---------------------------------------------------
    transaction_datetime TIMESTAMP,
    sending_address VARCHAR(255),
    receiving_address VARCHAR(255),
    amount NUMERIC(18, 2),
    transaction_type VARCHAR(150),
    location_region VARCHAR(150),
    ip_prefix VARCHAR(30),
    login_frequency INTEGER,
    session_duration INTEGER,
    purchase_pattern VARCHAR(150),
    age_group VARCHAR(150),
    risk_score DOUBLE PRECISION,
    anomaly VARCHAR(150)
);

CREATE INDEX idx_transformed_credit_fraud_source_hash ON transformed.credit_fraud (source_hash);

CREATE INDEX idx_transformed_credit_fraud_execution_id ON transformed.credit_fraud (execution_id);

CREATE INDEX idx_transformed_credit_fraud_transaction_datetime ON transformed.credit_fraud (transaction_datetime);