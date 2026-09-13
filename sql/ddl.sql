CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS transformed;

CREATE SCHEMA IF NOT EXISTS audit;

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
    source_row_number BIGINT,
    -- Hash calculado com dados já tratados da TRANSFORMED
    record_hash CHAR(64) NOT NULL PRIMARY KEY,
    -- Timestamp de quando o registro foi transformado
    transformed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ---------------------------------------------------
    transaction_timestamp TIMESTAMP,
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

CREATE INDEX idx_transformed_credit_fraud_transaction_timestamp ON transformed.credit_fraud (transaction_timestamp);

CREATE TABLE IF NOT EXISTS audit.data_quality_error (
    id BIGSERIAL PRIMARY KEY,
    execution_id VARCHAR(100) NOT NULL,
    source_hash CHAR(64) NOT NULL,
    source_row_number BIGINT,
    source_table VARCHAR(150) NOT NULL,
    target_table VARCHAR(150) NOT NULL,
    column_name VARCHAR(150),
    error_type VARCHAR(100) NOT NULL,
    error_message TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_audit_data_quality_error_execution_id ON audit.data_quality_error (execution_id);

CREATE INDEX idx_audit_data_quality_error_target_table ON audit.data_quality_error (target_table);

CREATE TABLE analytics.risk_score_per_location (
    location_region VARCHAR(150) PRIMARY KEY,
    avg_risk_score NUMERIC(10, 6) NOT NULL,
    total_records BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE analytics.top_receiving_address (
    receiving_address VARCHAR(255) PRIMARY KEY,
    amount NUMERIC(18, 6) NOT NULL,
    transaction_timestamp TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit.data_quality_metric (
    id BIGSERIAL PRIMARY KEY,
    execution_id VARCHAR(255) NOT NULL,
    target_table VARCHAR(255) NOT NULL,
    total_records INTEGER NOT NULL,
    valid_records INTEGER NOT NULL,
    invalid_records INTEGER NOT NULL,
    total_violations INTEGER NOT NULL,
    conformity_percentage NUMERIC(5, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (execution_id, target_table)
);

CREATE INDEX idx_audit_data_quality_metric_execution_id ON audit.data_quality_metric (execution_id);