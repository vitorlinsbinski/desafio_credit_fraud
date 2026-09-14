import hashlib
from pathlib import Path
from datetime import datetime
import pandas as pd
import logging
import math

logger = logging.getLogger(__name__)

from src.etl import CSVExtractor, DatabaseHandler, clean_credit_fraud_df, validate_risk_score_per_location, validate_top_receiving_address

CHUNK_SIZE = 100_000

class CreditFraudPipeline:
    def __init__(
        self, 
        execution_id: str,
        execution_timestamp: datetime,
        source_pipeline: str,
        database: DatabaseHandler
    ):  
        self.execution_id = execution_id
        self.execution_timestamp = execution_timestamp
        self.source_pipeline = source_pipeline
        self.database = database
    
    def extract(
        self,
        csv_file_path: str,
        csv_separator: str = ",",
    ):
        csv_extractor = CSVExtractor(
            file_path=csv_file_path,
            separator=csv_separator,
            chunk_size=CHUNK_SIZE
        )

        source_file_name = Path(csv_file_path).name
        row_offset = 0
        
        insert_query = """
            INSERT INTO raw.credit_fraud (
                transaction_timestamp,
                sending_address,
                receiving_address,
                amount,
                transaction_type,
                location_region,
                ip_prefix,
                login_frequency,
                session_duration,
                purchase_pattern,
                age_group,
                risk_score,
                anomaly,
                source_row_number,
                source_hash,
                source_pipeline,
                execution_id,
                execution_timestamp,
                source_file_name
            )
            VALUES (
                :transaction_timestamp,
                :sending_address,
                :receiving_address,
                :amount,
                :transaction_type,
                :location_region,
                :ip_prefix,
                :login_frequency,
                :session_duration,
                :purchase_pattern,
                :age_group,
                :risk_score,
                :anomaly,
                :source_row_number,
                :source_hash,
                :source_pipeline,
                :execution_id,
                :execution_timestamp,
                :source_file_name
            )
        """

        logger.info("Extraindo dados do arquivo CSV...")
        total = 0
        for i, chunk in enumerate(csv_extractor.read_chunks()):
            if i == 0:
                logger.info(f"Schema do dataset de entrada: ")
                chunk.info()
            
            total += len(chunk)
            logger.info(
                "Lendo chunk %s com %s registros...",
                (i + 1),
                len(chunk)
            )
                
            chunk.rename(
                columns={
                    "timestamp": "transaction_timestamp"
                },
                inplace=True
            )
            
            chunk["source_row_number"] = (
                range(row_offset + 1, row_offset + len(chunk) + 1)
            )
            
            row_offset += len(chunk)
            
            business_columns = [
                "transaction_timestamp",
                "sending_address",
                "receiving_address",
                "amount",
                "transaction_type",
                "location_region",
                "ip_prefix",
                "login_frequency",
                "session_duration",
                "purchase_pattern",
                "age_group",
                "risk_score",
                "anomaly",
            ]
            chunk["source_hash"] = chunk.apply(
                self._generate_source_hash,
                axis=1,
                args=(business_columns,)
            )
            
            chunk["source_pipeline"] = self.source_pipeline
            chunk["execution_id"] = self.execution_id
            chunk["execution_timestamp"] = self.execution_timestamp
            chunk["source_file_name"] = source_file_name
            
            records = chunk.to_dict(orient="records")
            
            logger.info(
                "Inserindo chunk %s na camada RAW...",
                (i + 1)
            )
            self.database.execute_many(
                query=insert_query,
                params=records
            )
            
            logger.info(
                "Chunk %s inserido na raw: %s registros",
                (i + 1),
                len(chunk)
            )
            
        logger.info(
            "Total de %s registros inseridos na camada RAW",
            total
        )    
            
    def transform(
        self
    ):
        logger.info(
            "Processando dados do id de execução '%s'",
            self.execution_id
        )
        
        logger.info("Iniciando limpeza de dados...")
        
        query_count = """
            SELECT 
                COUNT(*) as total
            FROM raw.credit_fraud
            WHERE 
                execution_id = :execution_id
        """
        
        result = self.database.fetch_one(
            query=query_count,
            params={
                "execution_id": self.execution_id
            }
        )
        
        total = result["total"]
        
        if total == 0:
            logger.warning("Nenhum registro para processar")
            
            return
        
        query_raw = """
            SELECT
                *
            FROM raw.credit_fraud
            WHERE
                execution_id = :execution_id
        """
        
        logger.info(
            "Processando %s registros...",
            total
        )
        df_raw_chunks = self.database.fetch_dataframe_chunks(
            query=query_raw,
            params={
                "execution_id": self.execution_id
            },
            chunk_size=50000
        )

        upsert_query = """
            INSERT INTO transformed.credit_fraud (
                execution_id,
                source_row_number,
                source_hash,
                record_hash,
                transformed_at,
                transaction_timestamp,
                sending_address,
                receiving_address,
                amount,
                transaction_type,
                location_region,
                ip_prefix,
                login_frequency,
                session_duration,
                purchase_pattern,
                age_group,
                risk_score,
                anomaly
            )
            VALUES (
                :execution_id,
                :source_row_number,
                :source_hash,
                :record_hash,
                :transformed_at,
                :transaction_timestamp,
                :sending_address,
                :receiving_address,
                :amount,
                :transaction_type,
                :location_region,
                :ip_prefix,
                :login_frequency,
                :session_duration,
                :purchase_pattern,
                :age_group,
                :risk_score,
                :anomaly
            )
            ON CONFLICT (record_hash) DO UPDATE SET
                execution_id = EXCLUDED.execution_id,
                source_row_number = EXCLUDED.source_row_number,
                source_hash = EXCLUDED.source_hash,
                transformed_at = EXCLUDED.transformed_at,
                transaction_timestamp = EXCLUDED.transaction_timestamp,
                sending_address = EXCLUDED.sending_address,
                receiving_address = EXCLUDED.receiving_address,
                amount = EXCLUDED.amount,
                transaction_type = EXCLUDED.transaction_type,
                location_region = EXCLUDED.location_region,
                ip_prefix = EXCLUDED.ip_prefix,
                login_frequency = EXCLUDED.login_frequency,
                session_duration = EXCLUDED.session_duration,
                purchase_pattern = EXCLUDED.purchase_pattern,
                age_group = EXCLUDED.age_group,
                risk_score = EXCLUDED.risk_score,
                anomaly = EXCLUDED.anomaly
        """
        
        for i, chunk in enumerate(df_raw_chunks):
            logger.info(
                "Processando chunk %s de %s registros...",
                (i + 1),
                len(chunk)
            )
            cleaned_chunk = clean_credit_fraud_df(chunk)
            
            business_columns = [
                "transaction_timestamp",
                "sending_address",
                "receiving_address",
                "amount",
                "transaction_type",
                "location_region",
                "ip_prefix",
                "login_frequency",
                "session_duration",
                "purchase_pattern",
                "age_group",
                "risk_score",
                "anomaly",
            ]
            cleaned_chunk["record_hash"] = cleaned_chunk.apply(
                self._generate_source_hash,
                axis=1,
                args=(business_columns,)
            )
            
            cleaned_chunk["transformed_at"] = datetime.now()

            records = (
                cleaned_chunk
                .astype(object)
                .where(pd.notna(cleaned_chunk), None)
                .to_dict(orient="records")
            )

            self.database.execute_many(
                query=upsert_query,
                params=records
            )

            logger.info(
                "Chunk %s carregado na transformed: %s registros",
                (i + 1),
                len(cleaned_chunk)
            )
        
    def load_risk_score_per_location(self):
        TARGET_TABLE = "risk_score_per_location"

        logger.info(
            "Iniciando carregamento do agregado %s para execution_id=%s",
            TARGET_TABLE,
            self.execution_id
        )

        query = """
            SELECT
                *
            FROM transformed.credit_fraud
            WHERE execution_id = :execution_id
        """

        df_transformed_chunks = self.database.fetch_dataframe_chunks(
            query=query,
            params={
                "execution_id": self.execution_id
            },
            chunk_size=50000
        )

        self._validate_and_record_quality(
            target_table=TARGET_TABLE,
            df_chunks=df_transformed_chunks,
            validator=validate_risk_score_per_location
        )

        self.database.execute(
            query=f"""
                TRUNCATE TABLE analytics.{TARGET_TABLE}
            """
        )

        self.database.execute(
            query="""
                INSERT INTO analytics.risk_score_per_location (
                    location_region,
                    avg_risk_score,
                    total_records,
                    updated_at
                )
                SELECT
                    location_region,
                    AVG(risk_score),
                    COUNT(*),
                    CURRENT_TIMESTAMP
                FROM transformed.credit_fraud
                WHERE
                    location_region IS NOT NULL
                    AND location_region IN (
                        'africa',
                        'asia',
                        'europe',
                        'north america',
                        'south america',
                        'oceania'
                    )
                    AND risk_score IS NOT NULL
                    AND risk_score BETWEEN 0 AND 100
                GROUP BY location_region
                ORDER BY AVG(risk_score) DESC
            """
        )

        logger.info(
            "Agregado %s carregado com sucesso.",
            TARGET_TABLE
        )
    
    def load_top_receiving_address(self):
        TARGET_TABLE = "top_receiving_address"

        logger.info(
            "Iniciando carregamento do agregado %s para execution_id=%s",
            TARGET_TABLE,
            self.execution_id
        )

        query = """
            SELECT
                *
            FROM transformed.credit_fraud
            WHERE execution_id = :execution_id
        """

        df_transformed_chunks = self.database.fetch_dataframe_chunks(
            query=query,
            params={
                "execution_id": self.execution_id
            },
            chunk_size=50000
        )

        self._validate_and_record_quality(
            target_table=TARGET_TABLE,
            df_chunks=df_transformed_chunks,
            validator=validate_top_receiving_address
        )

        self.database.execute(
            query=f"""
                TRUNCATE TABLE analytics.{TARGET_TABLE}
            """
        )

        self.database.execute(
            query="""
                INSERT INTO analytics.top_receiving_address (
                    receiving_address,
                    amount,
                    transaction_timestamp
                )
                WITH valid_transactions AS (
                    SELECT
                        *
                    FROM transformed.credit_fraud
                    WHERE
                        transaction_type = 'sale'
                        AND receiving_address IS NOT NULL
                        AND amount IS NOT NULL
                        AND transaction_timestamp IS NOT NULL
                ),
                transactions_ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY receiving_address
                            ORDER BY transaction_timestamp DESC
                        ) AS rn
                    FROM valid_transactions
                )
                SELECT
                    receiving_address,
                    amount,
                    transaction_timestamp
                FROM transactions_ranked
                WHERE rn = 1
                ORDER BY amount DESC
                LIMIT 3
            """
        )

        logger.info(
            "Agregado %s carregado com sucesso.",
            TARGET_TABLE
        )
    
    def report_data_quality(self):
        metrics_query = """
            SELECT
                target_table,
                total_records,
                valid_records,
                invalid_records,
                total_violations,
                conformity_percentage
            FROM audit.data_quality_metric
            WHERE execution_id = :execution_id
        """

        error_query = """
            SELECT
                target_table,
                error_type,
                column_name,
                COUNT(*) AS total_errors
            FROM audit.data_quality_error
            WHERE execution_id = :execution_id
            GROUP BY
                target_table,
                error_type,
                column_name
            ORDER BY total_errors DESC
        """

        metrics = self.database.fetch_all(
            metrics_query,
            params={"execution_id": self.execution_id}
        )

        errors = self.database.fetch_all(
            error_query,
            params={"execution_id": self.execution_id}
        )

        logger.info("========== DATA QUALITY REPORT ==========")

        for metric in metrics:
            error_percentage = (
                metric["invalid_records"]
                / metric["total_records"]
                * 100
                if metric["total_records"] > 0
                else 0
            )

            logger.info(
                "\n"
                "Tabela: %s\n"
                "Total de registros: %s\n"
                "Registros válidos: %s\n"
                "Registros inválidos: %s\n"
                "Total de violações: %s\n"
                "Conformidade: %.2f%%\n"
                "Taxa de erro: %.2f%%",
                metric["target_table"],
                metric["total_records"],
                metric["valid_records"],
                metric["invalid_records"],
                metric["total_violations"],
                metric["conformity_percentage"],
                error_percentage
            )

        logger.info("========== ANOMALIAS ENCONTRADAS ==========")

        for error in errors:
            logger.warning(
                "Tabela=%s | Tipo=%s | Coluna=%s | Ocorrências=%s",
                error["target_table"],
                error["error_type"],
                error["column_name"],
                error["total_errors"]
            )
        
    def _validate_and_record_quality(
        self,
        target_table: str,
        df_chunks,
        validator
    ):
        logger.info(
            "Iniciando validação de qualidade para %s...",
            target_table
        )

        self.database.execute(
            query="""
                DELETE FROM audit.data_quality_error
                WHERE execution_id = :execution_id
                AND target_table = :target_table
            """,
            params={
                "execution_id": self.execution_id,
                "target_table": target_table
            }
        )

        self.database.execute(
            query="""
                DELETE FROM audit.data_quality_metric
                WHERE execution_id = :execution_id
                AND target_table = :target_table
            """,
            params={
                "execution_id": self.execution_id,
                "target_table": target_table
            }
        )

        total_records = 0
        total_violations = 0
        invalid_rows = set()
        chunk_count = 0

        for chunk in df_chunks:
            chunk_count += 1
            total_records += len(chunk)

            df_errors = validator(chunk)

            if not df_errors.empty:
                total_violations += len(df_errors)

                invalid_rows.update(
                    df_errors["source_row_number"]
                    .dropna()
                    .tolist()
                )

                self.database.insert_dataframe(
                    df=df_errors,
                    schema="audit",
                    table="data_quality_error"
                )

            logger.info(
                "Chunk %s de %s validado. "
                "Registros=%s | Violações=%s",
                chunk_count,
                target_table,
                len(chunk),
                len(df_errors)
            )

        invalid_records = len(invalid_rows)
        valid_records = total_records - invalid_records

        conformity_percentage = (
            valid_records / total_records * 100
            if total_records > 0
            else 100.0
        )

        self.database.execute(
            query="""
                INSERT INTO audit.data_quality_metric (
                    execution_id,
                    target_table,
                    total_records,
                    valid_records,
                    invalid_records,
                    total_violations,
                    conformity_percentage
                )
                VALUES (
                    :execution_id,
                    :target_table,
                    :total_records,
                    :valid_records,
                    :invalid_records,
                    :total_violations,
                    :conformity_percentage
                )
            """,
            params={
                "execution_id": self.execution_id,
                "target_table": target_table,
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": invalid_records,
                "total_violations": total_violations,
                "conformity_percentage": conformity_percentage
            }
        )

        logger.info(
            (
                "\nQualidade de %s finalizada. "
                "Registros=%s | Válidos=%s | Inválidos=%s | "
                "Violações=%s | Conformidade=%.2f%%"
            ),
            target_table,
            total_records,
            valid_records,
            invalid_records,
            total_violations,
            conformity_percentage
        )
              
    @staticmethod
    def _generate_source_hash(row: pd.Series, cols: list[str]) -> str:
        value = "|".join(
            "" if pd.isna(row[col]) else str(row[col])
            for col in cols
        )
        
        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()
        
            
                
            


            
            
        
