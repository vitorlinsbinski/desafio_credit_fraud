import hashlib
from pathlib import Path
from datetime import datetime
import pandas as pd
import logging

logger = logging.getLogger(__name__)

from src.etl import CSVExtract
from src.etl import DatabaseHandler

CHUNK_SIZE = 100_000

class CreditFraudPipeline:
    def __init__(self, database: DatabaseHandler):
        self.database = database
    
    def extract(
        self,
        execution_id: str,
        execution_timestamp: datetime,
        source_pipeline: str,
        csv_file_path: str,
        csv_separator: str = ",",
    ):
        csv_extractor = CSVExtract(
            file_path=csv_file_path,
            separator=csv_separator,
            chunk_size=CHUNK_SIZE
        )

        source_file_name = Path(csv_file_path).name
        row_offset = 0

        for i, chunk in enumerate(csv_extractor.read_chunks()):
            if i == 1:
                logging.info(f"Schema do dataset de entrada: ")
                chunk.info()
                
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
            
            chunk["source_hash"] = chunk.apply(
                self._generate_source_hash,
                axis=1
            )
            
            chunk["source_pipeline"] = source_pipeline
            chunk["execution_id"] = execution_id
            chunk["execution_timestamp"] = execution_timestamp
            chunk["source_file_name"] = source_file_name
            
            self.database.insert_dataframe(
                df=chunk,
                schema="raw",
                table="credit_fraud"
            )
            
            logger.info(
                "Chunk %s inserido na raw: %s registros",
                i,
                len(chunk)
            )
    
    @staticmethod
    def _generate_source_hash(row: pd.Series) -> str:
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
        
        value = "|".join(
            "" if pd.isna(row[column]) else str(row[column])
            for column in business_columns
        )
        
        return hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()
        
            
                
            


            
            
        