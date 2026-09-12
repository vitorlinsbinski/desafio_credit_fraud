import pandas as pd
import logging
import uuid

logger = logging.getLogger(__name__)

from src.etl import CSVExtract
from src.etl import DatabaseHandler

CHUNK_SIZE = 100_000

class CreditFraudPipeline:
    def __init__(self, database: DatabaseHandler):
        self.database = database
    
    def extract(self, csv_file_path: str, csv_separator: str = ",", execution_id: str = ):
        csv_extractor = CSVExtract(file_path=csv_file_path, separator=csv_separator, chunk_size=CHUNK_SIZE)
        
        transformed_df = pd.DataFrame
        for i, chunk in enumerate(csv_extractor.read_chunks()):
            
            if i == 1:
                logging.info(f"Schema do dataset de entrada: ")
                chunk.info()
            
            
                
            


            
            
        