from sqlalchemy import text
from sqlalchemy.engine import Engine
import pandas as pd

class DatabaseHandler:
    def __init__(self, engine: Engine):
        self.engine = engine
    
    def check_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                
            return True
        except Exception:
            return False
    
    def execute(self, query: str, params: dict | None = None):
        with self.engine.begin() as conn:
            conn.execute(
                text(query),
                params or {}
            )
    
    def fetch_all(self, query: str, params: dict | None = None):
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                params or {}
            )
    
    def fetch_one(self, query: str, params: dict | None = None):
        with self.engine.connect() as conn:
            result = conn.execute(
                text(query),
                params or {}
            )
        
        return result.mappings().first()
    
    def execute_many(
        self,
        query: str,
        params: list[dict]
    ) -> None:
        with self.engine.begin() as conn:
            conn.execute(
                text(query),
                params
            )
    
    def fetch_dataframe(
        self,
        query: str,
        params: dict | None = None
    ) -> pd.DataFrame:
        return pd.read_sql(
            sql=text(query),
            con=self.engine,
            params=params or {}
        )
        
    def fetch_dataframe_chunks(
        self,
        query: str,
        params: dict | None = None,
        chunk_size: int = 100000
    ):
        return pd.read_sql(
            sql=text(query),
            con=self.engine,
            params=params or {},
            chunksize=chunk_size
        )
    
    def insert_dataframe(
        self,
        df: pd.DataFrame,
        schema: str,
        table: str
    ):
        if df.empty:
            return
        
        df.to_sql(
            name=table,
            schema=schema,
            con=self.engine,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000
        )