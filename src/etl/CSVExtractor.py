from collections.abc import Iterator

import pandas as pd

class CSVExtractor:
    def __init__(self, file_path: str, chunk_size: int = 100000, separator: str = ","):
        self.file_path = file_path
        self.chunk_size = chunk_size
        self.separator = separator
    
    def read_chunks(self) -> Iterator[pd.DataFrame]:
        for chunk in pd.read_csv(
            self.file_path,
            chunksize=self.chunk_size,
            sep=self.separator,
            low_memory=False
        ):
            yield chunk