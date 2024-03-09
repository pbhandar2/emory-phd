from pathlib import Path 
from pandas import read_csv, DataFrame


class CSVLoader:
    def __init__(self, csv_path: Path) -> None:
        self._path = csv_path
        self._df = read_csv(self._path)
    

    def get_sample_error_df(self) -> DataFrame:
        pass 