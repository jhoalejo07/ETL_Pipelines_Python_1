import pandas as pd
from pathlib import Path


class Extract:
    def __init__(self, *filenames: str):
        """
        Initializes Extract layer.
        Accepts multiple filenames and loads them dynamically into a dictionary.
        """
        self.dataframes = {}

        # Dynamically read all input files
        for filename in filenames:
            self.dataframes[filename] = self.read_files(filename)

    def read_files(self, filename: str, **kwargs) -> pd.DataFrame:
        """
        Generic file reader.
        Automatically detects file extension and selects appropriate pandas loader.
        """

        base_path = Path("data") / "raw"
        file_path = base_path / filename
        extension = file_path.suffix.lower()

        # Dynamic dispatch based on extension
        if extension == ".csv":
            return pd.read_csv(file_path, **kwargs)

        elif extension in [".xls", ".xlsx"]:
            return pd.read_excel(file_path, **kwargs)

        elif extension == ".parquet":
            return pd.read_parquet(file_path, **kwargs)

        else:
            raise ValueError(f"Unsupported file type: {extension}")

    def extract(self):
        """
        Returns raw DataFrames as a dictionary.
        """
        return self.dataframes


