import pandas as pd
from pathlib import Path


class Extract:
    def __init__(self, *filenames: str):
        #self.df1 = None
        #self.df2 =
        """
        Constructor accepts a variable number of filenames.
        """
        self.dataframes = {}  # Store all dataframes in a dictionary
        for filename in filenames:
            # Read each file and store in the dictionary
            self.dataframes[filename] = self.read_files(filename)

    def read_files(self, filename: str, **kwargs) -> pd.DataFrame:
        """
        Generic file reader for the Extract layer.
        Supports CSV, Excel and Parquet.
        """

        base_path = Path("data") / "raw"
        file_path = base_path / filename

        extension = file_path.suffix.lower()

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
        Extracts the data and returns raw DataFrames.
        """
        return self.dataframes  # Return the raw dataframes as they are


