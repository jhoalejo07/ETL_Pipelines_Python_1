import pandas as pd
from pathlib import Path


class Extract:
    def __init__(self):
        self.df1 = None
        self.df2 = None

    def read_file(self, filename: str, **kwargs) -> pd.DataFrame:
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
        Extract all required sources.
        """

        # Load files
        self.df1 = self.read_file("raw_data.csv")
        self.df2 = self.read_file("segments.csv")

        # Column standardization (still extract)
        self.df1 = self.rename_columns(self.df1)

        self.df2 = self.rename_columns(self.df2)

        return self.df1, self.df2


    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename columns by replacing spaces with underscores
        df.columns = df.columns.str.replace(' ', '_', regex=False)
        df.columns = df.columns.str.replace('/', '_', regex=False)
        return df


