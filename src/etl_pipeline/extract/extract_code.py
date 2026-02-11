

import pandas as pd
import os
from pathlib import Path


class Extract:
    def __init__(self):
        self.rentals = None
        self.segments = None

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
        self.rentals = self.read_file("raw_data.csv")
        self.segments = self.read_file("segments.csv")

        # Column standardization (still extract)
        '''
        self.rentals = self.rentals.rename(columns={
            "MARKET PLACE": "MARKET_PLACE",
            "Customer Site ID": "Customer_Site_ID",
            "Customer Name": "Customer_Name",
            "Product Code": "Product_code",
            "Product Serial Number": "Product_Serial_Number",
            "Equipment Rental Payment/Month": "Equipment_Rental_Payment_Month",
        })
        

        self.segments = self.segments.rename(columns={
            "Product code": "Product_code"
        })
        '''
        self.rentals = self.rename_columns(self.rentals)

        self.segments = self.rename_columns(self.segments)

        return self.rentals, self.segments


    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Rename columns by replacing spaces with underscores
        df.columns = df.columns.str.replace(' ', '_', regex=False)
        df.columns = df.columns.str.replace('/', '_', regex=False)
        return df


'''

import pandas as pd
from pathlib import Path

class Extract:
    def __init__(self, path: str):
            # self.data = self._extract()
            self.path = path

    def _read_file(self) -> pd.DataFrame:
            """
            Generic file reader (internal to Extract).
            """
            path = Path(self.path)
            ext = path.suffix.lower()

            if ext == ".csv":
                return pd.read_csv(path)
            elif ext == ".parquet":
                return pd.read_parquet(path)
            elif ext == ".json":
                return pd.read_json(path)
            elif ext in (".xls", ".xlsx"):
                return pd.read_excel(path)
            else:
                raise ValueError(f"Unsupported file type: {ext}")

    def _extract(self):
            """
            Extract multiple sources.
            """
            df = self._read_file(self.path)

            return {"dataframe": df}

'''

