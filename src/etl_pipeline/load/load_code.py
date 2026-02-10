from datetime import datetime
import pandas as pd
from pathlib import Path


class Load:
    def __init__(self, processed_data):
        self.df = processed_data.data

        self.output_dir = Path("data") / "output"
        self.versions_dir = self.output_dir / "versions"

        self.load()

    def load(self):
        """
        Persist dataframe with versioning (Excel).
        """
        self._ensure_directories()
        self._save_version()
        self._save_latest()

    def _ensure_directories(self):
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.versions_dir.mkdir(parents=True, exist_ok=True)

    def _save_version(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        version_file = self.versions_dir / f"v_{timestamp}.xlsx"

        self.df.to_excel(
            version_file,
            index=False,
            engine="openpyxl"
        )

    def _save_latest(self):
        latest_file = self.output_dir / "latest.xlsx"

        self.df.to_excel(
            latest_file,
            index=False,
            engine="openpyxl"
        )



