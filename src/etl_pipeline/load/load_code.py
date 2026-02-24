from datetime import datetime
from pathlib import Path


class Load:
    def __init__(self, processed_data):
        """
        Load layer.
        Responsible for persisting transformed dataset with version control.
        """
        # Transform class exposes final dataframe via .data
        self.df = processed_data.data

        # Define output directory structure
        self.output_dir = Path("data") / "output"
        self.versions_dir = self.output_dir / "versions"

        # Automatically execute persistence workflow upon instantiation
        self.load()

    def load(self):
        """
        Persist dataframe with versioning (Excel).
        """
        # Ensure directory structure exists
        self._ensure_directories()
        # Save immutable timestamped version
        self._save_version()
        # Update mutable 'latest' snapshot - Saves a timestamped Excel file.
        self._save_latest()

    def _ensure_directories(self):
        # Create main output directory if missing
        self.output_dir.mkdir(parents=True, exist_ok=True)
        # Create versioning subdirectory for historical files
        self.versions_dir.mkdir(parents=True, exist_ok=True)

    def _save_version(self):
        # Generate unique timestamp to avoid filename collision
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # Version file path follows append-only storage strategy
        version_file = self.versions_dir / f"v_{timestamp}.xlsx"

        # Put the dataframe in Excel without index (clean analytical output)
        self.df.to_excel(
            version_file,
            index=False,
            engine="openpyxl"  # Explicit engine for Excel compatibility
        )

    def _save_latest(self):
        latest_file = self.output_dir / "latest.xlsx"

        self.df.to_excel(
            latest_file,
            index=False,
            engine="openpyxl"
        )



