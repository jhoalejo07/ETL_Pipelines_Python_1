import pandas as pd
from src.etl_pipeline.utils.SQL_with_Dataframes import SQl_df


class Transform:
    def __init__(self, raw_data: dict):
        """
        Transformation layer for Marketplace.
        Receives raw data dictionary and executes SQL-like transformations.
        """

        self.dataframes = raw_data
        self.sql_df = SQl_df()  # SQL abstraction layer
        self.data = self._transform()  # Execute transformation pipeline

    def _rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns by replacing spaces and slashes with underscores.
        """
        df.columns = df.columns.str.replace(' ', '_', regex=False)
        df.columns = df.columns.str.replace('/', '_', regex=False)
        return df

    # =====================================================
    # Transformation Logic
    # =====================================================
    def _transform(self) -> pd.DataFrame:
        """
        Hospital billing transformation pipeline.
        Implements numeric conversion, filtering, categorization and rollup.
        """

        # Normalize column names
        for filename, df in self.dataframes.items():
            self.dataframes[filename] = self._rename_columns(df)

        # Dynamic unpacking of input datasets
        # Convert the dictionary of DataFrames into a list.
        # self.dataframes is a dictionary where:- keys: identifiers - values: pandas DataFrame
        df1, df2 = list(self.dataframes.values())[:2]

        # Ensure BillAmount is numeric
        df = self.sql_df.convert_to_numeric(df1, 'BillAmount')

        # Relational join with AgeRange reference table
        df = self.sql_df.join_dataframes(df, df2, 'AgeRangeID', 'inner')

        # Business threshold filter
        df = self.sql_df.apply_filters(df, 'BillAmount', '>=', 1000)

        # Projection
        df = self.sql_df.df_select_columns(
            df,
            ['Province', 'PatientID', 'AgeRangeLabel', 'Hospital', 'BillAmount']
        )

        # CASE classification for billing ranges
        df = self.sql_df.df_case(
            df=df,
            columns_to_keep=['Province', 'AgeRangeLabel', 'PatientID', 'BillAmount'],
            value_column='BillAmount',
            ranges=[(1000, 5000), (5001, 9999)],
            labels=['1.0-5k', '2.5k-10k'],
            default_label='3.10k +',
            new_column_name='Bill_Amt_Cat'
        )

        # Pivot age groups into columns
        base = self.sql_df.df_pivot_values_to_columns(
            df=df,
            group_col_1='Province',
            group_col_2='Bill_Amt_Cat',
            value_column='AgeRangeLabel',
            values=['Child', 'Adult', 'Elderly']
        )

        # Add subtotal and grand total rows (ROLLUP equivalent)
        totals = self.sql_df.df_groupby_rollup(
            base_df=base,
            group_col_1='Province',
            group_col_2='Bill_Amt_Cat'
        )

        df = pd.concat([base, totals], ignore_index=True)

        # Order data respecting grouping hierarchy
        df = self.sql_df.df_orderby_grouping(
            df,
            group_col_1='Province',
            group_col_2='Bill_Amt_Cat'
        )

        return df
