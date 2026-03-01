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


    def _transform(self) -> pd.DataFrame:
        """
        Core transformation pipeline.
        Implements SQL-inspired operations using abstraction layer.
        """

        # Normalize column names
        for filename, df in self.dataframes.items():
            self.dataframes[filename] = self.sql_df.rename_columns(df)

        # Dynamic unpacking of input datasets
        # Convert the dictionary of DataFrames into a list.
        # self.dataframes is a dictionary where:- keys: identifiers - values: pandas DataFrame
        dfs = list(self.dataframes.values())
        if len(dfs) < 2:
            # Ensure that at least two DataFrames are available.
            raise ValueError("At least two dataframes are required.")

        # Extract the first two DataFrames from the list.
        df1, df2 = dfs[:2]

        # =====================================================
        # Business rule Logic SQL_with_Dataframes
        # =====================================================

        # Convert text-based numeric column into real numeric type
        df = self.sql_df.convert_to_numeric(df1, 'Equipment_Rental_Payment_Month')

        # Perform relational inner join
        df = self.sql_df.join_dataframes(df, df2, 'Product_Code', 'inner')

        # Apply business filter condition
        df = self.sql_df.apply_filters(df, 'Equipment_Rental_Payment_Month', '>=', 25)

        # Project required columns (SELECT equivalent)
        df = self.sql_df.df_select_columns(
            df,
            ['MARKET_PLACE', 'Product_Code', 'Segment', 'Customer_Site_ID']
        )

        # Aggregate count per grouping dimensions
        df = self.sql_df.df_groupby(
            df,
            ['MARKET_PLACE', 'Customer_Site_ID', 'Segment'],
            'UnitCount',
            'Customer_Site_ID',
            "count"
        )

        # Create categorical segmentation (CASE WHEN equivalent)
        df = self.sql_df.df_case(
            df=df,
            columns_to_keep=['MARKET_PLACE', 'Segment', 'UnitCount'],
            value_column='UnitCount',
            ranges=[(1, 2), (3, 5)],
            labels=['1-2', '3-5'],
            default_label='6 or more',
            new_column_name='Category'
        )

        # Pivot values into columns
        base = self.sql_df.df_pivot_values_to_columns(
            df=df,
            group_col_1='MARKET_PLACE',
            group_col_2='Category',
            value_column='Segment',
            values=['Seg 1-3', 'Seg 4-6']
        )

        # Add subtotal and grand total rows (ROLLUP equivalent)
        totals = self.sql_df.df_groupby_rollup(
            base_df=base,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )

        df = pd.concat([base, totals], ignore_index=True)

        # Order data respecting grouping hierarchy
        df = self.sql_df.df_orderby_grouping(
            df,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )

        return df
