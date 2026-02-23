import pandas as pd
from src.etl_pipeline.utils.SQL_with_Dataframes import SQl_df


class Transform:
    def __init__(self, raw_data: dict):
        """
        Initialize the transform class,
        assigns raw data from extract class,
        initialize sql_df to handle SQL_with_dataframes .
        """
        self.dataframes = raw_data
        self.sql_df = SQl_df()
        self.data = self._transform()

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
        1st Step. column renaming for each dataframe
        """
        for filename, df in self.dataframes.items():
            # Apply the rename_columns transformation
            self.dataframes[filename] = self._rename_columns(df)

        """
        2nd Step. # Unpack dynamically
        """
        dfs = list(self.dataframes.values())

        if len(dfs) < 2:
            raise ValueError("At least two dataframes are required for this transformation.")

        df1, df2 = dfs[:2]  # works even if there are more

        # Convert into numeric the field BillAmount
        self.Column_To_Number1 = 'BillAmount'
        df = self.sql_df.convert_to_numeric(df1, self.Column_To_Number1)

        # Joining two dataframes
        self.column_to_join = 'AgeRangeID'
        self.join_type = 'inner'
        df = self.sql_df.join_dataframes(df, df2, self.column_to_join, self.join_type)

        '''
        # Grouping by determinate average and creating a new variable
        self.cols_groupby = ['BillAmount']
        self.agg_name = 'AvgBillAmount'
        self.column_to_agg = 'BillAmount'
        df_avg = self.sql_df.df_groupby(df, self.cols_groupby, self.agg_name, self.column_to_agg, "mean")
        '''
        # Filter the result for a specific column
        self.filter_column = 'BillAmount'
        self.filter_operator = '>='
        self.filter_value = 1000
        df = self.sql_df.apply_filters(df, self.filter_column, self.filter_operator, self.filter_value)

        # Select from specific columns
        self.cols_join = ['Province', 'PatientID', 'AgeRangeLabel', 'Hospital', 'BillAmount']
        df = self.sql_df.df_select_columns(df, self.cols_join)

        # Create a new column to categorize the counter based on the ranges: '1-2', '3-5', '6 or more'
        ranges = [(1000, 5000), (5001, 9999)]
        labels = ['<5000', '<10000']

        df = self.sql_df.df_case(
            df=df,
            columns_to_keep=['Province', 'AgeRangeLabel', 'PatientID', 'BillAmount'],
            value_column='BillAmount',
            ranges=ranges,
            labels=labels,
            default_label='>10000',
            new_column_name='Category'
        )

        base = self.sql_df.df_pivot_values_to_columns(
            df=df,
            group_col_1='Province',
            group_col_2='Category',
            value_column='AgeRangeLabel',
            values=['Child', 'Adult', 'Elderly']
        )

        totals = self.sql_df.df_groupby_rollup(
            base_df=base,
            group_col_1='Province',
            group_col_2='Category'
        )

        df = pd.concat([base, totals], ignore_index=True)

        df = self.sql_df.df_orderby_grouping(
            df=df,
            group_col_1='Province',
            group_col_2='Category'
        )

        return df
