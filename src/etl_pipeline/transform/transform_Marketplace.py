import pandas as pd
import numpy as np
from typing import List, Tuple
from src.etl_pipeline.extract.extract_Marketplace import Extract
from src.etl_pipeline.utils.SQL_with_Dataframes import SQl_df


class Transform:
    def __init__(self, extract: Extract):
        self.df1, self.df2 = extract.extract()
        self.sql_df = SQl_df()
        self.data = self._transform()


    # =====================================================
    # Main orchestration
    # =====================================================
    def _transform(self) -> pd.DataFrame:

        # Convert into numeric the field Equipment_Rental_Payment_Month
        self.Column_To_Number1 = 'Equipment_Rental_Payment_Month'
        df = self.sql_df.convert_to_numeric(self.df1, self.Column_To_Number1)

        # Joining two dataframes
        self.column_to_join = 'Product_Code'
        self.join_type = 'inner'
        df = self.sql_df.join_dataframes(df, self.df2, self.column_to_join, self.join_type)

        # Filter the result for a specific column
        self.filter_column = 'Equipment_Rental_Payment_Month'
        self.filter_operator = '>='
        self.filter_value = 25
        df = self.sql_df.apply_filters(df, self.filter_column, self.filter_operator, self.filter_value)

        # Select from specific columns
        self.cols_join = ['MARKET_PLACE', 'Product_Code', 'Segment', 'Customer_Site_ID']
        df = self.sql_df.df_select_columns(df, self.cols_join)

        # Grouping by determinate columns and creating a new column counter
        self.cols_groupby = ['MARKET_PLACE', 'Customer_Site_ID', 'Segment']
        self.Counter_name = 'UnitCount'
        df = self.sql_df.df_groupby(df, self.cols_groupby, self.Counter_name)

        # Create a new column to categorize the counter based on the ranges: '1-2', '3-5', '6 or more'
        ranges = [(1, 2), (3, 5)]
        labels = ['1-2', '3-5']

        df = self.sql_df.df_case(
            df=df,
            columns_to_keep=['MARKET_PLACE', 'Segment', 'UnitCount'],
            value_column='UnitCount',
            ranges=ranges,
            labels=labels,
            default_label='6 or more',
            new_column_name='Category'
        )


        base = self.sql_df.df_pivot_2values_to_2columns(
            df=df,
            group_col_1='MARKET_PLACE',
            group_col_2='Category',
            value_column='Segment',
            value_1='Seg 1-3',
            value_2='Seg 4-6'
        )

        totals = self.sql_df.df_groupby_rollup(
            base_df=base,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )


        df = pd.concat([base, totals], ignore_index=True)

        df = self.sql_df.df_orderby_grouping(
            df=df,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )

        return df


    # =====================================================
    # METHODS (business logic)
    # =====================================================


