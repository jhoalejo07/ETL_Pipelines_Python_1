import pandas as pd
import numpy as np
from typing import List, Tuple

class SQl_df():
    def __init__(self):
        pass

    def convert_to_numeric(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
           # Make a copy of the dataframe to avoid modifying the original
            df_copy = df.copy()

            # Clean the specified column by removing commas and converting to numeric
            df_copy[column_name] = (
                df_copy[column_name]
                .astype(str)
                .str.replace(',', '', regex=False)
            )

            # Convert the column to numeric, coerce any errors to NaN
            df_copy[column_name] = pd.to_numeric(df_copy[column_name], errors='coerce')

            return df_copy

    def join_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, column_to_join: str, join_type) -> pd.DataFrame:
        return df1.merge(
            df2,
            on=column_to_join,
            how=join_type
        )

    def apply_filters(self, df: pd.DataFrame, column_name: str, operator: str, value: float) -> pd.DataFrame:
        # Define a dictionary to map logical operators to their corresponding pandas functions
        operators = {
            '>=': df[column_name] >= value,
            '<=': df[column_name] <= value,
            '>': df[column_name] > value,
            '<': df[column_name] < value,
            '==': df[column_name] == value,
            '!=': df[column_name] != value
        }

        # Ensure the operator is valid
        if operator not in operators:
            raise ValueError(f"Invalid operator: {operator}. Supported operators are: {', '.join(operators.keys())}")

        # Apply the filter based on the operator
        return df[operators[operator]]


    def df_select_columns(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        return df[columns]

    def df_groupby(self, df: pd.DataFrame, columns_groupby: list[str], Counter_Name: str) -> pd.DataFrame:
        return (
            df
            .groupby(
                columns_groupby,
                as_index=False
            )
            .size()
            .rename(columns={'size': Counter_Name})
        )

