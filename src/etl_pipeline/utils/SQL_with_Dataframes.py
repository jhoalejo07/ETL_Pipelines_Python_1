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

    def df_case(self,
            df: pd.DataFrame,
            columns_to_keep: List[str],
            value_column: str,
            ranges: List[Tuple[int, int]],
            labels: List[str],
            default_label: str,
            new_column_name: str
    ) -> pd.DataFrame:
        """
        Classifies a numeric column into categories based on provided ranges.

        Parameters
        ----------
        df : pd.DataFrame
            Input DataFrame
        columns_to_keep : list of str
            Columns to retain in the result
        value_column : str
            Column to classify
        ranges : list of tuples
            List of (min, max) ranges (inclusive)
        labels : list of str
            Labels corresponding to each range
        default_label : str
            Label assigned when no range matches
        """

        if len(ranges) != len(labels):
            raise ValueError("The number of ranges and labels must be the same")

        result = df[columns_to_keep].copy()

        conditions = [
            result[value_column].between(min_val, max_val)
            for min_val, max_val in ranges
        ]

        result[new_column_name] = np.select(
            conditions,
            labels,
            default=default_label
        )

        return result


    def df_pivot_2values_to_2columns(self,
            df,
            group_col_1: str,
            group_col_2: str,
            value_column: str,
            value_1: str,
            value_2: str
        ):
        """

        Pivots two specific values (value_1&2) from a single column (value_column) into two separate columns
        with counts of how many times each value appears for each group (group_col_1&2).

        """

        base = (
            df
            .groupby([group_col_1, group_col_2], dropna=False)
            .agg(
                count_value_1=(value_column, lambda x: (x == value_1).sum()),
                count_value_2=(value_column, lambda x: (x == value_2).sum()),
                total_count=(value_column, "count")
            )
            .reset_index()
        )
        base = base.rename(columns={"count_value_1": value_1, "count_value_2": value_2, "total_count": "Grand_Total"})

        return base

    def df_groupby_rollup(self,
                  base_df,
                  group_col_1: str,
                  group_col_2: str,
                  grand_total_label: str = "Grand Total",
                  total_label: str = "Total"
                  ):
        """
        Creates subtotal by summing numeric columns grouped by the first column.
        """

        subtotal = (
            base_df
            .groupby(group_col_1, as_index=False)
            .sum(numeric_only=True)
        )

        # Put "Total" in second grouping column
        subtotal[group_col_2] = total_label

        # return subtotal

        """
        Creates one single row with the total of everything.
        """

        totals = base_df.sum(numeric_only=True)

        grand_total_row = {
            group_col_1: grand_total_label,
            group_col_2: total_label
        }

        # Add numeric totals
        for col in base_df.select_dtypes(include="number").columns:
            grand_total_row[col] = totals[col]

        grand_total_df = pd.DataFrame([grand_total_row])

        result = pd.concat([subtotal, grand_total_df], ignore_index=True)

        return result

    def df_orderby_grouping( self,
            df,
            group_col_1: str,
            group_col_2: str,
            total_label: str = "Total",
            grand_total_label: str = "Grand Total"
    ):
        """
        Sorts the rollup result so that:

        1. Normal rows appear first
        2. Subtotals (Total) appear after their group
        3. Grand Total appears at the very bottom
        """

        # ---------------------------------------------------------
        # STEP 1:
        # Create a temporary column to identify Grand Total rows
        # True = is Grand Total
        # False = normal row
        # ---------------------------------------------------------
        df["_group1_sort"] = df[group_col_1] == grand_total_label

        # ---------------------------------------------------------
        # STEP 2:
        # Create a temporary column to identify subtotal rows
        # True = is subtotal
        # False = normal row
        # ---------------------------------------------------------
        df["_group2_sort"] = df[group_col_2] == total_label

        # ---------------------------------------------------------
        # STEP 3:
        # Sort using:
        # 1) Grand Total flag
        # 2) First grouping column
        # 3) Subtotal flag
        # 4) Second grouping column
        # ---------------------------------------------------------
        df = df.sort_values(
            by=["_group1_sort", group_col_1, "_group2_sort", group_col_2]
        )

        # ---------------------------------------------------------
        # STEP 4:
        # Remove temporary helper columns
        # ---------------------------------------------------------
        df = df.drop(columns=["_group1_sort", "_group2_sort"])

        return df
