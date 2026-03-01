import pandas as pd
import numpy as np
from typing import List, Tuple


class SQl_df():
    """
    Helper class that provides SQL-like operations using pandas.

    The idea is to make transformations easier to read for someone familiarized with SQL logic (select, join, group by, etc.).
    """

    def __init__(self):
        """
        No state is stored. This class only provides reusable methods.
        """
        pass

    def rename_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Rename columns by replacing spaces and slashes with underscores.
        """
        df.columns = df.columns.str.replace(' ', '_', regex=False)
        df.columns = df.columns.str.replace('/', '_', regex=False)
        return df

    def convert_to_numeric(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
        """
        Converts a column to numeric type.  Also removes commas before converting.
        """

        # Create a copy to avoid modifying the original dataframe
        df_copy = df.copy()

        # Remove commas (example: "1,000" → "1000")
        df_copy[column_name] = (
            df_copy[column_name]
            .astype(str)
            .str.replace(',', '', regex=False)
        )

        # Convert to numeric, invalid values become NaN
        df_copy[column_name] = pd.to_numeric(df_copy[column_name], errors='coerce')

        return df_copy

    def join_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, column_to_join: str, join_type) -> pd.DataFrame:
        """
        Joins two dataframes using a common column.  Similar to SQL JOIN.
        """
        return df1.merge(
            df2,
            on=column_to_join,
            how=join_type
        )

    def apply_filters(self, df: pd.DataFrame, column_name: str, operator: str, value: float) -> pd.DataFrame:
        """
        Filters rows based on a condition. Similar to SQL WHERE.
        """
        # Map operators to pandas conditions
        operators = {
            '>=': df[column_name] >= value,
            '<=': df[column_name] <= value,
            '>': df[column_name] > value,
            '<': df[column_name] < value,
            '==': df[column_name] == value,
            '!=': df[column_name] != value
        }

        # Check if operator is valid
        if operator not in operators:
            raise ValueError("Invalid operator")

        # Return filtered dataframe
        # Ex: If operator = ">=", then: operators[">="] which is df["BillAmount"] >= 1000 So the line becomes:
        # return df[df["BillAmount"] >= 1000]
        return df[operators[operator]]

    def df_select_columns(self, df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
        """
        Select specific columns.   Similar to SQL SELECT.
        """
        return df[columns]

    def df_groupby_count(self, df: pd.DataFrame, columns_groupby: list[str], Counter_Name: str) -> pd.DataFrame:
        """
        Groups data and counts rows.
        """

        return (
            df
            .groupby(columns_groupby, as_index=False)  # Keep flat structure
            .size()
            .rename(columns={'size': Counter_Name})
        )

    def df_groupby(self, df: pd.DataFrame, columns_groupby: list[str], Agg_Name: str, column_to_agg: str,
                   agg_func: str) -> pd.DataFrame:
        """
        Groups data and applies an aggregation (count, mean, sum, etc.).
        """

        return (
            df
            # Group rows by the columns passed on the list
            .groupby(columns_groupby, as_index=False)
            # Apply an aggregation (agg_func) on the column indicated (column_to_agg) and create a new column (Agg_Name)
            .agg(**{Agg_Name: (column_to_agg, agg_func)})
        )

    def df_case(
            self,
            df: pd.DataFrame,
            columns_to_keep: List[str],
            value_column: str,
            ranges: List[Tuple[int, int]],
            labels: List[str],
            default_label: str,
            new_column_name: str
    ) -> pd.DataFrame:
        """
        Creates a new category column based on numeric ranges.  Similar to SQL CASE WHEN.
        """

        # Make sure ranges and labels match
        if len(ranges) != len(labels):
            raise ValueError("Ranges and labels must match")

        # Work on selected columns only
        result = df[columns_to_keep].copy()

        # check each value_column in every range and create a boolean list true is in the range or false if isn't
        conditions = [
            result[value_column].between(min_val, max_val)
            for min_val, max_val in ranges
        ]

        # Assigns labels based on if the condition is true the range put the respective label
        result[new_column_name] = np.select(
            conditions,
            labels,
            default=default_label  # in case condition is false for every range assigns default value
        )

        return result


    '''
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
    '''

    def df_pivot_values_to_columns(
            self,
            df,
            group_col_1,
            group_col_2,
            value_column,
            values
    ):
        """
        Creates a pivot table.
        It transforms specific values from one column into separate columns.
        Each new column shows how many times that value appears.
        """

        pivot = (
            # Keep only rows where value_column matches the selected values
            df[df[value_column].isin(values)]

            # Create pivot table:
            # - index: group by these two columns
            # - columns: turn value_column values into new columns
            # - aggfunc="size": count how many times each value appears
            # - fill_value=0: replace missing counts with 0
            .pivot_table(
                index=[group_col_1, group_col_2],
                columns=value_column,
                aggfunc="size",
                fill_value=0
            )

            # Convert index back into normal columns
            .reset_index()
        )

        # Create a total column summing all generated value columns
        pivot["Grand_Total"] = pivot[values].sum(axis=1)

        return pivot



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

    def df_orderby_grouping(self, df, group_col_1, group_col_2,
                            total_label="Total",
                            grand_total_label="Grand Total"):
        """
        Sorts the result so:
        - Normal rows come first
        - Subtotals come after each group
        - Grand total is last
        """

        # Create a temporary column to identify Grand Total rows, True = is Grand Total, False = normal row
        df["_group1_sort"] = df[group_col_1] == grand_total_label

        # Identify subtotal rows, True = is subtotal, False = normal row
        df["_group2_sort"] = df[group_col_2] == total_label

        # Sort using:
        # 1) Grand Total flag
        # 2) First grouping column
        # 3) Subtotal flag
        # 4) Second grouping column
        df = df.sort_values(
            by=["_group1_sort", group_col_1, "_group2_sort", group_col_2]
        )

        # Remove helper columns
        df = df.drop(columns=["_group1_sort", "_group2_sort"])

        return df
