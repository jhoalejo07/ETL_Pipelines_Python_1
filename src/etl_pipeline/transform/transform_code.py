import pandas as pd
import numpy as np
from typing import List, Tuple
from src.etl_pipeline.extract.extract_code import Extract
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

        df = self._categorize_by_range(
            df=df,
            columns_to_keep=['MARKET_PLACE', 'Segment', 'UnitCount'],
            value_column='UnitCount',
            ranges=ranges,
            labels=labels,
            default_label='6 or more'
        )

        # Step 1: Base rollup this is a pivot function with two rows values to pass to be columns
        base = self._pivot_values_to_columns(
            df=df,
            group_col_1='MARKET_PLACE',
            group_col_2='Category',
            value_column='Segment',
            value_1='Seg 1-3',
            value_2='Seg 4-6'
        )

        subtotal = self._create_subtotal_x_group(
            base_df=base,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )

        grand_total = self._create_grand_total(
            base_df=base,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )

        df = pd.concat([base, subtotal, grand_total], ignore_index=True)

        df = self._sort_rollup_result(
            df=df,
            group_col_1='MARKET_PLACE',
            group_col_2='Category'
        )

        return df


    # =====================================================
    # METHODS (business logic)
    # =====================================================
    def _categorize_by_range(self,
            df: pd.DataFrame,
            columns_to_keep: List[str],
            value_column: str,
            ranges: List[Tuple[int, int]],
            labels: List[str],
            default_label: str
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

        result['Category'] = np.select(
            conditions,
            labels,
            default=default_label
        )

        return result

    def _pivot_values_to_columns(self,
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

    def _create_subtotal_x_group(self,
            base_df,
            group_col_1: str,
            group_col_2: str,
            total_label: str = "Total"
        ):
        """
        Creates subtotal by summing numeric columns
        grouped by the first column.
        """

        subtotal = (
            base_df
            .groupby(group_col_1, as_index=False)
            .sum(numeric_only=True)
        )

        # Put "Total" in second grouping column
        subtotal[group_col_2] = total_label

        return subtotal

        # This creates one final row with everything summed.

    def _create_grand_total(self,
                            base_df,
                            group_col_1: str,
                            group_col_2: str,
                            grand_total_label: str = "Grand Total",
                            total_label: str = "Total"
                            ):
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

        return pd.DataFrame([grand_total_row])



    def _sort_rollup_result( self,
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

