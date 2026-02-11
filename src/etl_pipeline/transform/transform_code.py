import pandas as pd
import numpy as np
from src.etl_pipeline.extract.extract_code import Extract


class Transform:
    def __init__(self, extract: Extract):
        self.rentals, self.segments = extract.extract()

        self.data = self._transform()


    # =====================================================
    # Main orchestration
    # =====================================================
    def _transform(self) -> pd.DataFrame:
        #df = self._prepare_rentals()
        self.Column_To_Number1 = 'Equipment_Rental_Payment_Month'
        self.column_to_join = 'Product_Code'
        self.join_type = 'inner'
        self.filter_column = 'Equipment_Rental_Payment_Month'
        self.filter_operator = '>='
        self.filter_value = 25


        df = self._prepare_numeric_column(self.rentals, self.Column_To_Number1)
        df = self._join_dataframes(df, self.segments, self.column_to_join, self.join_type)
        df = self._apply_filters(df, self.filter_column, self.filter_operator, self.filter_value)
        df = self._select_columns(df)

        df = self._count_units_by_customer_site(df)
        df = self._classify_unit_counts(df)
        df = self._build_marketplace_rollup(df)

        return df

    # =====================================================
    # Existing logic (UNCHANGED)
    # =====================================================
    '''
    def _prepare_rentals(self) -> pd.DataFrame:
        df = self.rentals.copy()

        df['Equipment_Rental_Payment_Month'] = (
            df['Equipment_Rental_Payment_Month']
            .astype(str)
            .str.replace(',', '', regex=False)
        )

        df['Equipment_Rental_Payment_Month'] = pd.to_numeric(
            df['Equipment_Rental_Payment_Month'],
            errors='coerce'
        )

        return df
    '''

    def _prepare_numeric_column(self, df: pd.DataFrame, column_name: str) -> pd.DataFrame:
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

    '''
    def _join_segments(self, rentals_df: pd.DataFrame) -> pd.DataFrame:
        return rentals_df.merge(
            self.segments,
            on='Product_Code',
            how='inner'
        )
    '''
    def _join_dataframes(self, df1: pd.DataFrame, df2: pd.DataFrame, column_to_join: str, join_type) -> pd.DataFrame:
        return df1.merge(
            df2,
            on=column_to_join,
            how=join_type
        )

    '''
    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[df['Equipment_Rental_Payment_Month'] >= 25]
    '''

    def _apply_filters(self, df: pd.DataFrame, column_name: str, operator: str, value: float) -> pd.DataFrame:
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



    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df[
            ['MARKET_PLACE', 'Product_Code', 'Segment', 'Customer_Site_ID']
        ]

    # =====================================================
    # NEW METHODS (business logic)
    # =====================================================
    def _count_units_by_customer_site(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df
            .groupby(
                ['MARKET_PLACE', 'Customer_Site_ID', 'Segment'],
                as_index=False
            )
            .size()
            .rename(columns={'size': 'UnitCount'})
        )

    def _classify_unit_counts(self, df: pd.DataFrame) -> pd.DataFrame:
        result = df[['MARKET_PLACE', 'Segment', 'UnitCount']].copy()

        conditions = [
            result['UnitCount'].between(1, 2),
            result['UnitCount'].between(3, 5)
        ]

        choices = ['1-2', '3-5']

        result['Category'] = np.select(
            conditions,
            choices,
            default='6 or more'
        )

        return result

    def _build_marketplace_rollup(
        self,
        df: pd.DataFrame,
        market_col: str = 'MARKET_PLACE',
        category_col: str = 'Category',
        segment_col: str = 'Segment',
        seg_13_value: str = 'Seg 1-3',
        seg_46_value: str = 'Seg 4-6',
        total_label: str = 'Total',
        grand_total_label: str = 'Grand Total'
    ) -> pd.DataFrame:

        base = (
            df
            .groupby([market_col, category_col], dropna=False)
            .agg(
                **{
                    seg_13_value: (segment_col, lambda x: (x == seg_13_value).sum()),
                    seg_46_value: (segment_col, lambda x: (x == seg_46_value).sum()),
                    'Grand Total': (segment_col, 'count')
                }
            )
            .reset_index()
        )

        subtotal_market = (
            base
            .groupby(market_col, as_index=False)
            .agg({
                seg_13_value: 'sum',
                seg_46_value: 'sum',
                'Grand Total': 'sum'
            })
        )

        subtotal_market[category_col] = total_label

        grand_total = pd.DataFrame([{
            market_col: grand_total_label,
            category_col: total_label,
            seg_13_value: base[seg_13_value].sum(),
            seg_46_value: base[seg_46_value].sum(),
            'Grand Total': base['Grand Total'].sum()
        }])

        result = pd.concat(
            [base, subtotal_market, grand_total],
            ignore_index=True
        )

        result['_market_sort'] = result[market_col].eq(grand_total_label)
        result['_category_sort'] = result[category_col].eq(total_label)

        result = (
            result
            .sort_values(
                by=['_market_sort', market_col, '_category_sort', category_col]
            )
            .drop(columns=['_market_sort', '_category_sort'])
        )

        return result