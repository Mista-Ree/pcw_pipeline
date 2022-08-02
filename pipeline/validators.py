import logging

import pandas as pd

from pipeline.base import Validator

logger = logging.getLogger(__name__)


class ThresholdCheck(Validator):
    def __init__(self, column_to_check: str, min_val: float | None, max_val: float | None, fail_on_breach: bool=False) -> None:
        self.column_to_check = column_to_check
        self.min_val = min_val
        self.max_val = max_val
        self.fail_on_breach = fail_on_breach

    def validate(self, df: pd.DataFrame) -> bool:
        checked_df = df
        if self.min_val is not None:
            checked_df = checked_df[checked_df[self.column_to_check] >= self.min_val]

        if self.max_val is not None:
            checked_df = checked_df[checked_df[self.column_to_check] <= self.max_val]

        is_breached = len(checked_df) < len(df)
        if is_breached:
            num_rows_breached = len(df) - len(checked_df)
            logger.warning(f"{num_rows_breached:,}/{len(df):,} ({num_rows_breached/len(df):.2%}) of rows in '{self.column_to_check}' breach configured min/max thresholds {self.min_val}/{self.max_val}.")

        return not (self.fail_on_breach and is_breached)


class ReportNaN(Validator):
    def __init__(self, columns_to_check: list[str]) -> None:
        self.columns_to_check = columns_to_check

    def validate(self, df: pd.DataFrame) -> bool:
        rows_with_nan = df[df[self.columns_to_check].isna().any(axis=1)]
        if len(rows_with_nan) > 0:
            logger.warning(f"{len(rows_with_nan):,}/{len(df):,} ({len(rows_with_nan)/len(df):.2%}) of rows contain a NaN in one of {self.columns_to_check}.")

        return True
