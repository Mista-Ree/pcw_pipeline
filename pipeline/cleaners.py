import logging

import pandas as pd

from pipeline.base import Cleaner


logger = logging.getLogger(__name__)


class RemoveRowsWithZero(Cleaner):
    def __init__(self, columns_to_check: list[str]) -> None:
        self.columns_to_check = columns_to_check

    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        rows_with_zero = (df[self.columns_to_check] == 0).any(axis=1)
        logger.info(f"Dropping {len(df[rows_with_zero]):,}/{len(df):,} rows that contain a zero in {self.columns_to_check}")

        return df[~rows_with_zero]


