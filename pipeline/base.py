from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd
from numpy.typing import NDArray
from sklearn.linear_model import LinearRegression

class Cleaner(ABC):
    @abstractmethod
    def clean(self, df: pd.DataFrame) -> None:
        pass


class Validator(ABC):
    @abstractmethod
    def validate(self, df: pd.DataFrame) -> bool:
        pass


class Transform(ABC):
    def __init__(self, output_csv: str | None) -> None:
        self.output_csv = output_csv

    @abstractmethod
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        return df

@dataclass(frozen=True)
class ModelSpec():
    name: str
    model: LinearRegression
    generate_X: Callable[[pd.DataFrame], NDArray]
    generate_y: Callable[[pd.DataFrame], NDArray]
