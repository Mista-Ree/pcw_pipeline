import pandas as pd
from sklearn.linear_model import LinearRegression

from pipeline.base import ModelSpec

linear_regression = ModelSpec(
    name="Linear regression",
    model=LinearRegression(),
    generate_X=lambda df: df[~pd.isna(df["credit_score"])].groupby("quoteid")["credit_score"].first().values.reshape(-1, 1),
    generate_y=lambda df: df[~pd.isna(df["credit_score"])].groupby("quoteid")["apr"].first().values.reshape(-1, 1),
)

linear_regression_random_sample = ModelSpec(
    name="Linear regression random sample",
    model=LinearRegression(),
    generate_X=lambda df: df.dropna().sample(100)["credit_score"].values.reshape(-1, 1),
    generate_y=lambda df: df.dropna().sample(100)["apr"].values.reshape(-1, 1),
)
