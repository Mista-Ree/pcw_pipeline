import logging
from typing import Callable, Tuple

import pandas as pd
from sklearn.metrics import mean_squared_error

from pipeline.base import Cleaner, ModelSpec, Transform, Validator
from pipeline.pipeline_config import CLEAN_CSV, CLEANERS, INPUT_CSV, MODEL_SPECS, VALIDATORS, TRANSFORMS


FORMAT = '%(asctime)s %(levelname)s %(name)s %(message)s'
logging.basicConfig(level=logging.INFO, format=FORMAT)

logger = logging.getLogger(__name__)


def ingest(raw_data: Callable[[], pd.DataFrame] | str=INPUT_CSV, cleaners: list[Cleaner]=CLEANERS, output_csv_path: str | None=None) -> pd.DataFrame:
    if isinstance(raw_data, str):
        df = load_csv(raw_data)
    else:
        df = raw_data()

    logger.info("Extracting raw dataset.")

    for cleaner in cleaners:
        logger.info(f"Running cleaner: {cleaner.__class__.__name__}")
        cleaner.clean(df)

    if output_csv_path is not None:
        logger.info("Saving cleaned data as %s.", output_csv_path)
        df.to_csv(output_csv_path)

    return df


def validate(input: pd.DataFrame | str=CLEAN_CSV, validators: list[Validator]=VALIDATORS) -> bool:
    if isinstance(input, str):
        input = load_csv(input)

    for validator in validators:
        logger.info(f"Running validator: {validator.__class__.__name__}")
        if not validator.validate(input):
            logger.error(f"{validator.__class__.__name__} validator failed.")
            return False

    return True


def transform(input: pd.DataFrame | str=CLEAN_CSV, transforms: list[Transform]=TRANSFORMS, print_transform_output: bool=False) -> list[pd.DataFrame]:
    if isinstance(input, str):
        input = load_csv(input)

    dfs: list[pd.DataFrame] = []
    for transform in transforms:
        transformed_df = transform.run(input)
        dfs.append(transformed_df)
        if print_transform_output:
            print("\n")
            print(f"Output from {transform.__class__.__name__} transform")
            print(transformed_df)
            print("\n")
        if transform.output_csv is not None:
            logger.info(f"Saving output from {transform.__class__.__name__} to {transform.output_csv}")
            transformed_df.to_csv(transform.output_csv)

    return dfs


def evaluate(input: pd.DataFrame | str=CLEAN_CSV, model_specs: list[ModelSpec]=MODEL_SPECS, print_evaluation_output: bool=False) -> pd.DataFrame:
    if isinstance(input, str):
        input = load_csv(input)

    evaluations: list[Tuple[str, float]] = []
    for spec in model_specs:
        y_true = spec.generate_y(input)
        X = spec.generate_X(input)
        spec.model.fit(X, y_true)
        y_predict = spec.model.predict(X)
        error = mean_squared_error(y_true, y_predict)
        evaluations.append((spec.name, error))

    evaluation_df = pd.DataFrame(evaluations, columns=["model_name", "mean_squared_error"])
    if print_evaluation_output:
        print("Model fitting results:")
        print(evaluation_df)
    return evaluation_df


def run_pipeline(
    input_csv: str=INPUT_CSV,
    output_csv: str=CLEAN_CSV,
    print_transform_output: bool=False,
    print_evaluation_output: bool=False
) -> None:
        df = ingest(input_csv, output_csv_path=output_csv)
        if not validate(df):
            return
        transform(df, print_transform_output=print_transform_output)
        evaluate(df, print_evaluation_output=print_evaluation_output)


def load_csv(csv_path: str) -> pd.DataFrame:
    return pd.read_csv(csv_path, index_col=0, parse_dates=["datetime"])


if __name__ == "__main__":
    run_pipeline()
