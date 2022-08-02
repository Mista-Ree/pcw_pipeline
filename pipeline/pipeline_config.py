from pipeline.base import Cleaner, ModelSpec, Transform, Validator
from pipeline.cleaners import RemoveRowsWithZero
from pipeline.model_specs import linear_regression, linear_regression_random_sample
from pipeline.transforms import RankedFirstSummary, TopRankingAPR, WeeklySummary
from pipeline.validators import ReportNaN, ThresholdCheck

INPUT_CSV="quotes.csv.gz"
CLEAN_CSV="clean_quotes.csv.gz"

CLEANERS: list[Cleaner] = [
    RemoveRowsWithZero(["apr"])
]

VALIDATORS: list[Validator] = [
    ReportNaN(["credit_score"]),
    ThresholdCheck("apr", min_val=0.0, max_val=1.0)
]

TRANSFORMS: list[Transform] = [
    RankedFirstSummary(output_csv="ranked_first_summary.csv.gz"),
    TopRankingAPR(output_csv="top_ranking_apr.csv.gz"),
    WeeklySummary(output_csv="weekly_summary.csv.gz"),
]

MODEL_SPECS: list[ModelSpec] = [
    linear_regression,
    linear_regression_random_sample,
]
