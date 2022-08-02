import pandas as pd

from pipeline.base import Transform

class RankedFirstSummary(Transform):
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        binned = self._get_ranked_first_summary(5, df)
        total = self._get_ranked_first_summary(1, df)

        return pd.concat([binned, total])

    def _get_ranked_first_summary(self, num_bins: int, df: pd.DataFrame) -> pd.DataFrame:
        df["credit_score_range"] = pd.cut(df["credit_score"], num_bins)

        apr_series = pd.Series(df[df["rank"] == 1.0].groupby(["credit_score_range", "party"])["apr"].mean(), name="avg_apr")
        ranked_first_series = pd.Series(
            df.groupby(["credit_score_range", "party"]).apply(lambda df: self._get_ranked_first_rate(df.name[1], df)),
            name="ranked_first_rate"
        )
        return pd.concat([apr_series, ranked_first_series], axis=1)

    @staticmethod
    def _get_ranked_first_rate(bid: str, df: pd.DataFrame) -> float:
        ranked_first_df = df[(df["party"] == bid) & (df["rank"] == 1.0)]
        return len(ranked_first_df) / len(df["quoteid"].drop_duplicates())


class TopRankingAPR(Transform):
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        other_df = df[df["party"] != "Our Bid"]
        top_ranking_idx = other_df.groupby("quoteid")["rank"].transform(min) == other_df["rank"]
        top_ranking_other_df = other_df[top_ranking_idx]

        our_quotes_df = df[df["party"] == "Our Bid"]

        cols = ["apr", "rank", "quoteid", "datetime"]
        merged_df = top_ranking_other_df[cols].merge(
            our_quotes_df[cols],
            how="left",
            on=["quoteid", "datetime"],
            suffixes=("_other", "_ours")
        )
        merged_df.set_index("quoteid", inplace=True)
        return merged_df


class WeeklySummary(Transform):
    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.resample("W-Mon", on='datetime')[["duration_requested", "amount_requested", "apr", "quoteid"]].agg(
            count=("quoteid", "nunique"),
            avg_duration_requested=("duration_requested", "mean"),
            avg_amount_requestd=("amount_requested", "mean"),
            avg_apr=("apr", "mean")
        )

