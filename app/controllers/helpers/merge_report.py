import pandas as pd
from pprint import pprint


class Merger:
    def __init__(
        self,
        source_df,
        target_df,
        src_params,
        trgt_params
    ) -> None:
        self.source_df = source_df
        self.target_df = target_df
        self.src_params = src_params
        self.trgt_params = trgt_params
        self.merged_reports = self.reports_merger()
        self.matched_list = self.merged_reports["matched_list"]
        self.src_umatched_list = self.merged_reports["src_umatched_list"]
        self.trgt_umatched_list = self.merged_reports["trgt_umatched_list"]

    def reports_merger(self):

        merge_df = pd.merge(
            self.source_df,
            self.target_df,
            left_on=self.src_params,
            right_on=self.trgt_params,
            how="outer",
            suffixes=('_src', '_trg'),
            indicator=True,
        )

        matched_df = merge_df[merge_df["_merge"] == "both"]
        unmatched_src_df = merge_df[merge_df["_merge"] == "left_only"].dropna(axis="columns", how="all")
        unmatched_trgt_df = merge_df[merge_df["_merge"] == "right_only"].dropna(axis="columns", how="all")


        return {
            "matched_list": matched_df.to_dict('records'),
            "src_umatched_list": unmatched_src_df.to_dict('records'),
            "trgt_umatched_list": unmatched_trgt_df.to_dict('records'),
        }
