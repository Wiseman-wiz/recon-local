"""
###### segregation conditions #####
1. date_period within range
2. unmatch_gl - exist in gl but not in bs
3. unmatch_bs - exist in bs but not in gl
4. match - both exist in both gl and bs

create a benchmark for legacy code.
current benchmark for this implementatio is at 0:00:00.032753 ms
"""

from time import time
import pandas as pd
import timeit
from pprint import pprint
import numpy as np
from datetime import datetime
from .. import DbOps


mo = DbOps.MainOps()
client = mo.client
db = client["bank_recon_field_keywords"]
pd.options.mode.chained_assignment = None
# pd.options.display.float_format = '{:,.2f}'.format
# pd.set_option('precision', 2)


class Segregator:
    """Segregates match and unmatch list of 2 collections.

    Handles the merging, matching, and calculating the amounts of the
    collections using pandas.
    """

    def __init__(
        self,
        general_ledger_df,
        bank_statement_df,
        filtered_df,
        to_match: dict,
        from_date: datetime,
        to_date: datetime,
    ) -> None:
        """
        PARAMS
        ==========
        general_ledger_df (pandas.dataframes): documents of collection of gl.
        bank_statement_df (pandas.dataframes): documents of collection of bs.
        to_match: a dict of fields to match on collections. key as the gl and
            val as the bs field.
        from_date: datetime of start date of transaction period to recon in
            the collections.
        to_date: datetime of end date of transaction period to recon in the
            collections.
        """

        self.field_to_match = to_match
        self.strd_cr_amt = ""
        self.strd_dr_amt = ""
        self.strd_trn_ref = ""
        self.strd_trn_date = ""
        self.gl_suffix = "gl"
        self.bs_suffix = "bs"
        self.from_date = from_date
        self.to_date = to_date
        # --- above are static
        self.general_ledger_df = general_ledger_df
        self.bank_statement_df = bank_statement_df
        # --- no change
        self.filtered_df = filtered_df
        self.segregated = self.segregation()
        self.total_dr_cr_amts = self.segregated["total_dr_cr_amts"]
        self.book_errors_amt = self.segregated["book_errors_amt"]
        self.original_bs = self.segregated["original_bs"]
        self.original_gl = self.segregated["original_gl"]
        self.match = self.segregated["match"]
        self.book_errors = self.segregated["book_errors"]
        self.debit_memo_amount = self.segregated["debit_memo_amount"]
        self.credit_memo_amount = self.segregated["credit_memo_amount"]
        self.outstanding_checks_amount = self.segregated["outstanding_checks_amount"]
        self.reversal_of_aje_amount = self.segregated["reversal_of_aje_amount"]
        self.stale_checks_amount = self.segregated["stale_checks_amount"]
        self.deposit_on_transit_amount = self.segregated["deposit_on_transit_amount"]
        self.in_bs_no_gl = self.segregated["in_bs_no_gl"]
        self.in_gl_no_bs = self.segregated["in_gl_no_bs"]
        self.unmatched_bs_cols = self.segregated["unmatched_bs_cols"]
        self.unmatched_gl_cols = self.segregated["unmatched_gl_cols"]
        self.recon_items_unmatched_bs_cols = self.segregated[
            "recon_items_unmatched_bs_cols"
        ]
        self.recon_items_unmatched_gl_cols = self.segregated[
            "recon_items_unmatched_gl_cols"
        ]
        self.multiple_entries_bs = []
        self.multiple_entries_gl = []

    "attach_multiple_matches"

    # No need for matched_id
    def match_keywords(self, match_fields: dict):
        """Matches the keywords that are equivalent to fields"""
        keyword_mappings = {
            "transaction_reference": "strd_trn_ref",
            "credit_amount": "strd_cr_amt",
            "debit_amount": "strd_dr_amt",
            "transaction_date": "strd_trn_date",
        }
        strd_field_names = {}
        for k, v in match_fields.items():
            qry_res = db["field_keywords"].find_one({"keywords": v})
            if qry_res and qry_res["field"] in keyword_mappings:
                strd_field_names[keyword_mappings[qry_res["field"]]] = v
        return strd_field_names

    def return_mappings(self):
        field_names = self.match_keywords(self.field_to_match)
        return (
            field_names["strd_cr_amt"],
            field_names["strd_dr_amt"],
            field_names["strd_trn_ref"],
        )

    def return_working_dfs(self, gl_df, bs_df):
        gl_df_inrange, bs_df_inrange = self.get_time_range(gl_df, bs_df)
        gl_df_before, bs_df_before, prev_approved_gl, prev_approved_bs = (
            self.get_time_range2(gl_df, bs_df)
        )

        total_dr_cr_amts = self.get_total_amount(gl_df_inrange, bs_df_inrange)
        gl_df_inrange = pd.concat([gl_df_inrange, gl_df_before], ignore_index=False)
        bs_df_inrange = pd.concat([bs_df_inrange, bs_df_before], ignore_index=False)
        # BACKUP
        # total_dr_cr_amts = self.get_total_amount(gl_df_inrange, bs_df_inrange)
        return (
            gl_df_inrange,
            bs_df_inrange,
            total_dr_cr_amts,
            prev_approved_gl,
            prev_approved_bs,
        )

    def return_merged_periods(self, gl_df, bs_df, gl_df_inrange, bs_df_inrange):
        gl_df_inrange, bs_df_inrange = self.get_time_range(gl_df, bs_df)
        gl_df_before, bs_df_before, prev_approved_gl, prev_approved_bs = (
            self.get_time_range2(gl_df, bs_df)
        )

        total_dr_cr_amts = self.get_total_amount(gl_df_inrange, bs_df_inrange)
        gl_df_inrange = pd.concat([gl_df_inrange, gl_df_before], ignore_index=False)
        bs_df_inrange = pd.concat([bs_df_inrange, bs_df_before], ignore_index=False)
        return (
            gl_df_inrange,
            bs_df_inrange,
            total_dr_cr_amts,
            gl_df_before,
            bs_df_before,
            prev_approved_gl,
            prev_approved_bs,
        )

    # Methods responsible for Recon level 1
    def segregation(self):
        # Retrieve general ledger and bank statement DataFrames from the instance.
        gl_df = self.general_ledger_df
        bs_df = self.bank_statement_df
        # Rename columns in the general ledger DataFrame using mapping provided in self.field_to_match.
        gl_df.rename(self.field_to_match, axis=1, inplace=True)

        # Get mappings for credit amount, debit amount, and transaction reference fields.
        strd_cr_amt, strd_dr_amt, strd_trn_ref = self.return_mappings()

        # Retrieve working DataFrames within the valid date range, total debit/credit amounts, and previously approved transactions.
        (
            gl_df_inrange,
            bs_df_inrange,
            total_dr_cr_amts,
            prev_approved_gl,
            prev_approved_bs,
        ) = self.return_working_dfs(gl_df, bs_df)

        # Merge period-specific DataFrames to include records from previous periods and update totals and approvals.
        (
            gl_df_inrange,
            bs_df_inrange,
            total_dr_cr_amts,
            gl_df_before,
            bs_df_before,
            prev_approved_gl,
            prev_approved_bs,
        ) = self.return_merged_periods(gl_df, bs_df, gl_df_inrange, bs_df_inrange)

        # Calculate matching records in the GL and BS DataFrames using their respective suffixes.
        gl_df_inrange = self.calculate_matches(gl_df_inrange, self.gl_suffix)
        bs_df_inrange = self.calculate_matches(bs_df_inrange, self.bs_suffix)

        # Define transaction references to be excluded.
        to_exclude_refs = ["DM", "CM", "ENC", "Tax", "INT"]
        # Identify rows in GL where the transaction reference is empty or matches an excluded value.
        gl_conds = (gl_df_inrange[strd_trn_ref] == "") | (gl_df_inrange[strd_trn_ref].isin(to_exclude_refs))
        # Identify rows in BS where the transaction reference is empty or matches an excluded value.
        bs_conds = (bs_df_inrange[strd_trn_ref] == "") | (bs_df_inrange[strd_trn_ref].isin(to_exclude_refs))

        # Separate out the excluded records from GL and BS DataFrames.
        gl_excluded_df = gl_df_inrange[gl_conds]
        bs_excluded_df = bs_df_inrange[bs_conds]

        # Select records to be merged by excluding those marked above.
        gl_df_to_merge = gl_df_inrange[~gl_conds]
        bs_df_to_merge = bs_df_inrange[~bs_conds]

        # Merge the GL and BS DataFrames on transaction reference using an outer join to preserve all records.
        merge_df = gl_df_to_merge.merge(
            bs_df_to_merge,
            how="outer",
            on=[strd_trn_ref],
            suffixes=(f"_{self.gl_suffix}", f"_{self.bs_suffix}"),
        )

        # Identify records present in GL but missing in BS by checking for empty transaction reference or null amounts in BS.
        in_gl_no_bs = merge_df[
            (merge_df[strd_trn_ref] == "")
            | (merge_df[f"{strd_cr_amt}_{self.bs_suffix}"].isnull())
            & (merge_df[f"{strd_dr_amt}_{self.bs_suffix}"].isnull())
        ]
        # Identify records present in BS but missing in GL by checking for empty transaction reference or null amounts in GL.
        in_bs_no_gl = merge_df[
            (merge_df[strd_trn_ref] == "")
            | (merge_df[f"{strd_cr_amt}_{self.gl_suffix}"].isnull())
            & (merge_df[f"{strd_dr_amt}_{self.gl_suffix}"].isnull())
        ]
        # Identify matched records where transaction reference exists and all required credit and debit fields are not null.
        match_df = merge_df[
            (merge_df[strd_trn_ref] != "")
            & (merge_df[f"{strd_cr_amt}_{self.gl_suffix}"].notna())
            & (merge_df[f"{strd_cr_amt}_{self.bs_suffix}"].notna())
            & (merge_df[f"{strd_dr_amt}_{self.gl_suffix}"].notna())
            & (merge_df[f"{strd_dr_amt}_{self.bs_suffix}"].notna())
        ]

        # Check variance in the matched records and separate out records for adjustments (AJE).
        match_df, for_aje_df = self.check_variance(match_df)

        # From the AJE DataFrame, select records that have matching IDs in both BS and GL.
        considered_matched_book_err = for_aje_df[
            for_aje_df["record_matched_id_bs"].notnull()
            & for_aje_df["record_matched_id_gl"].notnull()
        ]

        # Retrieve transaction references from the considered matched book errors.
        be_trn_refs = considered_matched_book_err.get("transaction_reference")
        if not be_trn_refs.empty:
            # Filter previously approved GL and BS records using the transaction references from book errors.
            filtered_gl_be_dfs = prev_approved_gl[
                prev_approved_gl["transaction_reference"].isin(be_trn_refs)
            ]
            filtered_bs_be_dfs = prev_approved_bs[
                prev_approved_bs["transaction_reference"].isin(be_trn_refs)
            ]

            # Group filtered GL records by transaction reference and sum credit and debit amounts.
            gl_sum_amts = (
                filtered_gl_be_dfs.groupby("transaction_reference")
                .agg(
                    gl_credit_sum=("credit_amount", "sum"),
                    gl_debit_sum=("debit_amount", "sum"),
                )
                .reset_index()
            )

            # Group filtered BS records by transaction reference and sum credit and debit amounts.
            bs_sum_amts = (
                filtered_bs_be_dfs.groupby("transaction_reference")
                .agg(
                    bs_credit_sum=("credit_amount", "sum"),
                    bs_debit_sum=("debit_amount", "sum"),
                )
                .reset_index()
            )

            # Calculate net GL amount (credit minus debit) and round to 2 decimal places.
            gl_sum_amts["gl_amount"] = (
                gl_sum_amts["gl_credit_sum"] - gl_sum_amts["gl_debit_sum"]
            ).round(2)
            # Calculate net BS amount (debit minus credit) and round to 2 decimal places.
            bs_sum_amts["bs_amount"] = (
                bs_sum_amts["bs_debit_sum"] - bs_sum_amts["bs_credit_sum"]
            ).round(2)

            # Loop through each grouped GL record and update the matched book error DataFrame and overall GL totals.
            for i, gl_amt in gl_sum_amts.iterrows():
                gl_be_matched_df = considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == gl_amt["transaction_reference"],
                    ["gl_amount", "credit_amount_gl", "debit_amount_gl"],
                ]
                fin_amt = gl_be_matched_df["gl_amount"] + gl_amt["gl_amount"]
                considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == gl_amt["transaction_reference"],
                    "credit_amount_gl",
                ] = gl_be_matched_df["credit_amount_gl"] + gl_amt["gl_credit_sum"]
                considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == gl_amt["transaction_reference"],
                    "debit_amount_gl",
                ] = gl_be_matched_df["debit_amount_gl"] + gl_amt["gl_debit_sum"]
                considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == gl_amt["transaction_reference"],
                    "gl_amount",
                ] = fin_amt.round(2)
                total_dr_cr_amts["gl_credit_total_amt"] += round(
                    gl_amt["gl_debit_sum"], 2
                )  # Inverse amount due to bank statement as basis.
                total_dr_cr_amts["gl_debit_total_amt"] += round(
                    gl_amt["gl_credit_sum"], 2
                )  # Inverse amount due to bank statement as basis.
                total_dr_cr_amts["gl_total_cash_movement"] += round(
                    gl_amt["gl_credit_sum"], 2
                ) + round(gl_amt["gl_debit_sum"], 2)

            # Loop through each grouped BS record and update the matched book error DataFrame and overall BS totals.
            for i, bs_amt in bs_sum_amts.iterrows():
                bs_be_matched_df = considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == bs_amt["transaction_reference"],
                    ["bs_amount", "credit_amount_bs", "debit_amount_bs"],
                ].values[0]
                fin_amt = bs_be_matched_df["bs_amount"] + bs_amt["bs_amount"]
                considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == bs_amt["transaction_reference"],
                    "credit_amount_bs",
                ] = bs_amt["credit_amount_bs"] + bs_amt["bs_credit_sum"]
                considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == bs_amt["transaction_reference"],
                    "debit_amount_bs",
                ] = bs_amt["debit_amount_bs"] + bs_amt["bs_debit_sum"]
                considered_matched_book_err.loc[
                    considered_matched_book_err["transaction_reference"]
                    == bs_amt["transaction_reference"],
                    "bs_amount",
                ] = fin_amt.round(2)
                total_dr_cr_amts["bs_credit_total_amt"] += round(
                    bs_amt["bs_credit_sum"], 2
                )
                total_dr_cr_amts["bs_debit_total_amt"] += round(
                    bs_amt["bs_debit_sum"], 2
                )
                total_dr_cr_amts["bs_total_cash_movement"] += round(
                    bs_amt["bs_credit_sum"], 2
                ) + round(bs_amt["bs_debit_sum"], 2)

            # Set variance to 0 for all considered matched book error records after adjustments.
            considered_matched_book_err["variance"] = 0

        # Combine the directly matched records with the adjusted book error records.
        match_df = pd.concat(
            [match_df, considered_matched_book_err], ignore_index=False
        )
        # Filter the AJE DataFrame to remove records that already have matching IDs in both BS and GL.
        for_aje_df = for_aje_df[
            ~(
                for_aje_df["record_matched_id_bs"].notnull()
                & for_aje_df["record_matched_id_gl"].notnull()
            )
        ]

        # Replace NaN values with empty strings in all key DataFrames.
        in_gl_no_bs.replace(np.nan, "", regex=True, inplace=True)
        in_bs_no_gl.replace(np.nan, "", regex=True, inplace=True)
        match_df.replace(np.nan, "", regex=True, inplace=True)
        for_aje_df.replace(np.nan, "", regex=True, inplace=True)

        # Determine final column sets for BS and GL based on original columns or columns with specific suffixes.
        bs_cols = [
            x for x in in_bs_no_gl.columns if ((x in bs_df.columns) or ("_bs" in x))
        ]
        gl_cols = [
            x for x in in_gl_no_bs.columns if ((x in gl_df.columns) or ("_gl" in x))
        ]
        
        # Set the final columns for unmatched GL and BS DataFrames.
        in_gl_no_bs = in_gl_no_bs[gl_cols]
        in_bs_no_gl = in_bs_no_gl[bs_cols]

        # Replace any remaining NaN values with the string "None" in AJE and unmatched DataFrames.
        for_aje_df.fillna("None", inplace=True)
        in_bs_no_gl.fillna("None", inplace=True)
        in_gl_no_bs.fillna("None", inplace=True)

        # Normalize transaction date in AJE DataFrame: try 'transaction_date', else fallback to 'trndate_gl'.
        try:
            for_aje_df = for_aje_df[for_aje_df["transaction_date"] != "None"]
            for_aje_df["trndate"] = for_aje_df["transaction_date"]
            for_aje_df["transaction_date"] = for_aje_df["trndate"]
        except:
            for_aje_df = for_aje_df[for_aje_df["trndate_gl"] != "None"]
            for_aje_df["trndate"] = for_aje_df["trndate_gl"]
            for_aje_df["transaction_date"] = for_aje_df["trndate"]

        # Normalize transaction date in unmatched BS DataFrame: try 'transaction_date', else fallback to 'transaction_date_bs'.
        try:
            in_bs_no_gl = in_bs_no_gl[in_bs_no_gl["transaction_date"] != "None"]
            in_bs_no_gl["trndate"] = in_bs_no_gl["transaction_date"]
            in_bs_no_gl["transaction_date"] = in_bs_no_gl["trndate"]
        except:
            in_bs_no_gl = in_bs_no_gl[in_bs_no_gl["transaction_date_bs"] != "None"]
            in_bs_no_gl["trndate"] = in_bs_no_gl["transaction_date_bs"]
            in_bs_no_gl["transaction_date"] = in_bs_no_gl["trndate"]

        # Normalize transaction date in unmatched GL DataFrame with multiple fallbacks.
        try:
            in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["transaction_date"] != "None"]
            in_gl_no_bs["trndate"] = in_gl_no_bs["transaction_date"]
            in_gl_no_bs["transaction_date"] = in_gl_no_bs["trndate"]
        except:
            try:
                in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["transaction_date_gl"] != "None"]
                in_gl_no_bs["trndate"] = in_gl_no_bs["transaction_date_gl"]
                in_gl_no_bs["transaction_date"] = in_gl_no_bs["trndate"]
            except:
                pass

        # Remove duplicate rows from the AJE and unmatched DataFrames.
        for_aje_df.drop_duplicates()
        in_gl_no_bs.drop_duplicates()
        in_bs_no_gl.drop_duplicates()
        # Ensure that unmatched GL records have a valid transaction date.
        in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["trndate"] != "None"]

        # Rename GL excluded DataFrame columns to match those in the unmatched GL DataFrame based on the suffix.
        for col1 in gl_excluded_df.columns:
            for col2 in in_gl_no_bs.columns:
                if (str(col1) in str(col2)) and (self.gl_suffix in str(col2)):
                    gl_excluded_df.rename(columns={str(col1): str(col2)}, inplace=True)
                    break

        # Rename BS excluded DataFrame columns to match those in the unmatched BS DataFrame based on the suffix.
        for col1 in bs_excluded_df.columns:
            for col2 in in_bs_no_gl.columns:
                if (str(col1) in str(col2)) and (self.bs_suffix in str(col2)):
                    bs_excluded_df.rename(columns={str(col1): str(col2)}, inplace=True)
                    break

        # Concatenate the excluded records back into the unmatched DataFrames.
        in_gl_no_bs = pd.concat([in_gl_no_bs, gl_excluded_df], ignore_index=True)
        in_bs_no_gl = pd.concat([in_bs_no_gl, bs_excluded_df], ignore_index=True)

        # Swap the debit and credit column names in the unmatched GL DataFrame using a temporary column name.
        in_gl_no_bs.rename(
            columns={
                f"{strd_dr_amt}_{self.gl_suffix}": "temp_name_dr",
                f"{strd_cr_amt}_{self.gl_suffix}": f"{strd_dr_amt}_{self.gl_suffix}",
            },
            inplace=True,
        )
        in_gl_no_bs.rename(
            columns={
                "temp_name_dr": f"{strd_cr_amt}_{self.gl_suffix}",
            },
            inplace=True,
        )

        # Calculation of bank reconciliation report output.
        # Compute outstanding checks and deposits in transit based on unmatched GL records.
        if not in_gl_no_bs.empty:
            outstanding_checks_amount = in_gl_no_bs[
                f"{strd_cr_amt}_{self.gl_suffix}"
            ].sum()
            deposit_on_transit_amount = in_gl_no_bs[
                f"{strd_dr_amt}_{self.gl_suffix}"
            ].sum()
        else:
            outstanding_checks_amount = 0
            deposit_on_transit_amount = 0

        # If there are unmatched BS records, compute reversal of AJE, debit memo, and credit memo amounts.
        if not in_bs_no_gl.empty:
            reversal_of_aje_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] != ""]
            reversal_of_aje_amount = (
                reversal_of_aje_df[f"{strd_cr_amt}_{self.bs_suffix}"].sum()
                - reversal_of_aje_df[f"{strd_dr_amt}_{self.bs_suffix}"].sum()
            )

            debit_memo_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] == ""]
            debit_memo_df = debit_memo_df[
                debit_memo_df[f"{strd_dr_amt}_{self.bs_suffix}"] > 0
            ]
            debit_memo_amount = (
                debit_memo_df[f"{strd_cr_amt}_{self.bs_suffix}"].sum()
                - debit_memo_df[f"{strd_dr_amt}_{self.bs_suffix}"].sum()
            )

            credit_memo_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] == ""]
            credit_memo_df = credit_memo_df[
                credit_memo_df[f"{strd_cr_amt}_{self.bs_suffix}"] > 0
            ]
            credit_memo_amount = (
                credit_memo_df[f"{strd_cr_amt}_{self.bs_suffix}"].sum()
                - credit_memo_df[f"{strd_dr_amt}_{self.bs_suffix}"].sum()
            )
        else:
            reversal_of_aje_amount = 0
            debit_memo_amount = 0
            credit_memo_amount = 0

        # Initialize stale checks amount as 0 (requires check date, which is not available in sample data).
        stale_checks_amount = 0

        # Select only specific columns from the AJE DataFrame for the final report.
        for_aje_df = for_aje_df[
            [
                "trndate",
                "transaction_date",
                strd_trn_ref,
                "ref_count_bs",
                "ref_count_gl",
                "gl_amount",
                "bs_amount",
                "variance",
            ]
        ]
        # Sum the variance column to calculate the total book errors amount.
        book_errors_amt = for_aje_df["variance"].sum()

        # IDK I don't use crude variable names definitely ask Cali - JRS
        fucking_gl, fucking_bs = ({}, {})
        if not gl_df_before.empty:
            fucking_gl = gl_df[
                (gl_df["approved"] == "approved") & (gl_df["is_matched"] == False)
            ].to_dict()
        if not bs_df_before.empty:
            fucking_bs = bs_df[
                (bs_df["approved"] == "approved") & (bs_df["is_matched"] == False)
            ].to_dict()

        # Mark all matched records as approved/matched in both GL and BS.
        match_df["is_matched_gl"] = True
        match_df["is_matched_bs"] = True

        # Return a dictionary containing all processed data to be use on BankRecon.py
        return {
            "original_gl": gl_df_inrange.to_dict("records"),
            "original_bs": bs_df_inrange.to_dict("records"),
            "match": match_df.to_dict("records"),
            "book_errors": for_aje_df.to_dict("records"),
            "in_bs_no_gl": in_bs_no_gl.to_dict(
                "records"
            ),  # Records in BS without an equivalent in GL.
            "in_gl_no_bs": in_gl_no_bs.to_dict(
                "records"
            ),  # Records in GL without an equivalent in BS.
            "total_dr_cr_amts": total_dr_cr_amts,
            "book_errors_amt": book_errors_amt,
            "debit_memo_amount": debit_memo_amount,
            "credit_memo_amount": credit_memo_amount,
            "outstanding_checks_amount": outstanding_checks_amount,
            "reversal_of_aje_amount": reversal_of_aje_amount,
            "stale_checks_amount": stale_checks_amount,
            "deposit_on_transit_amount": deposit_on_transit_amount,
            # "level_2_match_df": level_2_match,
            # "level_3_match_df": level_3_match,
            "unmatched_bs_cols": in_bs_no_gl.columns.to_list(),
            "unmatched_gl_cols": in_gl_no_bs.columns.to_list(),
            "recon_items_unmatched_bs_cols": fucking_gl,  # IDK ask Cali - JRS
            "recon_items_unmatched_gl_cols": fucking_bs,  # IDK ask Cali - JRS
        }

    def get_time_range(self, gl_df, bs_df):
        try:
            gl_df = gl_df[gl_df["trndate"].between(self.from_date, self.to_date)]
        except:
            gl_df = gl_df[
                gl_df["transaction_date"].between(self.from_date, self.to_date)
            ]
        bs_df = bs_df[bs_df["transaction_date"].between(self.from_date, self.to_date)]
        return gl_df, bs_df

    def get_time_range2(self, gl_df, bs_df):
        try:
            gl_df = gl_df.loc[gl_df["trndate"] < self.from_date]
            gl_df = gl_df[gl_df["approved"] == "approved"]
            prev_approved_gl = gl_df
            gl_df = gl_df[gl_df["is_matched"] == False]
        except:
            gl_df = gl_df.loc[gl_df["transaction_date"] < self.from_date]
            gl_df = gl_df[gl_df["approved"] == "approved"]
            prev_approved_gl = gl_df
            gl_df = gl_df[gl_df["is_matched"] == False]
        bs_df = bs_df.loc[bs_df["transaction_date"] < self.from_date]
        bs_df = bs_df[bs_df["approved"] == "approved"]
        prev_approved_bs = bs_df
        bs_df = bs_df[bs_df["is_matched"] == False]

        return gl_df, bs_df, prev_approved_gl, prev_approved_bs

    def get_time_range_before(self, gl_df, bs_df):
        gl_columns = list(gl_df.columns)
        df_columns = list(bs_df.columns)
        init_gl = pd.DataFrame(columns=gl_columns)
        init_bs = pd.DataFrame(columns=df_columns)

        cutoff_gl_df = pd.to_datetime(self.from_date, format="%Y-%m-%d")
        cutoff_gl_df = str(pd.to_datetime(cutoff_gl_df, format="%Y-%m-%d"))
        gl_df = gl_df.loc[gl_df["trndate"] < cutoff_gl_df]
        bs_df = bs_df.loc[bs_df["transaction_date"] < cutoff_gl_df]
        if not gl_df.empty:
            gl_df = gl_df[gl_df["approved"] == "approved"]
            gl_df = gl_df[gl_df["is_matched"] == False]

        if not bs_df.empty:
            bs_df = bs_df[bs_df["approved"] == "approved"]
            bs_df = bs_df[bs_df["is_matched"] == False]

        if (not bs_df.empty) or not (gl_df.empty):
            if bs_df.empty:
                return gl_df
            else:
                return gl_df, bs_df
        return init_bs, init_gl

    def get_total_amount(self, gl_df, bs_df):
        """
        rename to values for pandas merge
        other_03 = transaction_reference
        dr_amt = credit_amount
        cr_amt = debit_amount
        """
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = (
            strd_field_names["strd_cr_amt"],
            strd_field_names["strd_dr_amt"],
        )

        gl_credit_total_amt = -(gl_df[strd_dr_amt].sum())
        gl_debit_total_amt = gl_df[strd_cr_amt].sum()

        bs_credit_total_amt = bs_df[strd_cr_amt].sum()
        bs_debit_total_amt = -(bs_df[strd_dr_amt].sum())

        gl_total_cash_movement = gl_credit_total_amt + gl_debit_total_amt
        bs_total_cash_movement = bs_credit_total_amt + bs_debit_total_amt

        return {
            "gl_credit_total_amt": gl_credit_total_amt,
            "gl_debit_total_amt": gl_debit_total_amt,
            "bs_credit_total_amt": bs_credit_total_amt,
            "bs_debit_total_amt": bs_debit_total_amt,
            "gl_total_cash_movement": gl_total_cash_movement,
            "bs_total_cash_movement": bs_total_cash_movement,
        }

    def calculate_matches(self, df, gl_or_pb):
        """Calculates the net amount of debit and credit amount of GL and BS.

        This method computes the net amount to match transactions (1:N or N:N).
        """
        # gets the standard keywords of fields to match
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt = strd_field_names["strd_cr_amt"]
        strd_dr_amt = strd_field_names["strd_dr_amt"]
        strd_trn_ref = strd_field_names["strd_trn_ref"]

        # counts all trn_ref of a transaction in the df
        df["ref_count"] = df[strd_trn_ref].map(df[strd_trn_ref].value_counts())
        multi_entries = df[df["ref_count"] > 1]
        if gl_or_pb == "gl":
            self.multiple_entries_gl = multi_entries
        else:
            self.multiple_entries_bs = multi_entries
        fieldnames = list(df.columns)
        d_fields = {}
        for k in fieldnames:
            if k == strd_dr_amt or k == strd_cr_amt:
                d_fields[k] = "sum"
            else:
                d_fields[k] = "first"
            col = d_fields
            
        to_exclude_refs = ["DM", "CM", "ENC", "Tax", "INT"]
        
        empty_trn_ref = df[df[strd_trn_ref] == ""]
        excluded_trn_ref = df[df[strd_trn_ref].isin(to_exclude_refs)]
        df_new = df[~(df[strd_trn_ref].isin(to_exclude_refs)) & (df[strd_trn_ref] != "")]
        df_new = (
            df_new.groupby(strd_trn_ref, as_index=False)
            .aggregate(col)
            .reindex(columns=df_new.columns)
        )
        df_new = pd.concat([excluded_trn_ref, df_new])
        df_new = pd.concat([empty_trn_ref, df_new])
        return df_new

    def get_multiple_entries_breakdown(self, trans_ref, gl_or_bs):
        df = self.multiple_entries_gl if gl_or_bs == "gl" else self.multiple_entries_bs
        return df[df[self.strd_trn_ref] == trans_ref].to_dict("records")

    def check_variance(self, df):
        """
        1. gl_amount =  debit_amount_gl - credit_amount_gl
        2. bs_amount = credit_amount_bs - debit_amount_bs
        3. result = bs_amount - gl_amount
        4. variance => if result != 0
        """
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = (
            strd_field_names["strd_cr_amt"],
            strd_field_names["strd_dr_amt"],
        )
        # pprint(strd_cr_amt)
        debit_amount_gl = df[f"{strd_cr_amt}_{self.gl_suffix}"]
        credit_amount_gl = df[f"{strd_dr_amt}_{self.gl_suffix}"]
        # debit_amount_gl = df[f'{strd_cr_amt}']
        # credit_amount_gl = df[f'{strd_dr_amt}']
        df["gl_amount"] = debit_amount_gl - credit_amount_gl
        df["bs_amount"] = (
            df[f"{strd_cr_amt}_{self.bs_suffix}"]
            - df[f"{strd_dr_amt}_{self.bs_suffix}"]
        )
        # df['bs_amount'] = df[f'{strd_cr_amt}'] - df[f'{strd_dr_amt}']
        df = df.astype({"bs_amount": float, "gl_amount": float})
        df["variance"] = df["bs_amount"] - df["gl_amount"]
        df = df.astype({"variance": float})
        match_df = df[
            df["variance"].round(2) == 0
        ]  # | (df["variance"] <= float(0.0009))
        for_aje_df = df[df["variance"].round(2) != 0]
        return match_df, for_aje_df
