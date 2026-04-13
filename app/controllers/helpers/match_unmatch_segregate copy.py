"""

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




class Segregator:
    """Segregates match and unmatch list of 2 collections.

    Handles the merging, matching, and calculating the amounts of the
    collections using pandas.
    """
    def __init__(self,
                 general_ledger_df,
                 bank_statement_df,
                 filtered_df,
                 to_match: dict,
                 from_date: datetime,
                 to_date: datetime) -> None:
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
        self.general_ledger_df = general_ledger_df
        self.bank_statement_df = bank_statement_df
        self.filtered_df = filtered_df
        self.segregated = self.segregation()
        self.total_dr_cr_amts = self.segregated["total_dr_cr_amts"]
        self.book_errors_amt = self.segregated["book_errors_amt"]
        self.match = self.segregated["match"]
        self.book_errors = self.segregated['book_errors']
        self.debit_memo_amount = self.segregated['debit_memo_amount']
        self.credit_memo_amount = self.segregated['credit_memo_amount']
        self.outstanding_checks_amount = self.segregated['outstanding_checks_amount']
        self.reversal_of_aje_amount = self.segregated['reversal_of_aje_amount']
        self.stale_checks_amount = self.segregated['stale_checks_amount']
        self.in_bs_no_gl = self.segregated["in_bs_no_gl"]
        self.in_gl_no_bs = self.segregated["in_gl_no_bs"]
        self.unmatched_bs_cols = self.segregated["unmatched_bs_cols"]
        self.unmatched_gl_cols = self.segregated["unmatched_gl_cols"]
        self.multiple_entries_bs = []
        self.multiple_entries_gl = []


    def attach_multiple_matches(self, df):
        """Attaching to df the matched multiple entries and creates breakdown.
        """
        df["mult"] = None
        mult_ent_transref_gl = list(self.multiple_entries_gl["transaction_reference"].unique())
        mult_ent_transref_bs = list(self.multiple_entries_bs["transaction_reference"].unique())
        for ref in mult_ent_transref_gl:
            ref_data = self.get_multiple_entries_breakdown(ref, "gl")
            df.loc[df.transaction_reference == ref, "mult"] = str(ref_data) if ref_data else None
        for ref2 in mult_ent_transref_bs:

            ref2_data = str(self.get_multiple_entries_breakdown(ref2, "bs"))
            df.loc[df.transaction_reference == ref2, 'mult'] = str(ref2_data) if ref2_data else None
        return df


    def match_keywords(self, match_fields: dict):
        """
        Matches the keywords that are equivalent to fields
        """
        strd_field_names = {}

        for k, v in match_fields.items():
            qry_res = db["field_keywords"].find_one({"keywords": v})
            if qry_res:
                strd_key = qry_res["field"]
                if strd_key == "transaction_reference":
                    strd_field_names["strd_trn_ref"] = v
                elif strd_key == "credit_amount":
                    strd_field_names["strd_cr_amt"] = v
                elif strd_key == "debit_amount":
                    strd_field_names["strd_dr_amt"] = v
                elif strd_key == "transaction_date":
                    strd_field_names["strd_trn_date"] = v
        return strd_field_names

    #experimental
    def segregation_2(self):
        gl_df = self.general_ledger_df
        bs_df = self.bank_statement_df
        gl_df.rename(self.field_to_match, axis=1, inplace=True)
        '''
            NOTE: breakdown of amounts
            1. fetch matched main BS and GL
            2. Create a list on match in-daterange and match out-daterange (to determine if they have match in whole records)
            3. Get match in-daterange and unmatch in-daterange (done)
            4. if unmatch in-daterange not exist in match out-daterange => unmatch in-daterange does not exist in whole record (For create of new entries)
            5. After uploading new entries in GL and BS, recompute the computations.
            6 For Final Post (Save to Main GL and BS and update check monitoring)
        '''
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt, strd_trn_ref = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"], strd_field_names["strd_trn_ref"]
        gl_df_inrange, bs_df_inrange = self.get_time_range(gl_df, bs_df)
        total_dr_cr_amts = self.get_total_amount(gl_df_inrange, bs_df_inrange)
        gl_df_inrange = self.calculate_matches(gl_df_inrange, self.gl_suffix)
        bs_df_inrange = self.calculate_matches(bs_df_inrange, self.bs_suffix)
        pprint(gl_df_inrange.columns)
        pprint(bs_df_inrange.columns)
        merge_df = pd.merge(
            gl_df_inrange,
            bs_df_inrange,
            on=[strd_trn_ref,strd_cr_amt,strd_dr_amt],
            how="outer",
            indicator=True,
            suffixes=(f'_{self.gl_suffix}', f'_{self.bs_suffix}')
        )

        in_gl_no_bs = merge_df[merge_df["_merge"] == "left_only"].dropna(
            axis="columns",
            how="all"
        )
        in_bs_no_gl = merge_df[merge_df["_merge"] == "right_only"].dropna(
            axis="columns",
            how="all"
        )
        match_df = merge_df[merge_df["_merge"] == "both"]
        match_df, for_aje_df = self.check_variance(match_df)
        outstanding_checks_amount = abs(in_gl_no_bs[f'{strd_cr_amt}_{self.gl_suffix}'].sum() - in_gl_no_bs[f'{strd_dr_amt}_{self.gl_suffix}'].sum())
        reversal_of_aje_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] != '']
        reversal_of_aje_amount = reversal_of_aje_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - reversal_of_aje_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()
        stale_checks_amount = 0
        debit_memo_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] == '']
        debit_memo_df = debit_memo_df[debit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'] > 0]
        debit_memo_amount = debit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - debit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()
        credit_memo_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] == '']
        credit_memo_df = credit_memo_df[credit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'] > 0]
        credit_memo_amount = credit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - credit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()
        in_gl_no_bs.replace(np.nan, '', regex=True, inplace=True)
        in_bs_no_gl.replace(np.nan, '', regex=True, inplace=True)
        match_df.replace(np.nan, '', regex=True, inplace=True)
        for_aje_df.replace(np.nan, '', regex=True, inplace=True)
        bs_cols = [x for x in in_bs_no_gl.columns if ((x in bs_df.columns) or ("_bs" in x))]
        gl_cols = [x for x in in_gl_no_bs.columns if ((x in gl_df.columns) or ("_gl" in x))]
        in_gl_no_bs = in_gl_no_bs[gl_cols]
        in_bs_no_gl = in_bs_no_gl[bs_cols]
        for_aje_df.fillna("None", inplace=True)
        in_bs_no_gl.fillna("None", inplace=True)
        in_gl_no_bs.fillna("None", inplace=True)
        for_aje_df = for_aje_df[for_aje_df["transaction_date"] != "None"]
        in_bs_no_gl = in_bs_no_gl[in_bs_no_gl["transaction_date"] != "None"]
        in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["trndate"] != "None"]
        for_aje_df = for_aje_df[[
            "trndate",
            "transaction_date",
            strd_trn_ref,
            "ref_count_bs",
            "ref_count_gl",
            "gl_amount",
            "bs_amount",
            "variance"
        ]]
        book_errors_amt = for_aje_df["variance"].sum()

        return {
            "match" : match_df.to_dict('records'),
            "book_errors" : for_aje_df.to_dict("records"),
            "in_bs_no_gl" : in_bs_no_gl.to_dict('records'),
            "in_gl_no_bs" : in_gl_no_bs.to_dict('records'),
            "total_dr_cr_amts": total_dr_cr_amts,
            "book_errors_amt": book_errors_amt,
            "debit_memo_amount": debit_memo_amount,
            "credit_memo_amount": credit_memo_amount,
            "outstanding_checks_amount": outstanding_checks_amount,
            "reversal_of_aje_amount": reversal_of_aje_amount,
            "stale_checks_amount": stale_checks_amount,
        }


    def segregation(self):
        """
            FIXME
            ==========
            in rename part
            GL
            cr_amt => debit_amount_gl
            dr_amt => credit_amount_gl
        """

        gl_df = self.general_ledger_df
        bs_df = self.bank_statement_df
        filtered_df = self.filtered_df

        gl_df.rename(self.field_to_match, axis=1, inplace=True)
        '''
            NOTE: breakdown of amounts
            1. fetch matched main BS and GL
            2. Create a list on match in-daterange and match out-daterange (to determine if they have match in whole records)
            3. Get match in-daterange and unmatch in-daterange (done)
            4. if unmatch in-daterange not exist in match out-daterange => unmatch in-daterange does not exist in whole record (For create of new entries)
            5. After uploading new entries in GL and BS, recompute the computations.
            6 For Final Post (Save to Main GL and BS and update check monitoring)
        '''
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt, strd_trn_ref = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"], strd_field_names["strd_trn_ref"]
        gl_df_inrange, bs_df_inrange = self.get_time_range(gl_df, bs_df)

        total_dr_cr_amts = self.get_total_amount(gl_df_inrange, bs_df_inrange)

        gl_df_inrange = self.calculate_matches(gl_df_inrange, self.gl_suffix)
        bs_df_inrange = self.calculate_matches(bs_df_inrange, self.bs_suffix)

        gl_excluded_df = gl_df_inrange[(gl_df_inrange[strd_trn_ref] == "") | (gl_df_inrange[strd_trn_ref] == "DM")]
        bs_excluded_df = bs_df_inrange[(bs_df_inrange[strd_trn_ref] == "") | (bs_df_inrange[strd_trn_ref] == "DM")]

        gl_df_to_merge = gl_df_inrange[(gl_df_inrange[strd_trn_ref] != "") & (gl_df_inrange[strd_trn_ref] != "DM")]
        bs_df_to_merge = bs_df_inrange[(bs_df_inrange[strd_trn_ref] != "") & (bs_df_inrange[strd_trn_ref] != "DM")]

        merge_df = gl_df_to_merge.merge(
                               bs_df_to_merge,
                               how = 'outer',
                               on = [strd_trn_ref],
                               suffixes=(f'_{self.gl_suffix}', f'_{self.bs_suffix}'))
        in_gl_no_bs = merge_df[
                            (merge_df[strd_trn_ref] == "") |
                            (merge_df[f'{strd_cr_amt}_{self.bs_suffix}'].isnull()) &
                            (merge_df[f'{strd_dr_amt}_{self.bs_suffix}'].isnull())]
        in_bs_no_gl = merge_df[
                            (merge_df[strd_trn_ref] == "") |
                            (merge_df[f'{strd_cr_amt}_{self.gl_suffix}'].isnull()) &
                            (merge_df[f'{strd_dr_amt}_{self.gl_suffix}'].isnull())]
        match_df = merge_df[
                    (merge_df[strd_trn_ref] != "") &
                    (merge_df[f'{strd_cr_amt}_{self.gl_suffix}'].notna()) &
                    (merge_df[f'{strd_cr_amt}_{self.bs_suffix}'].notna()) &
                    (merge_df[f'{strd_dr_amt}_{self.gl_suffix}'].notna()) &
                    (merge_df[f'{strd_dr_amt}_{self.bs_suffix}'].notna())]

        match_df, for_aje_df = self.check_variance(match_df)


        in_gl_no_bs.replace(np.nan, '', regex=True, inplace=True)
        in_bs_no_gl.replace(np.nan, '', regex=True, inplace=True)
        match_df.replace(np.nan, '', regex=True, inplace=True)
        for_aje_df.replace(np.nan, '', regex=True, inplace=True)


        bs_cols = [x for x in in_bs_no_gl.columns if ((x in bs_df.columns) or ("_bs" in x))]
        gl_cols = [x for x in in_gl_no_bs.columns if ((x in gl_df.columns) or ("_gl" in x))]

        in_gl_no_bs = in_gl_no_bs[gl_cols]
        in_bs_no_gl = in_bs_no_gl[bs_cols]

        for_aje_df.fillna("None", inplace=True)
        in_bs_no_gl.fillna("None", inplace=True)
        in_gl_no_bs.fillna("None", inplace=True)

        for_aje_df = for_aje_df[for_aje_df["transaction_date"] != "None"]
        in_bs_no_gl = in_bs_no_gl[in_bs_no_gl["transaction_date"] != "None"]
        in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["trndate"] != "None"]

        for col1 in gl_excluded_df.columns:
            for col2 in in_gl_no_bs.columns:
                if (str(col1) in str(col2)) and (self.gl_suffix in str(col2)):
                    gl_excluded_df.rename(columns = {str(col1):str(col2)}, inplace = True)
                    break

        for col1 in bs_excluded_df.columns:
            for col2 in in_bs_no_gl.columns:
                if (str(col1) in str(col2)) and (self.bs_suffix in str(col2)):
                    bs_excluded_df.rename(columns = {str(col1):str(col2)}, inplace = True)
                    break

        in_gl_no_bs = pd.concat([in_gl_no_bs, gl_excluded_df], ignore_index=True)
        in_bs_no_gl = pd.concat([in_bs_no_gl, bs_excluded_df], ignore_index=True)

        in_gl_no_bs.rename(
            columns={
                f'{strd_dr_amt}_{self.gl_suffix}': 'temp_name_dr',
                f'{strd_cr_amt}_{self.gl_suffix}': f'{strd_dr_amt}_{self.gl_suffix}'
            },
            inplace = True
        )
        in_gl_no_bs.rename(
            columns={
                'temp_name_dr': f'{strd_cr_amt}_{self.gl_suffix}',
            },
            inplace = True
        )

        #pprint(in_gl_no_bs.columns)

        outstanding_checks_amount = abs(in_gl_no_bs[f'{strd_cr_amt}_{self.gl_suffix}'].sum() - in_gl_no_bs[f'{strd_dr_amt}_{self.gl_suffix}'].sum())
        reversal_of_aje_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] != '']
        reversal_of_aje_amount = reversal_of_aje_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - reversal_of_aje_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()
        stale_checks_amount = 0

        debit_memo_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] == '']
        debit_memo_df = debit_memo_df[debit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'] > 0]
        debit_memo_amount = debit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - debit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()

        credit_memo_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] == '']
        credit_memo_df = credit_memo_df[credit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'] > 0]
        credit_memo_amount = credit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - credit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()

        for_aje_df = for_aje_df[[
            "trndate",
            "transaction_date",
            strd_trn_ref,
            "ref_count_bs",
            "ref_count_gl",
            "gl_amount",
            "bs_amount",
            "variance"
        ]]
        book_errors_amt = for_aje_df["variance"].sum()

        level_2_match = {}
        level_3_match = {}

        return {
            "match" : match_df.to_dict('records'),
            "book_errors" : for_aje_df.to_dict("records"),
            "in_bs_no_gl" : in_bs_no_gl.to_dict('records'),
            "in_gl_no_bs" : in_gl_no_bs.to_dict('records'),
            "total_dr_cr_amts": total_dr_cr_amts,
            "book_errors_amt": book_errors_amt,
            "debit_memo_amount": debit_memo_amount,
            "credit_memo_amount": credit_memo_amount,
            "outstanding_checks_amount": outstanding_checks_amount,
            "reversal_of_aje_amount": reversal_of_aje_amount,
            "stale_checks_amount": stale_checks_amount,
            "unmatched_bs_cols": in_bs_no_gl.columns.to_list(),
            "unmatched_gl_cols": in_gl_no_bs.columns.to_list(),
        }


    def get_time_range(self, gl_df, bs_df):
        gl_df = gl_df[gl_df['trndate'].between(self.from_date, self.to_date)]
        bs_df = bs_df[bs_df['transaction_date'].between(self.from_date, self.to_date)]
        return gl_df, bs_df


    def get_total_amount(self, gl_df, bs_df):
        '''
            rename to values for pandas merge
            other_03 = transaction_reference
            dr_amt = credit_amount
            cr_amt = debit_amount
        '''
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"]

        gl_credit_total_amt =  -(gl_df[strd_dr_amt].sum())
        gl_debit_total_amt = gl_df[strd_cr_amt].sum()

        bs_credit_total_amt = bs_df[strd_cr_amt].sum()
        bs_debit_total_amt = -(bs_df[strd_dr_amt].sum())

        gl_total_cash_movement = gl_credit_total_amt + gl_debit_total_amt
        bs_total_cash_movement = bs_credit_total_amt + bs_debit_total_amt

        return {
            "gl_credit_total_amt" : gl_credit_total_amt,
            "gl_debit_total_amt" : gl_debit_total_amt,
            "bs_credit_total_amt" : bs_credit_total_amt,
            "bs_debit_total_amt" : bs_debit_total_amt,
            "gl_total_cash_movement" : gl_total_cash_movement,
            "bs_total_cash_movement" : bs_total_cash_movement
        }


    def calculate_matches(self, df, gl_or_pb):
        """Calculates the net amount of debit and credit amount of GL and BS.

        This method computes the net amount to match transactions (1:N or N:N).
        """

        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt = strd_field_names["strd_cr_amt"]
        strd_dr_amt = strd_field_names["strd_dr_amt"]
        strd_trn_ref = strd_field_names["strd_trn_ref"]


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
                d_fields[k] = 'sum'
            else:
                d_fields[k] = 'first'
            col = d_fields
        empty_trn_ref = df[df[strd_trn_ref] == ""]
        dm_trn_ref = df[df[strd_trn_ref] == "DM"]
        df_new = df[(df[strd_trn_ref] != "DM") & (df[strd_trn_ref] != "")]
        df_new = df_new.groupby(strd_trn_ref, as_index=False).aggregate(
            col).reindex(columns=df_new.columns)
        df_new = pd.concat([dm_trn_ref, df_new])
        df_new = pd.concat([empty_trn_ref, df_new])
        return df_new

    def get_multiple_entries_breakdown(self, trans_ref, gl_or_bs):
        df = (self.multiple_entries_gl if gl_or_bs == "gl" else self.multiple_entries_bs)
        return df[df[self.strd_trn_ref] == trans_ref].to_dict("records")

    def check_variance(self, df):
        '''
        1. gl_amount =  debit_amount_gl - credit_amount_gl
        2. bs_amount = credit_amount_bs - debit_amount_bs
        3. result = bs_amount - gl_amount
        4. variance => if result != 0
        '''
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"]
        debit_amount_gl = df[f'{strd_cr_amt}_{self.gl_suffix}']
        credit_amount_gl = df[f'{strd_dr_amt}_{self.gl_suffix}']
        df['gl_amount'] = debit_amount_gl - credit_amount_gl
        df['bs_amount'] = df[f'{strd_cr_amt}_{self.bs_suffix}'] - df[f'{strd_dr_amt}_{self.bs_suffix}']
        df = df.astype({"bs_amount": float, "gl_amount": float})
        df['variance'] = df["bs_amount"] - df["gl_amount"]
        df = df.astype({"variance": float})
        match_df = df[df["variance"].round(2) == 0]
        for_aje_df = df[df["variance"].round(2) != 0]
        return match_df, for_aje_df
