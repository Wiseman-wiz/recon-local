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
        self.general_ledger_df = general_ledger_df
        self.bank_statement_df = bank_statement_df
        self.filtered_df = filtered_df
        
        
        self.segregated = self.segregation()
        self.total_dr_cr_amts = self.segregated["total_dr_cr_amts"]
        self.book_errors_amt = self.segregated["book_errors_amt"]
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
        self.multiple_entries_bs = []
        self.multiple_entries_gl = []

    
    def recurssive_dict(self, big_dick_energy):
        for k, v in big_dick_energy.items():
            if isinstance(v, dict):
                big_dick_energy[k] = self.recurssive_dict(v)
            if isinstance(v, pd.NaT):
                big_dick_energy[k] = None
        yield big_dick_energy

                

    def attach_multiple_matches(self, df):
        """Attaching to df the matched multiple entries and creates breakdown."""
        df["mult"] = None
        mult_ent_transref_gl = list(
            self.multiple_entries_gl["transaction_reference"].unique()
        )
        mult_ent_transref_bs = list(
            self.multiple_entries_bs["transaction_reference"].unique()
        )
        
        for ref in mult_ent_transref_gl:
            ref_data = self.get_multiple_entries_breakdown(ref, "gl")
            df.loc[df.transaction_reference == ref, "mult"] = (
                str(ref_data) if ref_data else None
            )
        for ref2 in mult_ent_transref_bs:
            # for many to many we need to append to string formed list of dictionaries
            ref2_data = str(self.get_multiple_entries_breakdown(ref2, "bs"))
            df.loc[df.transaction_reference == ref2, "mult"] = (
                str(ref2_data) if ref2_data else None
            )
        return df

    def match_keywords(self, match_fields: dict):
        """Matches the keywords that are equivalent to fields"""
        strd_field_names = {}
        # strd_cr_amt, strd_dr_amt, strd_trn_ref, strd_trn_date = "", "", "", ""
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
        # pprint(f"standard value for credit_amount => {strd_cr_amt}")
        # pprint(f"standard value for debit_amount => {strd_dr_amt}")
        # pprint(f"standard value for trn_ref => {strd_trn_ref}")
        # strd_cr_amt, strd_dr_amt, strd_trn_ref, strd_trn_date
        return strd_field_names
    
    def re_mapping(self,dataframe,map_for):
        '''
        mappings = {"trn_ref" : ["transaction_references","trnreference"],
        "cr_amt" : ["cr_amt","cramt","credit_amount"],
        "dr_amt":["dr_amt","dramt","debit_amount"]}
        
        '''

        mappings = {"trn_ref" : ["transaction_references","trnreference","trnref","trn_ref","transaction_references"],
        "cr_amt" : ["cr_amt","cramt","credit_amount"],
        "dr_amt":["dr_amt","dramt","debit_amount"]}
        
        
        df_cols = list(dataframe.columns)

        for k in mappings.get(map_for):
            if k in df_cols:
                return(k)



    def segregation(self):
        # start = timeit.default_timer()
        gl_df = self.general_ledger_df
        bs_df = self.bank_statement_df
        gl_df_2 = self.general_ledger_df
        bs_df_2 = self.bank_statement_df
        print(gl_df.columns)

        # renames the fields of GL df to have uniform field names when merging
        self.field_to_match["other_03"] = "trnref"
        gl_df.rename(self.field_to_match, axis=1, inplace=True)

        strd_field_names = self.match_keywords(self.field_to_match)

        strd_field_names["strd_trn_ref"] = "trnref"
        strd_cr_amt, strd_dr_amt, strd_trn_ref = (
            "credit_amount",
           "debit_amount",
           "transaction_reference",
        )

        gl_df_inrange, bs_df_inrange = self.get_time_range(gl_df, bs_df)
        gl_df_before, bs_df_before = self.get_time_range_before(gl_df_2, bs_df_2)

        gl_df_inrange = pd.concat([gl_df_inrange, gl_df_before])
        bs_df_inrange = pd.concat([bs_df_inrange, bs_df_before])

        total_dr_cr_amts = self.get_total_amount(gl_df_inrange, bs_df_inrange)

        gl_df_inrange = self.calculate_matches(gl_df_inrange, self.gl_suffix)
        bs_df_inrange = self.calculate_matches(bs_df_inrange, self.bs_suffix)

        strd_trn_ref = self.re_mapping(strd_field_names, "trn_ref")
        strd_cr_amt = self.re_mapping(strd_field_names, "dr_amt")
        strd_dr_amt = self.re_mapping(strd_field_names, "cr_amt")

        gl_excluded_df = gl_df_inrange[
            (gl_df_inrange[strd_trn_ref] == "") | (gl_df_inrange[strd_trn_ref] == "DM")
        ]
        bs_excluded_df = bs_df_inrange[
            (bs_df_inrange[strd_trn_ref] == "") | (bs_df_inrange[strd_trn_ref] == "DM")
        ]

        gl_df_to_merge = gl_df_inrange[
            (gl_df_inrange[strd_trn_ref] != "") & (gl_df_inrange[strd_trn_ref] != "DM")
        ]
        bs_df_to_merge = bs_df_inrange[
            (bs_df_inrange[strd_trn_ref] != "") & (bs_df_inrange[strd_trn_ref] != "DM")
        ]
        # pprint(gl_df_to_merge)
        # pprint("=============================")
        # pprint(bs_excluded_df.to_dict('records'))
        print(gl_df_to_merge.columns)
        merge_df = gl_df_to_merge.merge(
            bs_df_to_merge,
            how="outer",
            on=[strd_trn_ref],
            suffixes=(f"_{self.gl_suffix}", f"_{self.bs_suffix}"),
        )
        in_gl_no_bs = merge_df[
            (merge_df[strd_trn_ref] == "")
            | (merge_df[f"{strd_cr_amt}_{self.bs_suffix}"].isnull())
            & (merge_df[f"{strd_dr_amt}_{self.bs_suffix}"].isnull())
        ]
        in_bs_no_gl = merge_df[
            (merge_df[strd_trn_ref] == "")
            | (merge_df[f"{strd_cr_amt}_{self.gl_suffix}"].isnull())
            & (merge_df[f"{strd_dr_amt}_{self.gl_suffix}"].isnull())
        ]
        match_df = merge_df[
            (merge_df[strd_trn_ref] != "")
            & (merge_df[f"{strd_cr_amt}_{self.gl_suffix}"].notna())
            & (merge_df[f"{strd_cr_amt}_{self.bs_suffix}"].notna())
            & (merge_df[f"{strd_dr_amt}_{self.gl_suffix}"].notna())
            & (merge_df[f"{strd_dr_amt}_{self.bs_suffix}"].notna())
        ]

        match_df, for_aje_df = self.check_variance(match_df)

        #  replacing empty or NaN values to empty string
        in_gl_no_bs.replace(np.nan, "", regex=True, inplace=True)
        in_bs_no_gl.replace(np.nan, "", regex=True, inplace=True)
        match_df.replace(np.nan, "", regex=True, inplace=True)
        for_aje_df.replace(np.nan, "", regex=True, inplace=True)

        #  getting columns from bs cols if not in gl cols - vice versa
        bs_cols = [
            x for x in in_bs_no_gl.columns if ((x in bs_df.columns) or ("_bs" in x))
        ]
        gl_cols = [
            x for x in in_gl_no_bs.columns if ((x in gl_df.columns) or ("_gl" in x))
        ]
        #  end

        #  setting final cols of gl and bs
        in_gl_no_bs = in_gl_no_bs[gl_cols]
        in_bs_no_gl = in_bs_no_gl[bs_cols]
        #  end

        for_aje_df.fillna("None", inplace=True)
        in_bs_no_gl.fillna("None", inplace=True)
        in_gl_no_bs.fillna("None", inplace=True)

        for_aje_df = for_aje_df[for_aje_df["transaction_date"] != "None"]
        in_bs_no_gl = in_bs_no_gl[in_bs_no_gl["transaction_date"] != "None"]
        in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["trndate_gl"] != "None"]

        for col1 in gl_excluded_df.columns:
            for col2 in in_gl_no_bs.columns:
                if (str(col1) in str(col2)) and (self.gl_suffix in str(col2)):
                    gl_excluded_df.rename(columns={str(col1): str(col2)}, inplace=True)
                    break

        for col1 in bs_excluded_df.columns:
            for col2 in in_bs_no_gl.columns:
                if (str(col1) in str(col2)) and (self.bs_suffix in str(col2)):
                    bs_excluded_df.rename(columns={str(col1): str(col2)}, inplace=True)
                    break

        in_gl_no_bs = pd.concat([in_gl_no_bs, gl_excluded_df], ignore_index=True)
        in_bs_no_gl = pd.concat([in_bs_no_gl, bs_excluded_df], ignore_index=True)

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

        #  calculation of bank recon report output
        # outstanding_checks_df = in_gl_no_bs[in_gl_no_bs[strd_trn_ref] != 'DM']
        outstanding_checks_amount = in_gl_no_bs[f"{strd_cr_amt}_{self.gl_suffix}"].sum()
        # abs(in_gl_no_bs[f'{strd_cr_amt}_{self.gl_suffix}'].sum() - in_gl_no_bs[f'{strd_dr_amt}_{self.gl_suffix}'].sum())  # to test if abs is needed - old formula to test new
        reversal_of_aje_df = in_bs_no_gl[in_bs_no_gl[strd_trn_ref] != ""]
        reversal_of_aje_amount = (
            reversal_of_aje_df[f"{strd_cr_amt}_{self.bs_suffix}"].sum()
            - reversal_of_aje_df[f"{strd_dr_amt}_{self.bs_suffix}"].sum()
        )
        stale_checks_amount = (
            0  # we need check date for this one but in sample dont have one.
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

        # BS
        deposit_on_transit_amount = in_gl_no_bs[f"{strd_dr_amt}_{self.gl_suffix}"].sum()
        #  end

        # #  replacing empty or NaN values to empty string
        # in_gl_no_bs.replace(np.nan, '', regex=True, inplace=True)
        # in_bs_no_gl.replace(np.nan, '', regex=True, inplace=True)
        # match_df.replace(np.nan, '', regex=True, inplace=True)
        # for_aje_df.replace(np.nan, '', regex=True, inplace=True)
        # #  end

        # #  getting columns from bs cols if not in gl cols - vice versa
        # bs_cols = [x for x in in_bs_no_gl.columns if ((x in bs_df.columns) or ("_bs" in x))]
        # gl_cols = [x for x in in_gl_no_bs.columns if ((x in gl_df.columns) or ("_gl" in x))]
        # #  end

        # #  setting final cols of gl and bs
        # in_gl_no_bs = in_gl_no_bs[gl_cols]
        # in_bs_no_gl = in_bs_no_gl[bs_cols]
        # #  end

        # for_aje_df.fillna("None", inplace=True)
        # in_bs_no_gl.fillna("None", inplace=True)
        # in_gl_no_bs.fillna("None", inplace=True)

        # # commented due to bug when there is no results.
        # # for_aje_df = for_aje_df.loc[:, ~for_aje_df.eq("None").all()]
        # # in_bs_no_gl = in_bs_no_gl.loc[:, ~in_bs_no_gl.eq("None").all()]
        # # in_gl_no_bs = in_gl_no_bs.loc[:, ~in_gl_no_bs.eq("None").all()]

        # for_aje_df = for_aje_df[for_aje_df["transaction_date"] != "None"]
        # in_bs_no_gl = in_bs_no_gl[in_bs_no_gl["transaction_date"] != "None"]
        # in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["trndate"] != "None"]
        # # in_gl_no_bs = in_gl_no_bs[in_gl_no_bs["transaction_reference"] != "DM"]
        for_aje_df = for_aje_df[
            [
                "transaction_date",
                strd_trn_ref,
                "ref_count_bs",
                "ref_count_gl",
                "gl_amount",
                "bs_amount",
                "variance",
            ]
        ]
        book_errors_amt = for_aje_df["variance"].sum()
        return_gl = {}
        return_bs = {}

        if not gl_df_before.empty:
            
            return_gl = gl_df[gl_df["approved"] == "approved"]
            return_gl = return_gl[return_gl["is_matched"] == False].to_dict()

        if not bs_df_before.empty:
            
            return_bs = bs_df[bs_df["approved"] == "approved"]
            return_bs = return_bs[return_bs["is_matched"] == False].to_dict()

        match_df.replace({pd.NaT: None}, inplace=True)
        for_aje_df.replace({pd.NaT: None}, inplace=True)
        in_bs_no_gl.replace({pd.NaT: None}, inplace=True)
        in_gl_no_bs.replace({pd.NaT: None}, inplace=True)
        
        
        return {
            "match":match_df.to_dict("records"),
            "book_errors": for_aje_df.to_dict("records"),
            "in_bs_no_gl": in_bs_no_gl.to_dict(
                "records"
            ),  # base_table => PB, in_bs_no_gl is the records that exist in BS but no equivalent in GL table
            "in_gl_no_bs": in_gl_no_bs.to_dict(
                "records"
            ),  # base_table => GL, in_gl_no_bs is the records that exist in GL but no equivalent in BS table
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
            "recon_items_unmatched_bs_cols": return_gl,
            "recon_items_unmatched_gl_cols": return_bs,
        }

    def get_time_range(self, gl_df, bs_df):
        gl_df["trndate"] = pd.to_datetime(gl_df["trndate"])

        """
        gl_max_date = gl_df["trndate"].max()
        bs_max_date = bs_df["transaction_date"].max()
        print(gl_df["trndate"].unique())
        print(f"gl_max_date : {gl_max_date}")
        print(self.from_date, type(self.from_date))
        print(self.to_date, type(self.to_date))
        if gl_max_date >= self.to_date:
            gl_df = gl_df[gl_df["trndate"].between(self.from_date, self.to_date)]
        else:
            gl_df = pd.DataFrames(columns=list(gl_df.columns))
        if bs_df["transaction_date"].max() >= self.to_date:
            bs_df = bs_df[bs_df["transaction_date"].between(self.from_date, self.to_date)]
        bs_df["transaction_date"] = pd.to_datetime(bs_df["transaction_date"])
        gl_df = gl_df[gl_df["trndate"].between(self.from_date, self.to_date)]
        print("=========================================== START =====================================")
        print(gl_df)
        print("=====================================================================================")
        print(bs_df)
        print("========================================== END =======================================")
        """
        mask = (gl_df['trndate'] > self.from_date) & (gl_df['trndate'] <= self.to_date)
        gl_df = gl_df.loc[mask]
        mask = (bs_df['transaction_date'] > self.from_date) & (bs_df['transaction_date'] <= self.to_date)
        bs_df = bs_df.loc[mask]


        # raise ValueError(gl_df,bs_df)


        #gl_df = gl_df[gl_df["trndate"].between_time(self.from_date, self.to_date)]
        #bs_df = bs_df[bs_df["transaction_date"].between(self.from_date, self.to_date)]
        return gl_df, bs_df

    def get_time_range_before(self, gl_df, bs_df):
        gl_columns = list(gl_df.columns)
        df_columns = list(bs_df.columns)
        cutoff_gl_df = pd.to_datetime(self.from_date)
        gl_df = gl_df.loc[gl_df["trndate"] < cutoff_gl_df]
        bs_df = bs_df.loc[bs_df["transaction_date"] < cutoff_gl_df]

        for col in gl_df.columns:
            print(col)
        data = []
        if not gl_df.empty:
            gl_df_approved = gl_df[gl_df["approved"] == "approved"]
            gl_df_approved_not_matched = gl_df[gl_df["is_matched"] == False]
            data.append(gl_df_approved_not_matched)
            for col in gl_df_approved_not_matched.columns:
                print(col)

        if not bs_df.empty:
            bs_df = bs_df[bs_df["approved"] == "approved"]
            bs_df = bs_df[bs_df["is_matched"] == False]
            bl_df_approved_not_matched = bs_df[bs_df["is_matched"] == False]

            for col in bl_df_approved_not_matched.columns:
                print(col)
            data.append(bl_df_approved_not_matched)

        if (not bs_df.empty) or not (gl_df.empty):
            print(gl_df)
            set_data = (d for d in data)
            return set_data
        empty_gl_df = pd.DataFrame(columns=gl_columns)
        empty_bs_df = pd.DataFrame(columns=df_columns)
        return empty_gl_df, empty_bs_df

    def get_total_amount(self, gl_df, bs_df):
        """
        rename to values for pandas merge
        other_03 = transaction_reference
        dr_amt = credit_amount
        cr_amt = debit_amount
        """
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = (
            'credit_amount',
            "debit_amount",
        )

        strd_dr_amt = self.re_mapping(gl_df,"dr_amt")
        strd_cr_amt = self.re_mapping(gl_df,"cr_amt")
        gl_credit_total_amt = -(gl_df[strd_dr_amt].sum())
        gl_debit_total_amt = gl_df[strd_cr_amt].sum()

        strd_dr_amt = self.re_mapping(bs_df,"dr_amt")
        strd_cr_amt = self.re_mapping(bs_df,"cr_amt")
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
        strd_cr_amt = "cr_amt"
        strd_dr_amt = "dr_amt"
        strd_trn_ref = "trnref"

        # counts all trn_ref of a transaction in the df

        strd_trn_ref = self.re_mapping(df, "trn_ref")
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
        empty_trn_ref = df[df[strd_trn_ref] == ""]
        dm_trn_ref = df[df[strd_trn_ref] == "DM"]
        df_new = df[(df[strd_trn_ref] != "DM") & (df[strd_trn_ref] != "")]
        df_new = (
            df_new.groupby(strd_trn_ref, as_index=False)
            .aggregate(col)
            .reindex(columns=df_new.columns)
        )
        df_new = pd.concat([dm_trn_ref, df_new])
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
