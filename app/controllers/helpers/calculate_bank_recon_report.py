import pandas as pd
from pprint import pprint
import numpy as np
from datetime import datetime
from .. import DbOps


mo = DbOps.MainOps()
client = mo.client
db = client["bank_recon_field_keywords"]


class ReportCalculator:

    def __init__(self,
                 general_ledger_df,
                 bank_statement_df,
                 matched_df,
                 filtered_df,
                 book_errors_df,
                 to_match: dict) -> None:

        self.field_to_match = to_match
        self.strd_cr_amt = ""
        self.strd_dr_amt = ""
        self.strd_trn_ref = ""
        self.strd_trn_date = ""
        self.gl_suffix = "gl"
        self.bs_suffix = "bs"
        self.general_ledger_df = general_ledger_df
        self.bank_statement_df = bank_statement_df
        self.filtered_df = filtered_df
        self.matched_df = matched_df
        self.book_errors_df = book_errors_df
        self.bank_recon_report = self.calculate_bank_recon_report()
        self.total_dr_cr_amts = self.bank_recon_report["total_dr_cr_amts"]
        self.book_errors_amt = self.bank_recon_report["book_errors_amt"]
        self.match = self.bank_recon_report["match"]
        # self.book_errors = self.bank_recon_report['book_errors']
        self.debit_memo_amount = self.bank_recon_report['debit_memo_amount']
        self.credit_memo_amount = self.bank_recon_report['credit_memo_amount']
        self.outstanding_checks_amount = self.bank_recon_report['outstanding_checks_amount']
        self.reversal_of_aje_amount = self.bank_recon_report['reversal_of_aje_amount']
        self.stale_checks_amount = self.bank_recon_report['stale_checks_amount']
        self.deposit_on_transit_amount = self.bank_recon_report['deposit_on_transit_amount']
        self.in_bs_no_gl = self.bank_recon_report["in_bs_no_gl"]
        self.in_gl_no_bs = self.bank_recon_report["in_gl_no_bs"]
        self.unmatched_bs_cols = self.bank_recon_report["unmatched_bs_cols"]
        self.unmatched_gl_cols = self.bank_recon_report["unmatched_gl_cols"]
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
            #for many to many we need to append to string formed list of dictionaries
            ref2_data = str(self.get_multiple_entries_breakdown(ref2, "bs"))
            df.loc[df.transaction_reference == ref2, 'mult'] = str(ref2_data) if ref2_data else None
        return df

    
    def match_keywords(self, match_fields: dict):
        """Matches the keywords that are equivalent to fields
        """
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

        return strd_field_names


    def calculate_bank_recon_report(self):
        gl_df = self.general_ledger_df
        bs_df = self.bank_statement_df
        book_errors_df = self.book_errors_df
        match_df = self.matched_df

        # renames the fields of GL df to have uniform field names when merging
        gl_df.rename(self.field_to_match, axis=1, inplace=True)

        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt, strd_trn_ref = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"], strd_field_names["strd_trn_ref"]

        total_dr_cr_amts = self.get_total_amount(gl_df, bs_df)

        gl_df_dr_sum = gl_df[f'{strd_dr_amt}_{self.gl_suffix}'].sum() \
            if f'{strd_dr_amt}_{self.gl_suffix}' in gl_df.columns.tolist() \
            else 0
        
        gl_df_cr_sum = gl_df[f'{strd_cr_amt}_{self.gl_suffix}'].sum() \
            if f'{strd_cr_amt}_{self.gl_suffix}' in gl_df.columns.tolist() \
            else 0

        #  calculation of bank recon report output
        # outstanding_checks_df = in_gl_no_bs[in_gl_no_bs[strd_trn_ref] != 'DM']
        outstanding_checks_amount = gl_df_cr_sum
        ##-(gl_df[f'{strd_dr_amt}_{self.gl_suffix}'].sum() - gl_df[f'{strd_cr_amt}_{self.gl_suffix}'].sum())  # to test if abs is needed - old
        if not bs_df.empty: 
            reversal_of_aje_df = bs_df[bs_df[strd_trn_ref] != '']
            reversal_of_aje_amount = reversal_of_aje_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - reversal_of_aje_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()
            debit_memo_df = bs_df[bs_df[strd_trn_ref] == '']
            debit_memo_df = debit_memo_df[debit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'] > 0]
            debit_memo_amount = debit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - debit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()

            credit_memo_df = bs_df[bs_df[strd_trn_ref] == '']
            credit_memo_df = credit_memo_df[credit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'] > 0]
            credit_memo_amount = credit_memo_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() - credit_memo_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum()
        else:
            reversal_of_aje_amount = 0
            debit_memo_amount = 0
            credit_memo_amount = 0

        stale_checks_amount = 0  # we need check date for this one but in sample dont have one.


        # BS
        deposit_on_transit_amount = gl_df_dr_sum
        #  end

        book_errors_amt = book_errors_df["variance"].sum() \
            if "variance" in self.book_errors_df.columns.tolist() \
            else 0

        return {
            "match" : match_df.to_dict('records'),
            # "book_errors" : for_aje_df.to_dict("records"),
            "in_bs_no_gl" : bs_df.to_dict('records'), #base_table => PB, in_bs_no_gl is the records that exist in BS but no equivalent in GL table
            "in_gl_no_bs" : gl_df.to_dict('records'), #base_table => GL, in_gl_no_bs is the records that exist in GL but no equivalent in BS table
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
            "unmatched_bs_cols": bs_df.columns.to_list(),
            "unmatched_gl_cols": gl_df.columns.to_list(),
            
        }


    def get_total_amount(self, gl_df, bs_df):
        '''
            rename to values for pandas merge
            other_03 = transaction_reference
            dr_amt = credit_amount
            cr_amt = debit_amount
        '''
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"]

        matched_df_dr_sum = self.matched_df[f'{strd_dr_amt}_{self.gl_suffix}'].sum() \
            if f'{strd_dr_amt}_{self.gl_suffix}' in self.matched_df.columns.tolist() \
            else 0
        
        book_err_sum = self.book_errors_df[f'{self.gl_suffix}_amount'].sum() \
            if f'{self.gl_suffix}_amount' in self.book_errors_df.columns.tolist() \
            else 0
        
        filtered_df_sum = self.filtered_df[f'{strd_dr_amt}_{self.gl_suffix}'].sum() \
            if f'{strd_dr_amt}_{self.gl_suffix}' in self.filtered_df.columns.tolist() \
            else 0
        
        gl_df_dr_sum = gl_df[f'{strd_dr_amt}_{self.gl_suffix}'].sum() \
            if f'{strd_dr_amt}_{self.gl_suffix}' in gl_df.columns.tolist() \
            else 0
        
        gl_df_cr_sum = gl_df[f'{strd_cr_amt}_{self.gl_suffix}'].sum() \
            if f'{strd_cr_amt}_{self.gl_suffix}' in gl_df.columns.tolist() \
            else 0
        
        bs_df_dr_sum = bs_df[f'{strd_dr_amt}_{self.bs_suffix}'].sum() \
            if f'{strd_dr_amt}_{self.bs_suffix}' in bs_df.columns.tolist() \
            else 0
        
        bs_df_cr_sum = bs_df[f'{strd_cr_amt}_{self.bs_suffix}'].sum() \
            if f'{strd_cr_amt}_{self.bs_suffix}' in bs_df.columns.tolist() \
            else 0

        gl_credit_total_amt = -((gl_df_dr_sum) + (matched_df_dr_sum) + 
                                (filtered_df_sum) + (book_err_sum))

        gl_debit_total_amt = gl_df_cr_sum

        bs_credit_total_amt = bs_df_cr_sum
        bs_debit_total_amt = -(bs_df_dr_sum)

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
        df = (self.multiple_entries_gl
              if gl_or_bs == "gl"
              else self.multiple_entries_bs)
        return df[df[self.strd_trn_ref] == trans_ref].to_dict("records")


    def check_variance(self, df):
        strd_field_names = self.match_keywords(self.field_to_match)
        strd_cr_amt, strd_dr_amt = strd_field_names["strd_cr_amt"], strd_field_names["strd_dr_amt"]
        # pprint(strd_cr_amt)
        debit_amount_gl = df[f'{strd_cr_amt}_{self.gl_suffix}']
        credit_amount_gl = df[f'{strd_dr_amt}_{self.gl_suffix}']
        # debit_amount_gl = df[f'{strd_cr_amt}']
        # credit_amount_gl = df[f'{strd_dr_amt}']
        df['gl_amount'] = debit_amount_gl - credit_amount_gl
        df['bs_amount'] = df[f'{strd_cr_amt}_{self.bs_suffix}'] - df[f'{strd_dr_amt}_{self.bs_suffix}']
        # df['bs_amount'] = df[f'{strd_cr_amt}'] - df[f'{strd_dr_amt}']
        df = df.astype({"bs_amount": float, "gl_amount": float})
        df['variance'] = df["bs_amount"] - df["gl_amount"]
        df = df.astype({"variance": float})
        match_df = df[df["variance"].round(2) == 0] # | (df["variance"] <= float(0.0009))
        for_aje_df = df[df["variance"].round(2) != 0]
        return match_df, for_aje_df


